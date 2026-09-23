package main

import (
	"bat-edge-agent/internal/collector"
	"bat-edge-agent/internal/storage"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"
)

func routine(c *collector.SystemCollector, ctx context.Context, esteira chan<- []byte) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("start routine")

	for {
		select {
		case <-ctx.Done():
			log.Println("stop routine")
			return

		case <-ticker.C:
			metrics := executeWithSecurity(c)

			esteira <- metrics
		}
	}
}

func executeWithSecurity(c *collector.SystemCollector) []byte {
	defer func() {
		if r := recover(); r != nil {
			log.Println("recover from panic:", r)
		}
	}()
	return metricsCollect(c)
}

func metricsCollect(c *collector.SystemCollector) []byte {
	metrics, err := c.Collect()
	if err != nil {
		log.Println(err)
	}
	marshal, err := json.Marshal(metrics)
	if err != nil {
		return nil
	}
	return marshal
}

func walConsumer(wal *storage.WAL, esteira <-chan []byte) {
	log.Println("start wal consumer")

	for data := range esteira {
		log.Printf("WAL writing metric (%d bytes): %s\n", len(data), string(data))
		if err := wal.Write(data); err != nil {
			log.Printf("failed to write to WAL: %v\n", err)
		}
	}

	log.Println("stop wal consumer: pipeline fully drained")
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c := collector.NewSystemCollector()

	wal, err := storage.OpenWAL("telemetry.wal")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := wal.Close(); err != nil {
			log.Printf("Error: %v", err)
		}
	}()

	esteira := make(chan []byte, 10)

	var wg sync.WaitGroup

	wg.Go(func() {
		routine(c, ctx, esteira)
	})

	wg.Go(func() {
		walConsumer(wal, esteira)
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("ok\n"))
		if err != nil {
			return
		}
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}

	wg.Go(func() {
		log.Println("start http server")
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server stopped with error: %v", err)
			stop()
		}
	})

	<-ctx.Done()
	log.Println("starting shutdown server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server stopped with error: %v", err)
	}

	wg.Wait()
	log.Println("stop http server")
	fmt.Println("Goroutines:", runtime.NumGoroutine())
	_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
}
