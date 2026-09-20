package main

import (
	"bat-edge-agent/internal/collector"
	"context"
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

	"github.com/shirou/gopsutil/v4/host"
)

func routine(c *collector.SystemCollector, ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("start routine")

	for {
		select {
		case <-ctx.Done():
			log.Println("stop routine")
			return

		case <-ticker.C:
			executeWithSecurity(c)
		}
	}
}

func executeWithSecurity(c *collector.SystemCollector) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("recover from panic:", r)
		}
	}()
	metricsCollect(c)
}

func metricsCollect(c *collector.SystemCollector) {
	metrics, err := c.Collect()
	if err != nil {
		log.Println(err)
	}
	version, _ := host.KernelVersion()
	fmt.Println(version)

	platform, family, version, _ := host.PlatformInformation()
	fmt.Println("platform:", platform)
	fmt.Println("family:", family)
	fmt.Println("version:", version)

	fmt.Println(metrics)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	c := collector.NewSystemCollector()

	defer stop()

	var wg sync.WaitGroup
	wg.Go(func() {
		routine(c, ctx)
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
	stop()
	log.Println("stop http server")

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
