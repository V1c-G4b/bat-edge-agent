package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// helper para criar diretório temporário isolado por teste
func createTempWAL(t *testing.T) (string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	walPath := filepath.Join(dir, "test.wal")

	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	return walPath, cleanup
}

// 1. TESTE DO CAMINHO FELIZ: Grava N registros e recupera exatamente na ordem
func TestWAL_WriteAndRecover(t *testing.T) {
	walPath, cleanup := createTempWAL(t)
	defer cleanup()

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	payloads := [][]byte{
		[]byte("cpu_usage: 12.5%"),
		[]byte("mem_usage: 64.2%"),
		[]byte("disk_alert: threshold exceeded"),
	}

	for _, p := range payloads {
		if err := wal.Write(p); err != nil {
			t.Fatalf("wal.Write failed: %v", err)
		}
	}
	_ = wal.Close()

	// Recupera e valida integridade
	records, err := RecoverWAL(walPath)
	if err != nil {
		t.Fatalf("RecoverWAL failed: %v", err)
	}

	if len(records) != len(payloads) {
		t.Fatalf("expected %d records, got %d", len(payloads), len(records))
	}

	for i, r := range records {
		if !bytes.Equal(r.Data, payloads[i]) {
			t.Errorf("record %d data mismatch. Expected %s, got %s", i, payloads[i], r.Data)
		}
		if r.Timestamp <= 0 {
			t.Errorf("record %d has invalid timestamp: %d", i, r.Timestamp)
		}
	}
}

// 2. TESTE DE CONCORRÊNCIA: Múltiplas goroutines escrevendo em paralelo
func TestWAL_ConcurrentWrites(t *testing.T) {
	walPath, cleanup := createTempWAL(t)
	defer cleanup()

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	const numGoroutines = 10
	const writesPerGoroutine = 50
	totalExpected := numGoroutines * writesPerGoroutine

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for w := 0; w < writesPerGoroutine; w++ {
				data := []byte(fmt.Sprintf("g:%d-item:%d", routineID, w))
				if err := wal.Write(data); err != nil {
					t.Errorf("concurrent write error: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()
	_ = wal.Close()

	// Se o Mutex falhou, os bytes foram misturados e o CRC vai quebrar na recuperação
	records, err := RecoverWAL(walPath)
	if err != nil {
		t.Fatalf("RecoverWAL after concurrent writes failed: %v", err)
	}

	if len(records) != totalExpected {
		t.Fatalf("expected %d records total, got %d", totalExpected, len(records))
	}
}

// 3. TESTE DE FALHA: Simula arquivo corrompido ou truncado por corte de energia
func TestWAL_CorruptedRecord(t *testing.T) {
	walPath, cleanup := createTempWAL(t)
	defer cleanup()

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	_ = wal.Write([]byte("dado_integro_1"))
	_ = wal.Write([]byte("dado_integro_2"))
	_ = wal.Close()

	// Injetamos bytes lixo no final (simulando que a energia caiu enquanto o disco gravava)
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file for tampering: %v", err)
	}
	// Escreve apenas 5 bytes soltos (menos que os 16 bytes do HeaderSize)
	_, _ = f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01})
	_ = f.Close()

	// Recuperação deve resgatar os dois íntegros e acusar o truncamento do terceiro
	records, err := RecoverWAL(walPath)
	if !errors.Is(err, ErrTruncatedRecord) {
		t.Fatalf("expected ErrTruncatedRecord, got %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 valid recovered records, got %d", len(records))
	}
}
