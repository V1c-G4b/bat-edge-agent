package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

const (
	headerSize = 16
)

var (
	ErrCorruptedRecord = errors.New("wal: checksum verification failed (corrupted record)")
	ErrTruncatedRecord = errors.New("wal: incomplete or truncated record at end of file")
)

type Record struct {
	Timestamp int64
	Data      []byte
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal: %w", err)
	}

	return &WAL{file: file}, nil
}

func (w *WAL) Write(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	timestamp := time.Now().UnixNano()
	payloadSize := uint32(len(payload))

	headerBuf := make([]byte, headerSize)

	binary.BigEndian.PutUint64(headerBuf[4:12], uint64(timestamp))
	binary.BigEndian.PutUint32(headerBuf[12:16], payloadSize)

	hasher := crc32.NewIEEE()
	_, _ = hasher.Write(headerBuf[4:16])
	_, _ = hasher.Write(payload)
	checksum := hasher.Sum32()

	binary.BigEndian.PutUint32(headerBuf[0:4], checksum)

	if _, err := w.file.Write(headerBuf); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal: %w", err)
	}
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return err
	}
	return nil
}

func RecoverWAL(path string) ([]Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []Record
	headerBuf := make([]byte, headerSize)

	for {
		_, err := io.ReadFull(file, headerBuf)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return records, ErrTruncatedRecord
		}
		if err != nil {
			return records, err
		}

		expectedCRC := binary.BigEndian.Uint32(headerBuf[0:4])
		timestamp := int64(binary.BigEndian.Uint64(headerBuf[4:12]))
		payloadSize := binary.BigEndian.Uint32(headerBuf[12:16])

		payload := make([]byte, payloadSize)
		_, err = io.ReadFull(file, payload)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return records, ErrCorruptedRecord
		}
		if err != nil {
			return records, err
		}

		hasher := crc32.NewIEEE()
		_, _ = hasher.Write(headerBuf[4:16])
		_, _ = hasher.Write(payload)
		actualCRC := hasher.Sum32()

		if actualCRC != expectedCRC {
			return records, ErrCorruptedRecord
		}

		records = append(records, Record{timestamp, payload})
	}
	return records, nil
}
