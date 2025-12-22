package persistence

import (
	"encoding/gob"
	"io"
	"os"
	"sync"

	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
)

type FileWAL struct {
	filePath string
	file     *os.File
	encoder  *gob.Encoder
	mu       sync.Mutex // Protects concurrent writing to the file
	logger   ports.Logger
}

func NewFileWAL(filePath string, logger ports.Logger) (*FileWAL, error) {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &FileWAL{
		filePath: filePath,
		file:     f,
		encoder:  gob.NewEncoder(f),
		logger:   logger,
	}, nil
}

func (w *FileWAL) Save(msg domain.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.encoder.Encode(msg); err != nil {
		w.logger.Error("Failed to write to WAL", "error", err)
		return err
	}
	
	return nil
}

func (w *FileWAL) LoadAll() ([]domain.Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Reopens file for read-only access from beginning
	f, err := os.Open(w.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []domain.Message{}, nil
		}
		return nil, err
	}
	defer f.Close()

	var messages []domain.Message
	decoder := gob.NewDecoder(f)

	for {
		var msg domain.Message
		err := decoder.Decode(&msg)
		if err == io.EOF {
			break // end file
		}
		if err != nil {
			w.logger.Warn("Corrupted WAL entry found, stopping replay", "error", err)
			break
		}
		messages = append(messages, msg)
	}

	w.logger.Info("WAL Replay finished", "count", len(messages))
	return messages, nil
}

func (w *FileWAL) Close() error {
	return w.file.Close()
}