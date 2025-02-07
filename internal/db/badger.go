package db

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

type zapAdapter struct {
	*zap.Logger
}

func (z zapAdapter) Errorf(f string, v ...interface{}) {
	z.Sugar().Errorf(f, v...)
}

func (z zapAdapter) Warningf(f string, v ...interface{}) {
	z.Sugar().Warnf(f, v...)
}

func (z zapAdapter) Infof(f string, v ...interface{}) {
	z.Sugar().Infof(f, v...)
}

func (z zapAdapter) Debugf(f string, v ...interface{}) {
	// To avoid spam in dev, we don't log debug messages
}

func OpenBadger(path string) (*badger.DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create directory for BadgerDB: %w", err)
	}

	opts := badger.DefaultOptions(path).WithSyncWrites(true)
	opts.Logger = zapAdapter{zap.L()}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}
	return db, nil
}
