package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestOpenBadger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	t.Run("successfully opens database", func(t *testing.T) {
		db, err := OpenBadger(dbPath)
		require.NoError(t, err)
		require.NotNil(t, db)
		defer db.Close()

		_, err = os.Stat(tmpDir)
		assert.NoError(t, err)
	})

	t.Run("fails with invalid path", func(t *testing.T) {
		invalidPath := filepath.Join("/nonexistent", "invalid", "path", "db")
		db, err := OpenBadger(invalidPath)
		assert.Error(t, err)
		assert.Nil(t, db)
		assert.Contains(t, err.Error(), "failed to create directory")
	})
}

func TestZapAdapter(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	adapter := zapAdapter{logger}

	t.Run("test Errorf", func(t *testing.T) {
		adapter.Errorf("test error: %s", "message")
	})

	t.Run("test Warningf", func(t *testing.T) {
		adapter.Warningf("test warning: %s", "message")
	})

	t.Run("test Infof", func(t *testing.T) {
		adapter.Infof("test info: %s", "message")
	})

	t.Run("test Debugf", func(t *testing.T) {
		adapter.Debugf("test debug: %s", "message")
	})
}

func TestOpenBadger_ConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-concurrent-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "concurrent.db")

	db1, err := OpenBadger(dbPath)
	require.NoError(t, err)
	defer db1.Close()

	db2, err := OpenBadger(dbPath)
	assert.Error(t, err)
	assert.Nil(t, db2)
	assert.Contains(t, err.Error(), "failed to open BadgerDB")
}

func TestOpenBadger_DirectoryPermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-perms-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "subdir", "test.db")

	db, err := OpenBadger(dbPath)
	require.NoError(t, err)
	defer db.Close()

	info, err := os.Stat(filepath.Dir(dbPath))
	require.NoError(t, err)

	expectedPerm := os.FileMode(0755)
	actualPerm := info.Mode().Perm()

	assert.True(t, actualPerm <= expectedPerm,
		"Expected permissions to be at most %v, got %v", expectedPerm, actualPerm)
}
