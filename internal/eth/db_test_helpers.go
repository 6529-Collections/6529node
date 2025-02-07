package eth

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func setupTestInMemoryDB(t *testing.T) *badger.DB {
    t.Helper()

    opts := badger.DefaultOptions("").WithInMemory(true).
        WithLogger(nil) // disable logs for test cleanliness

    db, err := badger.Open(opts)
    require.NoError(t, err)

    return db
}