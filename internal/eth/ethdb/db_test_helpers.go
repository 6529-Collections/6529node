package ethdb

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func setupTestInMemoryDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	t.Cleanup(func() { db.Close() })

	return db
}
