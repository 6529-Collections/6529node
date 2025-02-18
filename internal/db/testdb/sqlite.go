package testdb

import (
	"database/sql"
	"os"
	"testing"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/stretchr/testify/require"
)

const testDBPath = "./db/sqlite/test/sqlite"

func SetupTestDB(t *testing.T) (*sql.DB, func()) {
	db, err := db.OpenSqlite(testDBPath)
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		os.RemoveAll(testDBPath)
	}
	return db, cleanup
}
