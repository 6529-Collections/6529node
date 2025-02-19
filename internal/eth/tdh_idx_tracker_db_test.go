package eth

import (
	"context"
	"math/big"
	"sync"
	"testing"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestTdhIdxTrackerDb(t *testing.T) (TdhIdxTrackerDb, func()) {
	db, cleanup := testdb.SetupTestDB(t)
	return NewTdhIdxTrackerDb(db), cleanup
}

func TestTdhIdxTrackerDb_GetProgress_KeyNotFound(t *testing.T) {
	tdhDb, cleanup := setupTestTdhIdxTrackerDb(t)
	defer cleanup()

	progress, err := tdhDb.GetProgress()
	require.NoError(t, err, "GetProgress should not fail when key is missing")
	assert.Equal(t, uint64(0), progress, "Expected progress to be zero if key not found")
}

func TestTdhIdxTrackerDb_SetAndGetProgress(t *testing.T) {
	tdhDb, cleanup := setupTestTdhIdxTrackerDb(t)
	defer cleanup()

	var blockNum uint64 = 42
	err := tdhDb.SetProgress(blockNum, context.Background())
	require.NoError(t, err, "SetProgress should not fail")

	retrieved, err := tdhDb.GetProgress()
	require.NoError(t, err, "GetProgress should not fail")
	assert.Equal(t, blockNum, retrieved, "Progress retrieved should match the one we set")
}

func TestTdhIdxTrackerDb_OverwriteProgress(t *testing.T) {
	tdhDb, cleanup := setupTestTdhIdxTrackerDb(t)
	defer cleanup()

	err := tdhDb.SetProgress(100, context.Background())
	require.NoError(t, err)

	err = tdhDb.SetProgress(50, context.Background())
	require.NoError(t, err)

	progress, err := tdhDb.GetProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(50), progress, "Progress should update to the new value")

	err = tdhDb.SetProgress(999999, context.Background())
	require.NoError(t, err)

	progress, err = tdhDb.GetProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(999999), progress, "Progress should update to the new value")
}

func TestTdhIdxTrackerDb_ConcurrentAccess(t *testing.T) {
	tdhDb, cleanup := setupTestTdhIdxTrackerDb(t)
	defer cleanup()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			err := tdhDb.SetProgress(uint64(i), context.Background())
			assert.NoError(t, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, err := tdhDb.GetProgress()
			assert.NoError(t, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < 200; i++ {
			err := tdhDb.SetProgress(uint64(i), context.Background())
			assert.NoError(t, err)
			val, err := tdhDb.GetProgress()
			assert.NoError(t, err)
			_ = val
		}
	}()

	wg.Wait()

	finalProgress, err := tdhDb.GetProgress()
	require.NoError(t, err)
	assert.True(t, finalProgress <= 199,
		"Final progress should be <1 99")
}

func TestTdhIdxTrackerDb_SetProgress_ErrorHandling(t *testing.T) {

	tdhDb, cleanup := setupTestTdhIdxTrackerDb(t)

	cleanup()

	err := tdhDb.SetProgress(777, context.Background())
	assert.Error(t, err, "Expected an error when trying to write to a closed DB")

	_, err = tdhDb.GetProgress()
	assert.Error(t, err, "Expected an error when trying to read from a closed DB")
}

func TestTdhIdxTrackerDb_BigNumbers(t *testing.T) {
	tdhDb, cleanup := setupTestTdhIdxTrackerDb(t)
	defer cleanup()

	bigVal := new(big.Int).SetUint64(1<<61 + 1234567)
	err := tdhDb.SetProgress(bigVal.Uint64(), context.Background())
	require.NoError(t, err)

	retrieved, err := tdhDb.GetProgress()
	require.NoError(t, err)
	assert.Equal(t, bigVal.Uint64(), retrieved, "Should retrieve the exact big value we set")
}
