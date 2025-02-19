package eth

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) (BlockHashDb, func()) {
	db, cleanup := testdb.SetupTestDB(t)
	return NewBlockHashDb(db), cleanup
}

func TestBlockHashDb_SetAndGetHash(t *testing.T) {
	blockHashDb, cleanup := setupTestDB(t)
	defer cleanup()

	blockNum := uint64(12345)
	hash := common.HexToHash("0x123456789abcdef")

	err := blockHashDb.SetHash(blockNum, hash)
	assert.NoError(t, err)

	retrievedHash, exists := blockHashDb.GetHash(blockNum)
	assert.True(t, exists)
	assert.Equal(t, hash, retrievedHash)
}

func TestBlockHashDb_GetNonExistentHash(t *testing.T) {
	blockHashDb, cleanup := setupTestDB(t)
	defer cleanup()

	hash, exists := blockHashDb.GetHash(999999)
	assert.False(t, exists)
	assert.Equal(t, common.Hash{}, hash)
}

func TestBlockHashDb_RevertFromBlock(t *testing.T) {
	blockHashDb, cleanup := setupTestDB(t)
	defer cleanup()

	for i := uint64(1); i <= 5; i++ {
		hash := common.HexToHash(fmt.Sprintf("0xblock%d", i))
		require.NoError(t, blockHashDb.SetHash(i, hash), "failed to set hash for block %d", i)
	}

	t.Run("Revert from a middle block", func(t *testing.T) {
		err := blockHashDb.RevertFromBlock(3)
		require.NoError(t, err)

		for i := uint64(1); i < 3; i++ {
			h, ok := blockHashDb.GetHash(i)
			assert.True(t, ok, "Expected block %d to remain", i)
			assert.Equal(t, common.HexToHash(fmt.Sprintf("0xblock%d", i)), h)
		}

		for i := uint64(3); i <= 5; i++ {
			h, ok := blockHashDb.GetHash(i)
			assert.False(t, ok, "Expected block %d to be deleted", i)
			assert.Equal(t, common.Hash{}, h)
		}
	})

	t.Run("Revert from block 0", func(t *testing.T) {
		for i := uint64(1); i <= 5; i++ {
			hash := common.HexToHash(fmt.Sprintf("0xblock%d", i))
			require.NoError(t, blockHashDb.SetHash(i, hash), "failed to set hash for block %d", i)
		}

		err := blockHashDb.RevertFromBlock(0)
		require.NoError(t, err)

		for i := uint64(1); i <= 5; i++ {
			_, ok := blockHashDb.GetHash(i)
			assert.False(t, ok, "Expected block %d to be removed by revert from block 0", i)
		}
	})

	t.Run("Revert from a block greater than existing blocks", func(t *testing.T) {
		for i := uint64(1); i <= 5; i++ {
			hash := common.HexToHash(fmt.Sprintf("0xblock%d", i))
			require.NoError(t, blockHashDb.SetHash(i, hash), "failed to set hash for block %d", i)
		}

		err := blockHashDb.RevertFromBlock(999)
		require.NoError(t, err)

		for i := uint64(1); i <= 5; i++ {
			h, ok := blockHashDb.GetHash(i)
			assert.True(t, ok)
			assert.Equal(t, common.HexToHash(fmt.Sprintf("0xblock%d", i)), h)
		}
	})

	t.Run("Revert from block 1 removes everything (1..∞)", func(t *testing.T) {
		err := blockHashDb.RevertFromBlock(1)
		require.NoError(t, err)

		for i := uint64(1); i <= 5; i++ {
			_, ok := blockHashDb.GetHash(i)
			assert.False(t, ok, "Expected block %d to be removed by revert from block 1", i)
		}
	})
}

func TestBlockHashDb_ConcurrentAccess(t *testing.T) {
	blockHashDb, cleanup := setupTestDB(t)
	defer cleanup()

	done := make(chan bool)
	go func() {
		for i := uint64(0); i < 100; i++ {
			hash := common.BigToHash(big.NewInt(int64(i)))
			err := blockHashDb.SetHash(i, hash)
			assert.NoError(t, err)
		}
		done <- true
	}()

	go func() {
		for i := uint64(0); i < 100; i++ {
			blockHashDb.GetHash(i)
		}
		done <- true
	}()

	<-done
	<-done
}
