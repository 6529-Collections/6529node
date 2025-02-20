package ethdb

import (
	"database/sql"
	"testing"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupNFTTestDB creates a fresh test DB, ensures the `nfts` table exists,
// and returns (NFTDb, *sql.DB, cleanupFn).
func setupNFTTestDB(t *testing.T) (NFTDb, *sql.DB, func()) {
	db, cleanup := testdb.SetupTestDB(t)

	// Create table for nfts, if it doesn't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS nfts (
		contract TEXT NOT NULL,
		token_id TEXT NOT NULL,
		supply INTEGER NOT NULL,
		burnt_supply INTEGER NOT NULL,
		PRIMARY KEY (contract, token_id)
	);
	`
	_, err := db.Exec(createTableSQL)
	require.NoError(t, err, "failed to create nfts table")

	// Return an instance of the NFTDb implementation.
	nftDb := NewNFTDb()

	return nftDb, db, func() {
		// Drop table (optional) so each test run is clean
		_, _ = db.Exec("DROP TABLE IF EXISTS nfts")
		cleanup()
	}
}

func TestNFTDb_UpdateSupply(t *testing.T) {
	nftDb, db, cleanup := setupNFTTestDB(t)
	defer cleanup()

	t.Run("successfully insert new NFT and increase supply", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = nftDb.UpdateSupply(tx, "contractA", "tokenA", 10)
		assert.NoError(t, err)

		// Commit the transaction so we can read the results
		require.NoError(t, tx.Commit())

		// Check row in DB
		txCheck, err := db.Begin()
		require.NoError(t, err)

		defer func() { _ = tx.Rollback() }()

		nft, err := nftDb.GetNft(txCheck, "contractA", "tokenA")
		require.NoError(t, err)
		assert.Equal(t, int64(10), nft.Supply)
		assert.Equal(t, int64(0), nft.BurntSupply)
	})

	t.Run("update existing NFT supply", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		// First insert
		err = nftDb.UpdateSupply(tx, "contractB", "tokenB", 5)
		require.NoError(t, err)

		// Then update
		err = nftDb.UpdateSupply(tx, "contractB", "tokenB", 3)
		assert.NoError(t, err)

		require.NoError(t, tx.Commit())

		txCheck, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		nft, err := nftDb.GetNft(txCheck, "contractB", "tokenB")
		require.NoError(t, err)
		assert.Equal(t, int64(8), nft.Supply)
	})

	t.Run("error on negative delta", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = nftDb.UpdateSupply(tx, "contractC", "tokenC", -1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delta must be non-negative")
	})
}

func TestNFTDb_UpdateSupplyReverse(t *testing.T) {
	nftDb, db, cleanup := setupNFTTestDB(t)
	defer cleanup()

	t.Run("successfully decrease existing supply", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		// Insert first
		err = nftDb.UpdateSupply(tx, "contractX", "tokenX", 10)
		require.NoError(t, err)

		// Now reverse some
		err = nftDb.UpdateSupplyReverse(tx, "contractX", "tokenX", 5)
		assert.NoError(t, err)

		require.NoError(t, tx.Commit())

		txCheck, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = txCheck.Rollback() }()

		nft, err := nftDb.GetNft(txCheck, "contractX", "tokenX")
		require.NoError(t, err)
		assert.Equal(t, int64(5), nft.Supply)
	})

	t.Run("error if delta <= 0", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = nftDb.UpdateSupplyReverse(tx, "contractX", "tokenX", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delta must be positive for reverse update")
	})

	t.Run("error if NFT doesn't exist", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = nftDb.UpdateSupplyReverse(tx, "nonExistent", "noToken", 5)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot revert supply on nonexistent NFT")
	})

	t.Run("error if resulting supply would go negative", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		// Insert with supply 5
		err = nftDb.UpdateSupply(tx, "contractNeg", "tokenNeg", 5)
		require.NoError(t, err)

		// Attempt to revert 10
		err = nftDb.UpdateSupplyReverse(tx, "contractNeg", "tokenNeg", 10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot revert 10 from supply 5 (would go negative)")
	})
}

func TestNFTDb_UpdateBurntSupply(t *testing.T) {
	nftDb, db, cleanup := setupNFTTestDB(t)
	defer cleanup()

	t.Run("successfully increase burnt supply", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		// Insert initial NFT
		err = nftDb.UpdateSupply(tx, "cBurn", "tBurn", 10)
		require.NoError(t, err)

		err = nftDb.UpdateBurntSupply(tx, "cBurn", "tBurn", 3)
		assert.NoError(t, err)

		require.NoError(t, tx.Commit())

		txCheck, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = txCheck.Rollback() }()

		nft, err := nftDb.GetNft(txCheck, "cBurn", "tBurn")
		require.NoError(t, err)
		assert.Equal(t, int64(10), nft.Supply)
		assert.Equal(t, int64(3), nft.BurntSupply)
	})

	t.Run("error if delta <= 0", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = nftDb.UpdateBurntSupply(tx, "cBurn", "tBurn", -1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delta must be positive for burning supply")
	})

	t.Run("error if NFT does not exist", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = nftDb.UpdateBurntSupply(tx, "noContract", "noToken", 2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot burn NFT that does not exist")
	})
}

func TestNFTDb_UpdateBurntSupplyReverse(t *testing.T) {
	nftDb, db, cleanup := setupNFTTestDB(t)
	defer cleanup()

	t.Run("successfully decrease burnt supply", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		// Insert the NFT with burnt supply 4
		err = nftDb.UpdateSupply(tx, "contractZ", "tokenZ", 10)
		require.NoError(t, err)
		err = nftDb.UpdateBurntSupply(tx, "contractZ", "tokenZ", 4)
		require.NoError(t, err)

		// Reverse 2 from burnt supply
		err = nftDb.UpdateBurntSupplyReverse(tx, "contractZ", "tokenZ", 2)
		assert.NoError(t, err)

		require.NoError(t, tx.Commit())

		txCheck, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = txCheck.Rollback() }()

		nft, err := nftDb.GetNft(txCheck, "contractZ", "tokenZ")
		require.NoError(t, err)
		assert.Equal(t, int64(10), nft.Supply)
		assert.Equal(t, int64(2), nft.BurntSupply)
	})

	t.Run("error if delta <= 0", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = nftDb.UpdateBurntSupplyReverse(tx, "contractZ", "tokenZ", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delta must be positive for reverse burnt supply update")
	})

	t.Run("error if NFT does not exist", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = nftDb.UpdateBurntSupplyReverse(tx, "doesNotExist", "tokenNA", 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot revert burnt supply on nonexistent NFT")
	})

	t.Run("error if burnt supply would go negative", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		// Insert the NFT with burnt supply 2
		err = nftDb.UpdateSupply(tx, "contractZZ", "tokenZZ", 10)
		require.NoError(t, err)
		err = nftDb.UpdateBurntSupply(tx, "contractZZ", "tokenZZ", 2)
		require.NoError(t, err)

		// Try to revert 5
		err = nftDb.UpdateBurntSupplyReverse(tx, "contractZZ", "tokenZZ", 5)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot revert 5 from burnt supply 2 (would go negative)")
	})
}

func TestNFTDb_GetNft(t *testing.T) {
	nftDb, db, cleanup := setupNFTTestDB(t)
	defer cleanup()

	t.Run("successfully get existing NFT", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		err = nftDb.UpdateSupply(tx, "cRead", "tRead", 10)
		require.NoError(t, err)

		require.NoError(t, tx.Commit())

		txCheck, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = txCheck.Rollback() }()

		nft, err := nftDb.GetNft(txCheck, "cRead", "tRead")
		require.NoError(t, err)
		assert.Equal(t, int64(10), nft.Supply)
		assert.Equal(t, int64(0), nft.BurntSupply)
	})

	t.Run("error if NFT not found", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		nft, err := nftDb.GetNft(tx, "unknownContract", "unknownToken")
		assert.Nil(t, nft)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "NFT not found")
	})
}

func TestNFTDb_ErrorOnTransactionFailure(t *testing.T) {
	// This test attempts to force an error from the DB by using a closed tx
	nftDb, db, cleanup := setupNFTTestDB(t)
	defer cleanup()

	t.Run("closed transaction should fail queries", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Rollback()) // immediately rollback

		// All subsequent queries should fail with "sql: transaction has already been committed or rolled back"
		err = nftDb.UpdateSupply(tx, "any", "any", 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")

		_, err = nftDb.GetNft(tx, "any", "any")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")
	})
}

func TestNFTDb_ManualQueryErrorInjection(t *testing.T) {
	// In some cases, you may want to test the code path where Exec returns an error
	// (e.g., invalid SQL, locked table, etc.). This is optional, but can help you push coverage
	// over the top by hitting zap.L().Error logs, etc.
	nftDb, db, cleanup := setupNFTTestDB(t)
	defer cleanup()

	t.Run("force invalid SQL to test error logging", func(t *testing.T) {
		// We'll do this by replacing the table name with something invalid
		// in a sub-transaction. This is a bit hacky but ensures coverage for the error path.
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		// Temporarily rename the table
		_, renameErr := tx.Exec(`ALTER TABLE nfts RENAME TO nfts_tmp`)
		if renameErr != nil {
			// If SQLite doesn't allow rename in transaction, skip
			t.Skipf("Skipping rename test: %v", renameErr)
		}
		defer func() {
			// Attempt to rename back
			_, _ = tx.Exec(`ALTER TABLE nfts_tmp RENAME TO nfts`)
		}()

		// Now the code that calls "INSERT INTO nfts ..." will fail because "nfts" no longer exists.
		err = nftDb.UpdateSupply(tx, "broken", "broken", 10)
		assert.Error(t, err)
	})
}
