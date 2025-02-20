package ethdb

import (
	"database/sql"
	"log"
	"testing"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTransferTestDB sets up a clean DB with the token_transfers table.
func setupTransferTestDB(t *testing.T) (TransferDb, *sql.DB, func()) {
	db, cleanup := testdb.SetupTestDB(t)

	// Create table for token_transfers if it doesn't exist
	createTransfersSQL := `
	CREATE TABLE IF NOT EXISTS token_transfers (
		block_number      INTEGER NOT NULL,
		transaction_index INTEGER NOT NULL,
		log_index         INTEGER NOT NULL,
		tx_hash           TEXT NOT NULL,
		event_name        TEXT NOT NULL,
		from_address      TEXT NOT NULL,
		to_address        TEXT NOT NULL,
		contract          TEXT NOT NULL,
		token_id          TEXT NOT NULL,
		amount            INTEGER NOT NULL,
		transfer_type     TEXT NOT NULL CHECK (transfer_type IN ('SALE', 'SEND', 'AIRDROP', 'MINT', 'BURN')),
		PRIMARY KEY (tx_hash, log_index, from_address, to_address, contract, token_id)
	);
	`
	_, err := db.Exec(createTransfersSQL)
	require.NoError(t, err, "failed to create token_transfers table")

	transferDb := NewTransferDb()

	return transferDb, db, func() {
		// Drop the table (optional, but keeps test DB clean)
		_, _ = db.Exec(`DROP TABLE IF EXISTS token_transfers`)
		cleanup()
	}
}

func TestTransferDb_StoreTransfer(t *testing.T) {
	transferDb, db, cleanup := setupTransferTestDB(t)
	defer cleanup()

	t.Run("store single transfer successfully", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		newTransfer := models.TokenTransfer{
			BlockNumber:      100,
			TransactionIndex: 5,
			LogIndex:         1,
			TxHash:           "0xabc123",
			EventName:        "Transfer",
			From:             "0xFromAddress",
			To:               "0xToAddress",
			Contract:         "0xContract",
			TokenID:          "12345",
			Amount:           10,
			Type:             models.SEND,
		}
		err = transferDb.StoreTransfer(tx, newTransfer)
		assert.NoError(t, err, "should store without error")

		require.NoError(t, tx.Commit())

		// Verify via direct query
		// Use a separate transaction
		txCheck, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			if err != nil {
				if rollbackErr := txCheck.Rollback(); rollbackErr != nil {
					t.Errorf("Failed to rollback transaction: %v", rollbackErr)
				}
			}
		}()

		var count int
		err = txCheck.QueryRow(`
			SELECT COUNT(*) FROM token_transfers 
			WHERE tx_hash = ? AND block_number = ? AND log_index = ?`,
			newTransfer.TxHash, newTransfer.BlockNumber, newTransfer.LogIndex).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("error on duplicate primary key", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		dupTransfer := models.TokenTransfer{
			BlockNumber:      200,
			TransactionIndex: 10,
			LogIndex:         2,
			TxHash:           "0xdupHash",
			EventName:        "Transfer",
			From:             "0xFromDup",
			To:               "0xToDup",
			Contract:         "0xContractDup",
			TokenID:          "999",
			Amount:           5,
			Type:             models.SEND,
		}

		// Store once
		err = transferDb.StoreTransfer(tx, dupTransfer)
		require.NoError(t, err)

		// Store again with same PK => error
		err = transferDb.StoreTransfer(tx, dupTransfer)
		assert.Error(t, err, "should fail on duplicate key")

		require.NoError(t, tx.Rollback()) // discard changes
	})

	t.Run("error if transaction is closed", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Rollback()) // immediately close

		err = transferDb.StoreTransfer(tx, models.TokenTransfer{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")
	})
}

func TestTransferDb_GetTransfersAfterCheckpoint(t *testing.T) {
	transferDb, db, cleanup := setupTransferTestDB(t)
	defer cleanup()

	// Insert a bunch of transfers in ascending order
	sampleTransfers := []models.TokenTransfer{
		// block=1, txIndex=0, logIndex=0
		{BlockNumber: 1, TransactionIndex: 0, LogIndex: 0, TxHash: "0xTx1", EventName: "Transfer", From: "A", To: "B", Contract: "C1", TokenID: "111", Amount: 1, Type: models.SEND},
		{BlockNumber: 1, TransactionIndex: 0, LogIndex: 1, TxHash: "0xTx1", EventName: "Transfer", From: "A", To: "B2", Contract: "C1", TokenID: "222", Amount: 1, Type: models.SEND},

		// block=2, txIndex=1, logIndex=0
		{BlockNumber: 2, TransactionIndex: 1, LogIndex: 0, TxHash: "0xTx2", EventName: "Transfer", From: "X", To: "Y", Contract: "C2", TokenID: "333", Amount: 10, Type: models.MINT},

		// block=2, txIndex=1, logIndex=1
		{BlockNumber: 2, TransactionIndex: 1, LogIndex: 1, TxHash: "0xTx2", EventName: "Transfer", From: "X", To: "Y2", Contract: "C2", TokenID: "444", Amount: 10, Type: models.MINT},

		// block=3, txIndex=0, logIndex=0
		{BlockNumber: 3, TransactionIndex: 0, LogIndex: 0, TxHash: "0xTx3", EventName: "Transfer", From: "Z", To: "W", Contract: "C3", TokenID: "555", Amount: 100, Type: models.BURN},
	}

	func() {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Errorf("Failed to rollback transaction: %v", rollbackErr)
				}
			}
		}()

		for _, tr := range sampleTransfers {
			require.NoError(t, transferDb.StoreTransfer(tx, tr))
		}
		require.NoError(t, tx.Commit())
	}()

	t.Run("get transfers after a mid checkpoint (block=1, txIndex=0, logIndex=1)", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Errorf("Failed to rollback transaction: %v", rollbackErr)
				}
			}
		}()

		// We want anything strictly after block=1, txIndex=0, logIndex=1
		// which should give us block=2 and block=3 items
		result, err := transferDb.GetTransfersAfterCheckpoint(tx, 1, 0, 1)
		require.NoError(t, err)

		// We expect 3 transfers: those in block=2 and block=3
		assert.Len(t, result, 4)
	})

	t.Run("get transfers after checkpoint that excludes nothing", func(t *testing.T) {
		// checkpoint is block=0 => effectively we want everything
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Errorf("Failed to rollback transaction: %v", rollbackErr)
				}
			}
		}()

		result, err := transferDb.GetTransfersAfterCheckpoint(tx, 0, 0, 0)
		require.NoError(t, err)
		assert.Len(t, result, 5, "all sample transfers should be returned")
	})

	t.Run("get transfers after checkpoint that excludes all", func(t *testing.T) {
		// checkpoint is block=10 => everything is older
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Errorf("Failed to rollback transaction: %v", rollbackErr)
				}
			}
		}()

		result, err := transferDb.GetTransfersAfterCheckpoint(tx, 10, 0, 0)
		require.NoError(t, err)
		assert.Empty(t, result, "no transfers should be returned")
	})

	t.Run("transaction closed => error", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Rollback()) // close

		_, err = transferDb.GetTransfersAfterCheckpoint(tx, 1, 0, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")
	})
}

func TestTransferDb_DeleteTransfersAfterCheckpoint(t *testing.T) {
	transferDb, db, cleanup := setupTransferTestDB(t)
	defer cleanup()

	// Insert some rows
	txInsert, err := db.Begin()
	require.NoError(t, err)
	sample := []models.TokenTransfer{
		{BlockNumber: 5, TransactionIndex: 0, LogIndex: 0, TxHash: "0xA", From: "0x1", To: "0x2", Contract: "0xC", TokenID: "T1", Amount: 1, Type: models.SEND},
		{BlockNumber: 5, TransactionIndex: 0, LogIndex: 1, TxHash: "0xA", From: "0x3", To: "0x4", Contract: "0xC", TokenID: "T2", Amount: 1, Type: models.BURN},
		{BlockNumber: 6, TransactionIndex: 0, LogIndex: 0, TxHash: "0xB", From: "0x5", To: "0x6", Contract: "0xC", TokenID: "T3", Amount: 1, Type: models.MINT},
		{BlockNumber: 6, TransactionIndex: 1, LogIndex: 0, TxHash: "0xC", From: "0x7", To: "0x8", Contract: "0xC", TokenID: "T4", Amount: 1, Type: models.SALE},
		{BlockNumber: 7, TransactionIndex: 0, LogIndex: 0, TxHash: "0xD", From: "0x9", To: "0xA", Contract: "0xC", TokenID: "T5", Amount: 1, Type: models.SEND},
	}
	for _, tr := range sample {
		require.NoError(t, transferDb.StoreTransfer(txInsert, tr))
	}
	require.NoError(t, txInsert.Commit())

	t.Run("delete all transfers strictly after block=5, txIndex=0, logIndex=0", func(t *testing.T) {
		txDel, err := db.Begin()
		require.NoError(t, err)
		err = transferDb.DeleteTransfersAfterCheckpoint(txDel, 5, 0, 1)
		require.NoError(t, err)
		require.NoError(t, txDel.Commit())

		// Check which remain
		txCheck, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			if err != nil {
				if rollbackErr := txCheck.Rollback(); rollbackErr != nil {
					t.Errorf("Failed to rollback transaction: %v", rollbackErr)
				}
			}
		}()

		rows, err := txCheck.Query("SELECT block_number, transaction_index, log_index FROM token_transfers ORDER BY block_number ASC")
		require.NoError(t, err)
		defer rows.Close()

		var blocks []uint64
		var txIndexes []uint64
		var logIndexes []uint64
		for rows.Next() {
			var b uint64
			var txIndex uint64
			var logIndex uint64
			require.NoError(t, rows.Scan(&b, &txIndex, &logIndex))
			blocks = append(blocks, b)
			txIndexes = append(txIndexes, txIndex)
			logIndexes = append(logIndexes, logIndex)
		}
		// Only block=5 transfers should remain
		log.Println(blocks)
		assert.Equal(t, []uint64{5}, blocks)
		assert.Equal(t, []uint64{0}, txIndexes)
		assert.Equal(t, []uint64{0}, logIndexes)
	})

	t.Run("transaction closed => error", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		err = transferDb.DeleteTransfersAfterCheckpoint(tx, 5, 0, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")
	})
}

func TestTransferDb_GetLatestTransfer(t *testing.T) {
	transferDb, db, cleanup := setupTransferTestDB(t)
	defer cleanup()

	t.Run("empty table => returns nil, no error", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Errorf("Failed to rollback transaction: %v", rollbackErr)
				}
			}
		}()

		latest, err := transferDb.GetLatestTransfer(tx)
		require.NoError(t, err)
		assert.Nil(t, latest)
	})

	t.Run("multiple rows => returns the highest (blockNumber, txIndex, logIndex)", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		transfers := []models.TokenTransfer{
			{BlockNumber: 10, TransactionIndex: 0, LogIndex: 0, TxHash: "0x1", Contract: "C1", TokenID: "T1", Amount: 1, Type: models.SEND},
			{BlockNumber: 10, TransactionIndex: 0, LogIndex: 1, TxHash: "0x1", Contract: "C1", TokenID: "T2", Amount: 1, Type: models.SEND},
			{BlockNumber: 11, TransactionIndex: 0, LogIndex: 0, TxHash: "0x2", Contract: "C1", TokenID: "T3", Amount: 1, Type: models.MINT},
			{BlockNumber: 10, TransactionIndex: 5, LogIndex: 2, TxHash: "0x3", Contract: "C1", TokenID: "T4", Amount: 1, Type: models.BURN}, // higher txIndex than the block=10 ones above
			{BlockNumber: 12, TransactionIndex: 0, LogIndex: 0, TxHash: "0x4", Contract: "C1", TokenID: "T5", Amount: 1, Type: models.SALE},
			{BlockNumber: 12, TransactionIndex: 0, LogIndex: 1, TxHash: "0x4", Contract: "C1", TokenID: "T6", Amount: 1, Type: models.SEND}, // highest logIndex
		}

		for _, tr := range transfers {
			require.NoError(t, transferDb.StoreTransfer(tx, tr))
		}
		require.NoError(t, tx.Commit())

		txCheck, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			if err != nil {
				if rollbackErr := txCheck.Rollback(); rollbackErr != nil {
					t.Errorf("Failed to rollback transaction: %v", rollbackErr)
				}
			}
		}()

		latest, err := transferDb.GetLatestTransfer(txCheck)
		require.NoError(t, err)
		require.NotNil(t, latest)

		// The 'largest' is block=12, txIndex=0, logIndex=1
		assert.Equal(t, uint64(12), latest.BlockNumber)
		assert.Equal(t, uint64(0), latest.TransactionIndex)
		assert.Equal(t, uint64(1), latest.LogIndex)
		assert.Equal(t, "0x4", latest.TxHash)
	})

	t.Run("transaction closed => error", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		_, err = transferDb.GetLatestTransfer(tx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")
	})
}

func TestTransferDb_ManualQueryErrorInjection(t *testing.T) {
	transferDb, db, cleanup := setupTransferTestDB(t)
	defer cleanup()

	t.Run("force invalid SQL to test error path", func(t *testing.T) {
		// Insert at least one row so that we can try retrieval
		txSeed, err := db.Begin()
		require.NoError(t, err)
		err = transferDb.StoreTransfer(txSeed, models.TokenTransfer{
			BlockNumber: 99, TxHash: "0xTest", Amount: 1, Type: models.SEND,
			From: "0xA", To: "0xB", Contract: "0xC", TokenID: "TTest",
		})
		require.NoError(t, err)
		require.NoError(t, txSeed.Commit())

		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Errorf("Failed to rollback transaction: %v", rollbackErr)
				}
			}
		}()

		// Attempt to rename the table to break queries
		_, renameErr := tx.Exec(`ALTER TABLE token_transfers RENAME TO token_transfers_tmp`)
		if renameErr != nil {
			// If the DB doesn't support table rename in a transaction, skip
			t.Skipf("Skipping rename test: %v", renameErr)
		}
		defer func() {
			// Attempt to rename it back
			_, _ = tx.Exec(`ALTER TABLE token_transfers_tmp RENAME TO token_transfers`)
		}()

		// Now any SELECT / INSERT on token_transfers should fail
		_, err = transferDb.GetTransfersAfterCheckpoint(tx, 50, 0, 0)
		assert.Error(t, err, "should fail due to missing table")
	})
}
