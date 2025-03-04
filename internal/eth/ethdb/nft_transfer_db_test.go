package ethdb

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	sqlite "github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestTransferDb sets up a fresh DB, returns:
//   - db:      *sql.DB
//   - transferDb: NFTTransferDb (the SUT)
//   - cleanup: function to close DB and clean up
func setupTestTransferDb(t *testing.T) (*sql.DB, NFTTransferDb, func()) {
	db, cleanup := testdb.SetupTestDB(t)

	transferDb := &TransferDbImpl{}

	return db, transferDb, cleanup
}

func TestTransferDb_StoreTransfer_Basic(t *testing.T) {
	db, transferDb, cleanup := setupTestTransferDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfer := models.TokenTransfer{
		BlockNumber:      100,
		TransactionIndex: 1,
		LogIndex:         0,
		TxHash:           "0xTestHash",
		EventName:        "Transfer",
		From:             "0xAlice",
		To:               "0xBob",
		Contract:         "0xContractTest",
		TokenID:          "123",
		BlockTime:        1650001111,
		Type:             "MINT",
	}

	err = transferDb.StoreTransfer(tx, transfer, 999)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Verify that the row was actually stored, e.g., by calling GetLatestTransfer
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	latest, err := transferDb.GetLatestTransfer(txCheck)
	require.NoError(t, err)
	require.NotNil(t, latest, "Should have at least one transfer in table")

	assert.Equal(t, uint64(100), latest.BlockNumber)
	assert.Equal(t, uint64(1), latest.TransactionIndex)
	assert.Equal(t, uint64(0), latest.LogIndex)
	assert.Equal(t, "0xTestHash", latest.TxHash)
	assert.Equal(t, "Transfer", latest.EventName)
	assert.Equal(t, "0xAlice", latest.From)
	assert.Equal(t, "0xBob", latest.To)
	assert.Equal(t, "0xContractTest", latest.Contract)
	assert.Equal(t, "123", latest.TokenID)
	assert.Equal(t, uint64(999), latest.TokenUniqueID)
	assert.Equal(t, uint64(1650001111), latest.BlockTime)
	assert.Equal(t, models.MINT, latest.Type)

	_ = txCheck.Rollback()
}

func TestTransferDb_StoreTransfer_DuplicatePrimaryKey(t *testing.T) {
	// Because the primary key is (tx_hash, log_index, from_address, to_address, contract, token_id, token_unique_id),
	// attempting to store the exact same row should fail if the DB enforces that PK uniqueness.
	db, transferDb, cleanup := setupTestTransferDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfer := models.TokenTransfer{
		BlockNumber:      10,
		TransactionIndex: 0,
		LogIndex:         0,
		TxHash:           "0xDupHash",
		EventName:        "Transfer",
		From:             "0xA",
		To:               "0xB",
		Contract:         "0xC",
		TokenID:          "TID",
		BlockTime:        1650002222,
		Type:             "SEND",
	}
	err = transferDb.StoreTransfer(tx, transfer, 555)
	require.NoError(t, err, "First insert should succeed")

	// Insert same primary key
	err = transferDb.StoreTransfer(tx, transfer, 555)
	assert.Error(t, err, "Second insert with same PK must fail")

	_ = tx.Rollback()
}

func TestTransferDb_GetTransfersAfterCheckpoint_NoResults(t *testing.T) {
	db, transferDb, cleanup := setupTestTransferDb(t)
	defer cleanup()

	// Table is empty, so we expect no results
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfers, err := transferDb.GetTransfersAfterCheckpoint(tx, 100, 0, 0)
	require.NoError(t, err)
	assert.Empty(t, transfers, "No transfers should be returned in an empty table")

	_ = tx.Rollback()
}

func TestTransferDb_GetTransfersAfterCheckpoint_Basic(t *testing.T) {
	db, transferDb, cleanup := setupTestTransferDb(t)
	defer cleanup()

	// Insert multiple transfers, some before the checkpoint, some after
	allTransfers := []models.TokenTransfer{
		{BlockNumber: 90, TransactionIndex: 5, LogIndex: 0, TxHash: "0xHash1", EventName: "Transfer", From: "0xA", To: "0xB", Contract: "0xC", TokenID: "Token1", BlockTime: 1000, Type: "SEND"},
		{BlockNumber: 100, TransactionIndex: 0, LogIndex: 1, TxHash: "0xHash2", EventName: "Transfer", From: "0xAA", To: "0xBB", Contract: "0xCC", TokenID: "Token2", BlockTime: 2000, Type: "MINT"},
		{BlockNumber: 100, TransactionIndex: 1, LogIndex: 0, TxHash: "0xHash3", EventName: "Transfer", From: "0xAAA", To: "0xBBB", Contract: "0xCCC", TokenID: "Token3", BlockTime: 3000, Type: "AIRDROP"},
		{BlockNumber: 100, TransactionIndex: 1, LogIndex: 10, TxHash: "0xHash4", EventName: "Transfer", From: "0xAAAA", To: "0xBBBB", Contract: "0xCCCC", TokenID: "Token4", BlockTime: 4000, Type: "SEND"},
		{BlockNumber: 101, TransactionIndex: 0, LogIndex: 0, TxHash: "0xHash5", EventName: "Transfer", From: "0xAAAAA", To: "0xBBBBB", Contract: "0xCCCCC", TokenID: "Token5", BlockTime: 5000, Type: "SALE"},
	}

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	for i, tr := range allTransfers {
		err := transferDb.StoreTransfer(tx, tr, uint64(i+1))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	// We want everything strictly "after" blockNumber=100, or
	// "equal to 100 but txIndex > 1, or equal to 100 & txIndex=1 but logIndex >= ??"
	// But in the code, the condition is:
	//  block_number > ? OR
	//  (block_number = ? AND transaction_index > ?) OR
	//  (block_number = ? AND transaction_index = ? AND log_index >= ?)

	checkBlock := uint64(100)
	checkTxIndex := uint64(1)
	checkLogIndex := uint64(0) // be mindful that it's >= 0 for that last condition

	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	result, err := transferDb.GetTransfersAfterCheckpoint(tx2, checkBlock, checkTxIndex, checkLogIndex)
	require.NoError(t, err)
	require.NoError(t, tx2.Commit())

	// Let’s see which of the allTransfers pass the filter:
	// 1) #0 => blockNumber=90 => less than 100 => does NOT pass
	// 2) #1 => blockNumber=100, txIndex=0 => not > 1 => does NOT pass
	// 3) #2 => blockNumber=100, txIndex=1 => not > 1 => so next check => logIndex=0 => >= 0 => passes
	// 4) #3 => blockNumber=100, txIndex=1 => logIndex=10 => >=0 => passes
	// 5) #4 => blockNumber=101 => definitely > 100 => passes

	require.Len(t, result, 3, "We should get 3 transfers after the checkpoint")

	// Check the actual ones returned (order is not necessarily guaranteed by the query, unless we ORDER BY, but let's see)
	// If you want them in ascending order, adapt the test or query with an ORDER BY.
	var foundHash3, foundHash4, foundHash5 bool
	for _, r := range result {
		switch r.TxHash {
		case "0xHash3":
			foundHash3 = true
			assert.Equal(t, uint64(100), r.BlockNumber)
			assert.Equal(t, uint64(1), r.TransactionIndex)
			assert.Equal(t, uint64(0), r.LogIndex)
		case "0xHash4":
			foundHash4 = true
			assert.Equal(t, uint64(100), r.BlockNumber)
			assert.Equal(t, uint64(1), r.TransactionIndex)
			assert.Equal(t, uint64(10), r.LogIndex)
		case "0xHash5":
			foundHash5 = true
			assert.Equal(t, uint64(101), r.BlockNumber)
			assert.Equal(t, uint64(0), r.TransactionIndex)
			assert.Equal(t, uint64(0), r.LogIndex)
		default:
			assert.Fail(t, "Unexpected TxHash in results: %s", r.TxHash)
		}
	}
	assert.True(t, foundHash3 && foundHash4 && foundHash5, "Should find those 3 hashes in results")
}

func TestTransferDb_DeleteTransfer_Basic(t *testing.T) {
	db, transferDb, cleanup := setupTestTransferDb(t)
	defer cleanup()

	// Insert a few transfers
	txs := []models.TokenTransfer{
		{BlockNumber: 50, TransactionIndex: 0, LogIndex: 0, TxHash: "0xOld1", From: "A", To: "B", Contract: "C", TokenID: "1", Type: "SEND"},
		{BlockNumber: 100, TransactionIndex: 0, LogIndex: 1, TxHash: "0xMid1", From: "A2", To: "B2", Contract: "C2", TokenID: "2", Type: "MINT"},
		{BlockNumber: 100, TransactionIndex: 1, LogIndex: 5, TxHash: "0xMid2", From: "A3", To: "B3", Contract: "C3", TokenID: "3", Type: "SALE"},
		{BlockNumber: 101, TransactionIndex: 0, LogIndex: 0, TxHash: "0xNew1", From: "A4", To: "B4", Contract: "C4", TokenID: "4", Type: "SEND"},
		{BlockNumber: 101, TransactionIndex: 2, LogIndex: 0, TxHash: "0xNew2", From: "A5", To: "B5", Contract: "C5", TokenID: "5", Type: "MINT"},
		{BlockNumber: 101, TransactionIndex: 2, LogIndex: 1, TxHash: "0xNew3", From: "A6", To: "B6", Contract: "C6", TokenID: "6", Type: "SEND"},
	}

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	for i, tr := range txs {
		err := transferDb.StoreTransfer(tx, tr, uint64(i+10))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	txDel, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfersToDelete, err := transferDb.GetTransfersAfterCheckpoint(txDel, 100, 1, 0)
	require.NoError(t, err)

	for _, xfer := range transfersToDelete {
		err := transferDb.DeleteTransfer(txDel, *xfer, xfer.TokenUniqueID)
		require.NoError(t, err)
	}

	require.NoError(t, txDel.Commit())

	// Now anything with blockNumber>100 should be removed, or blockNumber=100 and txIndex>1, etc.
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	allAfter, err := transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0) // basically get everything
	require.NoError(t, err)

	_ = txCheck.Rollback()

	// Original set (with blockNumber, txIndex, logIndex):
	// - #0 => blockNumber=50 => should remain
	// - #1 => blockNumber=100, txIndex=0 => should remain
	// - #2 => blockNumber=100, txIndex=1, logIndex=5 => compare to checkpoint(100,1,0):
	//         (block=100=100, txIndex=1=1, logIndex=5 >=0) => that means "after checkpoint"
	//         => should be deleted
	// - #3 => blockNumber=101 => definitely after => should be deleted
	// - #4 => blockNumber=101 => definitely after => should be deleted

	expectRemaining := map[string]bool{
		"0xOld1": true, // #0
		"0xMid1": true, // #1
	}
	expectDeleted := map[string]bool{
		"0xMid2": true,
		"0xNew1": true,
		"0xNew2": true,
	}

	actualRemaining := make(map[string]bool)
	for _, xfer := range allAfter {
		actualRemaining[xfer.TxHash] = true
	}

	// Check each expected remain
	for txHash := range expectRemaining {
		_, found := actualRemaining[txHash]
		assert.True(t, found, "Expected transfer %s to remain in DB", txHash)
	}

	// Check each expected deletion
	for txHash := range expectDeleted {
		_, found := actualRemaining[txHash]
		assert.False(t, found, "Expected transfer %s to be deleted", txHash)
	}
}

func TestTransferDb_GetLatestTransfer_EmptyTable(t *testing.T) {
	db, transferDb, cleanup := setupTestTransferDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	latest, err := transferDb.GetLatestTransfer(tx)
	require.NoError(t, err)
	assert.Nil(t, latest, "Should get nil when table is empty")

	_ = tx.Rollback()
}

func TestTransferDb_GetLatestTransfer_Basic(t *testing.T) {
	db, transferDb, cleanup := setupTestTransferDb(t)
	defer cleanup()

	// Insert multiple transfers with different block/txIndex/logIndex combos
	testTransfers := []models.TokenTransfer{
		{BlockNumber: 10, TransactionIndex: 2, LogIndex: 5, TxHash: "0xEarly", Type: "SEND"},
		{BlockNumber: 15, TransactionIndex: 0, LogIndex: 0, TxHash: "0xMid", Type: "SEND"},
		{BlockNumber: 15, TransactionIndex: 0, LogIndex: 1, TxHash: "0xMid2", Type: "SEND"},
		{BlockNumber: 15, TransactionIndex: 2, LogIndex: 0, TxHash: "0xMid3", Type: "SEND"},
		{BlockNumber: 20, TransactionIndex: 0, LogIndex: 10, TxHash: "0xLatest", Type: "MINT"},
		// We'll also put something with blockNumber=20 but a smaller txIndex or logIndex
		{BlockNumber: 20, TransactionIndex: 0, LogIndex: 5, TxHash: "0xAlmostLatest", Type: "SALE"},
	}

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	for i, tf := range testTransfers {
		err := transferDb.StoreTransfer(tx, tf, uint64(i+100))
		require.NoError(t, err)
	}

	require.NoError(t, tx.Commit())

	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// The code orders by block_number DESC, transaction_index DESC, log_index DESC LIMIT 1
	// => The top candidate is blockNumber=20, transactionIndex=0, logIndex=10 => "0xLatest"
	latest, err := transferDb.GetLatestTransfer(txCheck)
	require.NoError(t, err)
	require.NotNil(t, latest, "Should find a record")

	assert.Equal(t, "0xLatest", latest.TxHash, "Should retrieve the highest blockNumber/txIndex/logIndex record")
	assert.Equal(t, uint64(10), latest.LogIndex)
	assert.Equal(t, uint64(0), latest.TransactionIndex)
	assert.Equal(t, uint64(20), latest.BlockNumber)
	assert.Equal(t, models.MINT, latest.Type)

	_ = txCheck.Rollback()
}

func TestTransferDb_ClosedDbBehavior(t *testing.T) {
	db, transferDb, cleanup := setupTestTransferDb(t)
	// close DB
	cleanup()

	// Now any operation should fail
	tx, err := db.BeginTx(context.Background(), nil)
	if err == nil {
		// If by chance we got a transaction handle, let's see if queries fail
		transfer := models.TokenTransfer{
			BlockNumber:      999,
			TransactionIndex: 999,
			LogIndex:         999,
			TxHash:           "0xClosedHash",
			From:             "0xAlice",
			To:               "0xBob",
			Contract:         "0xC",
			TokenID:          "TID",
			BlockTime:        999999,
			Type:             "SEND",
		}
		err2 := transferDb.StoreTransfer(tx, transfer, 123)
		assert.Error(t, err2, "Expected an error with a closed DB/transaction")

		_ = tx.Rollback()
	} else {
		assert.Error(t, err, "Expected error when attempting to beginTx on a closed DB")
	}
}

func TestTransferDb_ConcurrentAccess(t *testing.T) {
	db, transferDb, cleanup := setupTestTransferDb(t)
	defer cleanup()

	var wg sync.WaitGroup

	storeWorker := func(i int) {
		defer wg.Done()
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)

		xfer := models.TokenTransfer{
			BlockNumber:      uint64(100 + i),
			TransactionIndex: uint64(i),
			LogIndex:         0,
			TxHash:           "0xTxHashMulti",
			From:             "0xConcurrentA",
			To:               "0xConcurrentB",
			Contract:         "0xSomeContract",
			TokenID:          "TokenX",
			BlockTime:        uint64(100000 + int64(i)),
			Type:             "SEND",
		}
		err = transferDb.StoreTransfer(tx, xfer, uint64(i+1000))
		assert.NoError(t, err)

		err = tx.Commit()
		assert.NoError(t, err)
	}

	// Launch multiple goroutines to insert different blockNumbers
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go storeWorker(i)
	}

	wg.Wait()

	// Check we have 5 distinct blockNumbers from 100..104
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfers, err := transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
	require.NoError(t, err)
	_ = txCheck.Rollback()

	blockNums := make(map[uint64]bool)
	for _, t := range transfers {
		if t.TxHash == "0xTxHashMulti" {
			blockNums[t.BlockNumber] = true
		}
	}
	// We expect blockNumbers=100..104
	for i := 0; i < 5; i++ {
		_, found := blockNums[uint64(100+i)]
		assert.True(t, found, "Expected to find blockNumber %d in results", 100+i)
	}
}

func TestTransferDb_GetPaginatedResponseForQuery_Basic(t *testing.T) {
	db, transferDb, cleanup := setupTestTransferDb(t)
	defer cleanup()

	// Prepare a collection of NFTTransfer records.
	// Note: Some records share the same contract, tokenID, or txHash for filtering.
	testTransfers := []models.TokenTransfer{
		{
			BlockNumber:      10,
			TransactionIndex: 1,
			LogIndex:         0,
			TxHash:           "0xA",
			EventName:        "Mint",
			From:             "",
			To:               "0xRecipient1",
			Contract:         "0xContract1",
			TokenID:          "101",
			BlockTime:        1000,
			Type:             "MINT",
		},
		{
			BlockNumber:      12,
			TransactionIndex: 0,
			LogIndex:         2,
			TxHash:           "0xB",
			EventName:        "Sale",
			From:             "0xSender1",
			To:               "0xRecipient2",
			Contract:         "0xContract2",
			TokenID:          "201",
			BlockTime:        1200,
			Type:             "SALE",
		},
		{
			BlockNumber:      15,
			TransactionIndex: 2,
			LogIndex:         3,
			TxHash:           "0xC",
			EventName:        "Transfer",
			From:             "0xSender2",
			To:               "0xRecipient3",
			Contract:         "0xContract1",
			TokenID:          "102",
			BlockTime:        1500,
			Type:             "SEND",
		},
		{
			BlockNumber:      20,
			TransactionIndex: 1,
			LogIndex:         0,
			TxHash:           "0xD",
			EventName:        "Mint",
			From:             "",
			To:               "0xRecipient4",
			Contract:         "0xTarget",
			TokenID:          "501",
			BlockTime:        2000,
			Type:             "MINT",
		},
		{
			BlockNumber:      22,
			TransactionIndex: 0,
			LogIndex:         1,
			TxHash:           "0xE",
			EventName:        "Sale",
			From:             "0xSender3",
			To:               "0xRecipient5",
			Contract:         "0xOther",
			TokenID:          "601",
			BlockTime:        2200,
			Type:             "SALE",
		},
		{
			BlockNumber:      25,
			TransactionIndex: 2,
			LogIndex:         3,
			TxHash:           "0xF",
			EventName:        "Transfer",
			From:             "0xSender4",
			To:               "0xRecipient6",
			Contract:         "0xTarget",
			TokenID:          "502",
			BlockTime:        2500,
			Type:             "SEND",
		},
		{
			BlockNumber:      30,
			TransactionIndex: 0,
			LogIndex:         0,
			TxHash:           "0xG",
			EventName:        "Mint",
			From:             "",
			To:               "0xRecipient7",
			Contract:         "0xCT",
			TokenID:          "1001",
			BlockTime:        3000,
			Type:             "MINT",
		},
		{
			BlockNumber:      32,
			TransactionIndex: 1,
			LogIndex:         1,
			TxHash:           "0xH",
			EventName:        "Sale",
			From:             "0xSender5",
			To:               "0xRecipient8",
			Contract:         "0xCT",
			TokenID:          "1002",
			BlockTime:        3200,
			Type:             "SALE",
		},
		{
			BlockNumber:      35,
			TransactionIndex: 2,
			LogIndex:         2,
			TxHash:           "0xI",
			EventName:        "Transfer",
			From:             "0xSender6",
			To:               "0xRecipient9",
			Contract:         "0xCT",
			TokenID:          "1001",
			BlockTime:        3500,
			Type:             "SEND",
		},
		{
			BlockNumber:      40,
			TransactionIndex: 0,
			LogIndex:         0,
			TxHash:           "0xTargetTx",
			EventName:        "Mint",
			From:             "",
			To:               "0xRecipient10",
			Contract:         "0xCT1",
			TokenID:          "2001",
			BlockTime:        4000,
			Type:             "MINT",
		},
		{
			BlockNumber:      42,
			TransactionIndex: 1,
			LogIndex:         1,
			TxHash:           "0xOtherTx",
			EventName:        "Sale",
			From:             "0xSender7",
			To:               "0xRecipient11",
			Contract:         "0xCT2",
			TokenID:          "2002",
			BlockTime:        4200,
			Type:             "SALE",
		},
		{
			BlockNumber:      45,
			TransactionIndex: 2,
			LogIndex:         2,
			TxHash:           "0xTargetTx",
			EventName:        "Transfer",
			From:             "0xSender8",
			To:               "0xRecipient12",
			Contract:         "0xCT1",
			TokenID:          "2003",
			BlockTime:        4500,
			Type:             "SEND",
		},
	}

	// Insert all records in one transaction.
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	for i, nft := range testTransfers {
		err := transferDb.StoreTransfer(tx, nft, uint64(i+100))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	// Open a new transaction for querying.
	txQuery, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// 1. Test GetAllTransfers.
	queryOptions := sqlite.QueryOptions{
		Direction: sqlite.QueryDirectionAsc,
		Page:      1,
		PageSize:  20,
	}
	queryParams := []interface{}{}
	totalAll, allTransfers, err := transferDb.GetPaginatedResponseForQuery(txQuery, queryOptions, queryParams)
	require.NoError(t, err)
	assert.Equal(t, len(testTransfers), totalAll, "Total count should equal all inserted records")
	assert.Len(t, allTransfers, len(testTransfers), "Should return all records")

	// 2. Test GetTransfersForContract (for contract "0xTarget").
	queryOptions = sqlite.QueryOptions{
		Where:     "contract = ?",
		Direction: sqlite.QueryDirectionAsc,
		Page:      1,
		PageSize:  20,
	}
	queryParams = []interface{}{"0xTarget"}
	totalTarget, targetTransfers, err := transferDb.GetPaginatedResponseForQuery(txQuery, queryOptions, queryParams)
	require.NoError(t, err)
	assert.Equal(t, 2, totalTarget, "Should have 2 records for contract 0xTarget")
	for _, rec := range targetTransfers {
		assert.Equal(t, "0xTarget", rec.Contract)
	}

	// // 3. Test GetTransfersForContractToken (for contract "0xCT" and tokenID "1001").
	queryOptions = sqlite.QueryOptions{
		Where:     "contract = ? AND token_id = ?",
		Direction: sqlite.QueryDirectionAsc,
		Page:      1,
		PageSize:  20,
	}
	queryParams = []interface{}{"0xCT", "1001"}
	totalCT, ctTransfers, err := transferDb.GetPaginatedResponseForQuery(txQuery, queryOptions, queryParams)
	require.NoError(t, err)
	assert.Equal(t, 2, totalCT, "Should have 2 records for contract 0xCT with tokenID 1001")
	for _, rec := range ctTransfers {
		assert.Equal(t, "0xCT", rec.Contract)
		assert.Equal(t, "1001", rec.TokenID)
	}

	// // 4. Test GetTransfersForTxHash (for txHash "0xTargetTx").
	queryOptions = sqlite.QueryOptions{
		Where:     "tx_hash = ?",
		Direction: sqlite.QueryDirectionAsc,
		Page:      1,
		PageSize:  20,
	}
	queryParams = []interface{}{"0xTargetTx"}
	totalTx, txTransfers, err := transferDb.GetPaginatedResponseForQuery(txQuery, queryOptions, queryParams)
	require.NoError(t, err)
	assert.Equal(t, 2, totalTx, "Should have 2 records for txHash 0xTargetTx")
	for _, rec := range txTransfers {
		assert.Equal(t, "0xTargetTx", rec.TxHash)
	}
}

func TestTransferDb_GetPaginatedResponseForQuery_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err, "error creating sqlmock")
	defer db.Close()

	// Begin transaction
	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	// Mock the SELECT query to fail
	mock.ExpectQuery(`(?i)SELECT\s+.*\s+FROM\s+nft_transfers\s+WHERE\s+contract\s+=\s+\?\s+ORDER\s+BY\s+block_number\s+ASC,\s+transaction_index\s+ASC,\s+log_index\s+ASC\s+LIMIT\s+\?\s+OFFSET\s+\?`).
		WillReturnError(errors.New("query error"))

	transferDb := NewTransferDb()

	queryOptions := sqlite.QueryOptions{
		Where:     "contract = ?",
		Direction: sqlite.QueryDirectionAsc,
		Page:      1,
		PageSize:  20,
	}
	queryParams := []interface{}{"0xabc"}
	total, transfers, err := transferDb.GetPaginatedResponseForQuery(tx, queryOptions, queryParams)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query error")
	assert.Equal(t, 0, total)
	assert.Nil(t, transfers)
}

func TestTransferDb_GetPaginatedResponseForQuery_RowScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)
	mock.ExpectQuery(`(?i)SELECT\s+.*\s+FROM\s+nft_transfers\s+ORDER BY\s+block_number\s+ASC,.*LIMIT\s+\?\s+OFFSET\s+\?`).WillReturnRows(
		sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index", "tx_hash", "event_name", "from_address", "to_address", "contract", "token_id", "token_unique_id", "block_time", "transfer_type"}).
			AddRow(1, 1, 1, "0x1", "Mint", "0x1", "0x2", "0x3", "100", "100", 1000, "MINT").
			RowError(0, errors.New("row scan error")),
	)

	transferDb := &TransferDbImpl{}

	queryOptions := sqlite.QueryOptions{
		Direction: sqlite.QueryDirectionAsc,
		Page:      1,
		PageSize:  20,
	}
	queryParams := []interface{}{}
	total, transfers, err := transferDb.GetPaginatedResponseForQuery(tx, queryOptions, queryParams)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row scan error")
	assert.Equal(t, 0, total)
	assert.Nil(t, transfers)
}

func TestTransferDb_GetPaginatedResponseForQuery_TotalCountError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err, "error creating sqlmock")
	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectQuery(`(?i)SELECT\s+.*\s+FROM\s+nft_transfers\s+ORDER BY\s+block_number\s+ASC,.*LIMIT\s+\?\s+OFFSET\s+\?`).WillReturnRows(
		sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index", "tx_hash", "event_name", "from_address", "to_address", "contract", "token_id", "token_unique_id", "block_time", "transfer_type"}).
			AddRow(1, 1, 1, "0x1", "Mint", "0x1", "0x2", "0x3", "100", "100", 1000, "MINT"),
	)

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM nft_transfers`).
		WillReturnError(errors.New("total count error"))

	transferDb := &TransferDbImpl{}

	queryOptions := sqlite.QueryOptions{
		Where:     "",
		Direction: sqlite.QueryDirectionAsc,
		Page:      1,
		PageSize:  20,
	}
	queryParams := []interface{}{}
	total, transfers, err := transferDb.GetPaginatedResponseForQuery(tx, queryOptions, queryParams)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "total count error")
	assert.Equal(t, 0, total)
	assert.Nil(t, transfers)
}
