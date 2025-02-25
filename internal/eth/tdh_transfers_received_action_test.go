package eth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/6529-Collections/6529node/internal/eth/ethdb"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
)

type testDefaultTdhTransfersReceivedActionDeps struct {
	db              *sql.DB
	progressTracker ethdb.TdhIdxTrackerDb
	transferDb      ethdb.TransferDb
	ownerDb         ethdb.OwnerDb
	nftDb           ethdb.NFTDb
	cleanup         func()
}

func setupTestDefaultTdhTransfersReceivedActionDeps(t *testing.T) *testDefaultTdhTransfersReceivedActionDeps {
	db, cleanup := testdb.SetupTestDB(t)

	progressTracker := ethdb.NewTdhIdxTrackerDb(db)
	transferDb := ethdb.NewTransferDb()
	ownerDb := ethdb.NewOwnerDb()
	nftDb := ethdb.NewNFTDb()

	return &testDefaultTdhTransfersReceivedActionDeps{
		db:              db,
		progressTracker: progressTracker,
		transferDb:      transferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
		cleanup:         cleanup,
	}
}

func setupTestDefaultTdhTransfersReceivedActionDepsUsingDB(db *sql.DB) *testDefaultTdhTransfersReceivedActionDeps {
	progressTracker := ethdb.NewTdhIdxTrackerDb(db)
	transferDb := ethdb.NewTransferDb()
	ownerDb := ethdb.NewOwnerDb()
	nftDb := ethdb.NewNFTDb()

	return &testDefaultTdhTransfersReceivedActionDeps{
		db:              db,
		progressTracker: progressTracker,
		transferDb:      transferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
		cleanup: func() {
			_ = db.Close()
		},
	}
}

func newDefaultTdhTransfersReceivedAction(t *testing.T, deps *testDefaultTdhTransfersReceivedActionDeps) *DefaultTdhTransfersReceivedAction {
	ctx := context.Background()
	orchestrator := NewTdhTransfersReceivedActionImpl(
		ctx,
		deps.db,
		deps.transferDb,
		deps.ownerDb,
		deps.nftDb,
		deps.progressTracker,
	)
	require.NotNil(t, orchestrator, "Should successfully create orchestrator instance")
	return orchestrator
}

func TestDefaultTdhTransfersReceivedAction_NoTransfers(t *testing.T) {
	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
	defer deps.cleanup()

	o := newDefaultTdhTransfersReceivedAction(t, deps)

	// Provide an empty batch
	batch := models.TokenTransferBatch{
		Transfers:   []models.TokenTransfer{},
		BlockNumber: 999,
	}
	err := o.Handle(batch)
	require.NoError(t, err, "Handling an empty batch should not fail")

	// Final progress should be updated to 999 (the blockNumber in the batch),
	// but the orchestrator calls SetProgress only if there's at least 1 transfer.
	// The code does "if len(transfersBatch.Transfers) == 0 => return nil" before
	// calling progressTracker.SetProgress. So we expect no progress update.
	progress, dbErr := deps.progressTracker.GetProgress()
	require.NoError(t, dbErr)
	assert.Equal(t, uint64(0), progress, "No transfers => no progress update")
}

func TestDefaultTdhTransfersReceivedAction_SingleTransfer_Mint(t *testing.T) {
	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
	defer deps.cleanup()

	o := newDefaultTdhTransfersReceivedAction(t, deps)

	transfer := models.TokenTransfer{
		From:             "", // typical for mint
		To:               "0xMintReceiver",
		Contract:         "0xMintContract",
		TokenID:          "TokenA",
		Amount:           1, // For MINT, we might have multiple minted in one go
		BlockNumber:      100,
		TransactionIndex: 0,
		LogIndex:         0,
		BlockTime:        123456,
		Type:             models.MINT,
	}

	batch := models.TokenTransferBatch{
		Transfers:   []models.TokenTransfer{transfer},
		BlockNumber: 100,
	}

	err := o.Handle(batch)
	require.NoError(t, err)

	// Check progress
	progress, err := deps.progressTracker.GetProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(100), progress, "Progress should match batch.BlockNumber")

	// Verify DB side effects
	// 1) NFT supply => Should be 1
	txCheck, err := deps.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	nft, nftErr := deps.nftDb.GetNft(txCheck, "0xMintContract", "TokenA")
	require.NoError(t, nftErr)
	require.NotNil(t, nft, "NFT should be created for minted token")
	assert.Equal(t, uint64(1), nft.Supply, "Supply should be 1 after a single mint")

	// 2) Ownership => 0xMintReceiver
	uniqueID, uidErr := deps.ownerDb.GetUniqueID(txCheck, "0xMintContract", "TokenA", "0xMintReceiver")
	require.NoError(t, uidErr)
	assert.Equal(t, uint64(1), uniqueID, "UniqueID should match the minted supply (1)")

	// 3) Transfer record
	xfers, afterErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
	require.NoError(t, afterErr)
	require.Len(t, xfers, 1, "Expect exactly 1 transfer stored")
	assert.Equal(t, "0xMintReceiver", xfers[0].To)

	_ = txCheck.Rollback()
}

func TestDefaultTdhTransfersReceivedAction_SingleTransfer_Burn(t *testing.T) {
	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
	defer deps.cleanup()

	o := newDefaultTdhTransfersReceivedAction(t, deps)

	// First, we need to own an NFT so we can burn it
	// We'll do a quick mint or airdrop manually using the orchestrator or a direct DB approach.
	mintXfer := models.TokenTransfer{
		From:             "",
		To:               "0xBurner",
		Contract:         "0xBurnContract",
		TokenID:          "TokenB",
		Amount:           1,
		BlockNumber:      50,
		TransactionIndex: 0,
		LogIndex:         0,
		BlockTime:        1000,
		Type:             models.MINT,
	}
	err := o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{mintXfer}, BlockNumber: 50})
	require.NoError(t, err)

	// Now we do a BURN
	burnXfer := models.TokenTransfer{
		From:             "0xBurner",
		To:               "", // often burning sends it to zero address
		Contract:         "0xBurnContract",
		TokenID:          "TokenB",
		Amount:           1,
		BlockNumber:      55,
		TransactionIndex: 1,
		LogIndex:         0,
		BlockTime:        2000,
		Type:             models.BURN,
	}
	err = o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{burnXfer}, BlockNumber: 55})
	require.NoError(t, err)

	// check progress
	progress, err := deps.progressTracker.GetProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(55), progress)

	// check burnt supply
	txCheck, err := deps.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	nft, err := deps.nftDb.GetNft(txCheck, "0xBurnContract", "TokenB")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nft.Supply, "Supply remains 1 (once minted, doesn't necessarily decrement on burn in your code logic)")
	assert.Equal(t, uint64(1), nft.BurntSupply, "Burnt supply should be incremented by 1")

	// check ownership => burner shouldn't have it anymore
	_, getUidErr := deps.ownerDb.GetUniqueID(txCheck, "0xBurnContract", "TokenB", "0xBurner")
	assert.Error(t, getUidErr, "The burner no longer owns it")

	_ = txCheck.Rollback()
}

func TestDefaultTdhTransfersReceivedAction_ChunkedTransfers(t *testing.T) {
	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
	defer deps.cleanup()

	o := newDefaultTdhTransfersReceivedAction(t, deps)

	// We exceed the batchSize=100 => 120 transfers => 2 sub-batches
	var bigTransfers []models.TokenTransfer
	for i := 0; i < 120; i++ {
		bigTransfers = append(bigTransfers, models.TokenTransfer{
			From:             "", // empty for MINT
			To:               fmt.Sprintf("0xReceiver%d", i),
			Contract:         "0xBatchContract",
			TokenID:          "TokenXYZ",
			Amount:           1,
			BlockNumber:      200,
			TransactionIndex: 0,
			LogIndex:         uint64(i),
			BlockTime:        99999,
			Type:             models.MINT, // <-- CHANGED to MINT
		})
	}

	batch := models.TokenTransferBatch{
		Transfers:   bigTransfers,
		BlockNumber: 200,
	}
	err := o.Handle(batch)
	require.NoError(t, err)

	// Final progress should be 200
	progress, progErr := deps.progressTracker.GetProgress()
	require.NoError(t, progErr)
	assert.Equal(t, uint64(200), progress)

	// Verify that 120 transfers were stored
	txCheck, err := deps.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	xfers, xferErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
	require.NoError(t, xferErr)
	assert.Len(t, xfers, 120, "All 120 should be stored")

	_ = txCheck.Rollback()
}

func TestDefaultTdhTransfersReceivedAction_ResetDueToCheckpointMismatch(t *testing.T) {
	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
	defer deps.cleanup()

	o := newDefaultTdhTransfersReceivedAction(t, deps)

	// 1) Insert a transfer with blockNumber=100 => checkpoint=100
	t1 := models.TokenTransfer{
		From:             "",
		To:               "0xFirstOwner",
		Contract:         "0xResetTest",
		TokenID:          "T1",
		Amount:           1,
		BlockNumber:      100,
		TransactionIndex: 0,
		LogIndex:         0,
		BlockTime:        1111,
		Type:             models.MINT,
	}
	err := o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{t1}, BlockNumber: 100})
	require.NoError(t, err)

	// 2) Next transfer has blockNumber=99 => behind last checkpoint => triggers reset
	t2 := models.TokenTransfer{
		From:             "",
		To:               "0xOwnerButInPast",
		Contract:         "0xResetTest",
		TokenID:          "T2",
		Amount:           1,
		BlockNumber:      99,
		TransactionIndex: 0,
		LogIndex:         0,
		BlockTime:        2222,
		Type:             models.MINT,
	}
	err = o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{t2}, BlockNumber: 99})
	require.NoError(t, err)

	// The orchestrator should detect blockNumber 99 < last checkpoint 100 => reset everything after block=99
	// That includes the old transfer at block=100. So we expect the DB to remove it and revert ownership.

	// final progress => after a successful Handle, the code sets the progress = batch.BlockNumber => 99
	progress, progErr := deps.progressTracker.GetProgress()
	require.NoError(t, progErr)
	assert.Equal(t, uint64(99), progress, "Progress should revert to 99 after reset logic")

	// check DB => the original minted token at block=100 should have been undone
	txCheck, err := deps.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// nft_transfers should contain only the new one (block=99) or none at all if it was inserted after the reset
	xfers, xferErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
	require.NoError(t, xferErr)

	// Because the code first does the reset *before* storing the new ones in the same transaction,
	// we expect the old ones to be reversed/deleted, then the new transfer is applied. So let's confirm:
	if assert.Len(t, xfers, 1, "Should only have the second minted transfer in DB") {
		assert.Equal(t, uint64(99), xfers[0].BlockNumber)
		assert.Equal(t, "0xOwnerButInPast", xfers[0].To)
	}

	// Confirm the first minted NFT is undone
	_, uidErr := deps.ownerDb.GetUniqueID(txCheck, "0xResetTest", "T1", "0xFirstOwner")
	assert.Error(t, uidErr, "Should be gone after reset")

	// The second minted NFT should be present
	uid2, uid2Err := deps.ownerDb.GetUniqueID(txCheck, "0xResetTest", "T2", "0xOwnerButInPast")
	require.NoError(t, uid2Err)
	assert.Equal(t, uint64(1), uid2)

	_ = txCheck.Rollback()
}

func TestDefaultTdhTransfersReceivedAction_ErrorDuringApplyTransfer(t *testing.T) {
	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
	defer deps.cleanup()

	o := newDefaultTdhTransfersReceivedAction(t, deps)

	// We’ll artificially force an error scenario. For instance, if you do a BURN
	// but there's no prior ownership. "ownerDb.UpdateOwnership" might fail
	// because current owner is not found. That should cause the entire transaction
	// to roll back.

	burnNoOwner := models.TokenTransfer{
		From:             "0xNotActualOwner",
		To:               "",
		Contract:         "0xBadBurnContract",
		TokenID:          "BadToken1",
		Amount:           1,
		BlockNumber:      123,
		TransactionIndex: 0,
		LogIndex:         0,
		BlockTime:        3333,
		Type:             models.BURN,
	}

	err := o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{burnNoOwner}, BlockNumber: 123})
	assert.Error(t, err, "We expect an error because there's no prior ownership to burn")

	// Check that nothing was committed
	txCheck, err := deps.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	xfers, xferErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
	require.NoError(t, xferErr)
	assert.Empty(t, xfers, "No transfer should be stored since we expect a rollback")

	// Progress also should remain 0
	p, pErr := deps.progressTracker.GetProgress()
	require.NoError(t, pErr)
	assert.Equal(t, uint64(0), p, "No progress update because everything rolled back")

	_ = txCheck.Rollback()
}

func TestDefaultTdhTransfersReceivedAction_MultipleAmountsInOneTransfer(t *testing.T) {
	// The code does `for i := int64(0); i < transfer.Amount; i++ { ... }`
	// We test that each "unit" of NFT is minted, stored, and has unique supply/ownership.

	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
	defer deps.cleanup()

	o := newDefaultTdhTransfersReceivedAction(t, deps)

	transfer := models.TokenTransfer{
		From:             "",
		To:               "0xMultiMintReceiver",
		Contract:         "0xMultiMintContract",
		TokenID:          "MultiToken",
		Amount:           3,
		BlockNumber:      300,
		TransactionIndex: 2,
		LogIndex:         5,
		BlockTime:        99999,
		Type:             models.MINT,
	}

	err := o.Handle(models.TokenTransferBatch{
		Transfers:   []models.TokenTransfer{transfer},
		BlockNumber: 300,
	})
	require.NoError(t, err)

	// Check supply => should be 3
	txCheck, err := deps.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	nft, nftErr := deps.nftDb.GetNft(txCheck, "0xMultiMintContract", "MultiToken")
	require.NoError(t, nftErr)
	assert.Equal(t, uint64(3), nft.Supply, "Should have minted 3 total")

	// The code calls `tokenUniqueID = newSupply` for each minted item. That means:
	//  1st iteration => newSupply=1
	//  2nd iteration => newSupply=2
	//  3rd iteration => newSupply=3
	// For ownership, each iteration calls `UpdateOwnership(..., tokenUniqueID)`.
	// In practice, multiple identical owner rows can happen unless your code logic merges them.
	// We expect 3 separate calls to StoreTransfer with unique tokenUniqueIDs=1,2,3
	// but the same “to” address. This is an edge case for ERC1155-like multi-mint or a single-edition NFT.
	// The tests confirm the code does not crash.

	xfers, xferErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
	require.NoError(t, xferErr)
	assert.Len(t, xfers, 3, "Should have 3 stored transfers (one per minted item)")

	_ = txCheck.Rollback()
}

// func TestDefaultTdhTransfersReceivedAction_ConcurrentHandleCalls_ExpectError(t *testing.T) {
// 	// We still spin up everything the same way...
// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	defer deps.cleanup()

// 	o := newDefaultTdhTransfersReceivedAction(t, deps)

// 	var wg sync.WaitGroup

// 	transfersA := models.TokenTransferBatch{
// 		Transfers: []models.TokenTransfer{
// 			{
// 				From:        "",
// 				To:          "0xConcurrentA",
// 				Contract:    "0xCC",
// 				TokenID:     "Tid",
// 				Amount:      1,
// 				BlockNumber: 400,
// 				Type:        models.MINT,
// 			},
// 		},
// 		BlockNumber: 400,
// 	}
// 	transfersB := models.TokenTransferBatch{
// 		Transfers: []models.TokenTransfer{
// 			{
// 				From:        "",
// 				To:          "0xConcurrentB",
// 				Contract:    "0xCC",
// 				TokenID:     "Tid",
// 				Amount:      1,
// 				BlockNumber: 401,
// 				Type:        models.MINT,
// 			},
// 		},
// 		BlockNumber: 401,
// 	}

// 	// We'll track each goroutine's error in a slice.
// 	errs := make([]error, 2)

// 	worker := func(batch models.TokenTransferBatch, idx int) {
// 		defer wg.Done()
// 		errs[idx] = o.Handle(batch)
// 	}

// 	wg.Add(2)
// 	go worker(transfersA, 0)
// 	go worker(transfersB, 1)
// 	wg.Wait()

// 	// We now EXPECT that at least one call fails due to concurrency or checkpoint mismatch.
// 	if errs[0] == nil && errs[1] == nil {
// 		t.Error("Expected concurrency error, but both calls surprisingly succeeded")
// 	} else {
// 		t.Logf("Worker 0 error: %v\nWorker 1 error: %v", errs[0], errs[1])
// 	}
// }

func TestDefaultTdhTransfersReceivedAction_ClosedDbAtConstruction(t *testing.T) {
	// If we close the DB before constructing the orchestrator, we won't even get a valid orchestrator.
	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
	deps.cleanup() // closes the DB

	ctx := context.Background()
	o := NewTdhTransfersReceivedActionImpl(
		ctx,
		deps.db,
		deps.transferDb,
		deps.ownerDb,
		deps.nftDb,
		deps.progressTracker,
	)
	assert.Nil(t, o, "Should fail to create orchestrator because beginTx likely fails and logs an error")
}

func TestDefaultTdhTransfersReceivedAction_ClosedDbDuringHandle(t *testing.T) {
	// We'll create a valid orchestrator but close the DB *after* creation, then call Handle.
	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
	o := newDefaultTdhTransfersReceivedAction(t, deps)

	deps.cleanup() // closes DB

	transfer := models.TokenTransfer{
		From:             "",
		To:               "0xReceiver",
		Contract:         "0xClosedDBContract",
		TokenID:          "ClosedDBToken",
		Amount:           1,
		BlockNumber:      500,
		TransactionIndex: 0,
		LogIndex:         0,
		BlockTime:        9999,
		Type:             models.MINT,
	}
	err := o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{transfer}, BlockNumber: 500})
	assert.Error(t, err, "Should fail because DB is closed mid-handle")
}

func TestNewTdhTransfersReceivedActionImpl_BeginTxError(t *testing.T) {
	// Create a sqlmock DB.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Simulate BeginTx error.
	mock.ExpectBegin().WillReturnError(errors.New("begin tx error"))

	// Call the function.
	action := NewTdhTransfersReceivedActionImpl(ctx, db, nil, nil, nil, nil)
	if action != nil {
		t.Errorf("expected nil action when BeginTx fails, got: %#v", action)
	}

	// Verify expectations.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestNewTdhTransfersReceivedActionImpl_CheckpointError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Expect BeginTx to succeed.
	mock.ExpectBegin()

	// Override getLastSavedCheckpoint to simulate an error.
	origGetCheckpoint := getLastSavedCheckpoint
	getLastSavedCheckpoint = func(tx *sql.Tx) (uint64, uint64, uint64, error) {
		return 0, 0, 0, errors.New("checkpoint error")
	}
	defer func() { getLastSavedCheckpoint = origGetCheckpoint }()

	// Expect a rollback since checkpoint retrieval fails.
	mock.ExpectRollback()

	action := NewTdhTransfersReceivedActionImpl(ctx, db, nil, nil, nil, nil)
	if action != nil {
		t.Errorf("expected nil action when checkpoint error occurs, got: %#v", action)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestNewTdhTransfersReceivedActionImpl_Success(t *testing.T) {
	// Create a sqlmock DB.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	// Ensure that the DB used by your dependencies is this sqlmock DB.
	deps := setupTestDefaultTdhTransfersReceivedActionDepsUsingDB(db)
	defer deps.cleanup()

	// Set expectation for BeginTx (which is used inside NewTdhTransfersReceivedActionImpl).
	mock.ExpectBegin()

	origGetCheckpoint := getLastSavedCheckpoint
	getLastSavedCheckpoint = func(tx *sql.Tx) (uint64, uint64, uint64, error) {
		return 100, 1, 2, nil
	}
	defer func() { getLastSavedCheckpoint = origGetCheckpoint }()

	// Expect commit since there is no error.
	mock.ExpectCommit()

	// Create the action using the sqlmock DB.
	action := newDefaultTdhTransfersReceivedAction(t, deps)

	if action == nil {
		t.Errorf("expected non-nil action on success")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}
