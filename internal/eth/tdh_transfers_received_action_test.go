package eth

// import (
// 	"context"
// 	"database/sql"
// 	"errors"
// 	"fmt"
// 	"testing"

// 	"github.com/DATA-DOG/go-sqlmock"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/mock"
// 	"github.com/stretchr/testify/require"

// 	"github.com/6529-Collections/6529node/internal/db"
// 	"github.com/6529-Collections/6529node/internal/db/testdb"
// 	"github.com/6529-Collections/6529node/internal/eth/ethdb"
// 	"github.com/6529-Collections/6529node/internal/eth/mocks"
// 	"github.com/6529-Collections/6529node/pkg/tdh/models"
// )

// type testDefaultTdhTransfersReceivedActionDeps struct {
// 	db              *sql.DB
// 	progressTracker ethdb.TdhIdxTrackerDb
// 	transferDb      ethdb.NFTTransferDb
// 	ownerDb         ethdb.NFTOwnerDb
// 	nftDb           ethdb.NFTDb
// 	cleanup         func()
// }

// func setupTestDefaultTdhTransfersReceivedActionDeps(t *testing.T) *testDefaultTdhTransfersReceivedActionDeps {
// 	db, cleanup := testdb.SetupTestDB(t)

// 	progressTracker := ethdb.NewTdhIdxTrackerDb(db)
// 	transferDb := ethdb.NewTransferDb()
// 	ownerDb := ethdb.NewOwnerDb()
// 	nftDb := ethdb.NewNFTDb()

// 	return &testDefaultTdhTransfersReceivedActionDeps{
// 		db:              db,
// 		progressTracker: progressTracker,
// 		transferDb:      transferDb,
// 		ownerDb:         ownerDb,
// 		nftDb:           nftDb,
// 		cleanup:         cleanup,
// 	}
// }

// func setupTestDefaultTdhTransfersReceivedActionDepsUsingDB(db *sql.DB) *testDefaultTdhTransfersReceivedActionDeps {
// 	progressTracker := ethdb.NewTdhIdxTrackerDb(db)
// 	transferDb := ethdb.NewTransferDb()
// 	ownerDb := ethdb.NewOwnerDb()
// 	nftDb := ethdb.NewNFTDb()

// 	return &testDefaultTdhTransfersReceivedActionDeps{
// 		db:              db,
// 		progressTracker: progressTracker,
// 		transferDb:      transferDb,
// 		ownerDb:         ownerDb,
// 		nftDb:           nftDb,
// 		cleanup: func() {
// 			_ = db.Close()
// 		},
// 	}
// }

// func newDefaultTdhTransfersReceivedAction(t *testing.T, deps *testDefaultTdhTransfersReceivedActionDeps) *DefaultTdhTransfersReceivedAction {
// 	ctx := context.Background()
// 	orchestrator := NewTdhTransfersReceivedActionImpl(
// 		ctx,
// 		deps.db,
// 		deps.transferDb,
// 		deps.ownerDb,
// 		deps.nftDb,
// 		deps.progressTracker,
// 	)
// 	require.NotNil(t, orchestrator, "Should successfully create orchestrator instance")
// 	return orchestrator
// }

// // ------------------------------
// // Mock definitions
// // ------------------------------

// type mockNFTDb struct {
// 	UpdateSupplyFn         func(txn *sql.Tx, contract, tokenID string) (uint64, error)
// 	UpdateBurntSupplyFn    func(txn *sql.Tx, contract, tokenID string) error
// 	UpdateSupplyReverseFn  func(txn *sql.Tx, contract, tokenID string) (uint64, error)
// 	UpdateBurntSupplyRevFn func(txn *sql.Tx, contract, tokenID string) error
// 	GetNftFn               func(rq db.QueryRunner, contract, tokenID string) (*ethdb.NFT, error)
// 	GetAllNftsFn           func(rq db.QueryRunner, pageSize int, page int) (total int, nfts []ethdb.NFT, err error)
// 	GetNftsForContractFn   func(rq db.QueryRunner, contract string, pageSize int, page int) (total int, nfts []ethdb.NFT, err error)
// }

// func (m *mockNFTDb) UpdateSupply(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 	return m.UpdateSupplyFn(txn, contract, tokenID)
// }
// func (m *mockNFTDb) UpdateBurntSupply(txn *sql.Tx, contract, tokenID string) error {
// 	return m.UpdateBurntSupplyFn(txn, contract, tokenID)
// }
// func (m *mockNFTDb) UpdateSupplyReverse(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 	return m.UpdateSupplyReverseFn(txn, contract, tokenID)
// }
// func (m *mockNFTDb) UpdateBurntSupplyReverse(txn *sql.Tx, contract, tokenID string) error {
// 	return m.UpdateBurntSupplyRevFn(txn, contract, tokenID)
// }
// func (m *mockNFTDb) GetNft(rq db.QueryRunner, contract, tokenID string) (*ethdb.NFT, error) {
// 	return m.GetNftFn(rq, contract, tokenID)
// }
// func (m *mockNFTDb) GetAllNfts(rq db.QueryRunner, pageSize int, page int) (total int, nfts []ethdb.NFT, err error) {
// 	return m.GetAllNftsFn(rq, pageSize, page)
// }
// func (m *mockNFTDb) GetNftsForContract(rq db.QueryRunner, contract string, pageSize int, page int) (total int, nfts []ethdb.NFT, err error) {
// 	return m.GetNftsForContractFn(rq, contract, pageSize, page)
// }

// type mockOwnerDb struct {
// 	GetUniqueIDFn               func(txn *sql.Tx, contract, tokenID, address string) (uint64, error)
// 	UpdateOwnershipFn           func(txn *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error
// 	UpdateOwnershipRevFn        func(txn *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error
// 	GetBalanceFn                func(txn *sql.Tx, owner, contract, tokenID string) (uint64, error)
// 	GetAllOwnersFn              func(rq db.QueryRunner, pageSize int, page int) (total int, owners []ethdb.NFTOwner, err error)
// 	GetOwnersForContractFn      func(rq db.QueryRunner, contract string, pageSize int, page int) (total int, owners []ethdb.NFTOwner, err error)
// 	GetOwnersForContractTokenFn func(rq db.QueryRunner, contract string, tokenID string, pageSize int, page int) (total int, owners []ethdb.NFTOwner, err error)
// }

// func (m *mockOwnerDb) GetUniqueID(txn *sql.Tx, contract, tokenID, address string) (uint64, error) {
// 	return m.GetUniqueIDFn(txn, contract, tokenID, address)
// }
// func (m *mockOwnerDb) UpdateOwnership(txn *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error {
// 	return m.UpdateOwnershipFn(txn, transfer, tokenUniqueID)
// }
// func (m *mockOwnerDb) UpdateOwnershipReverse(txn *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 	return m.UpdateOwnershipRevFn(txn, transfer, tokenUniqueID)
// }
// func (m *mockOwnerDb) GetBalance(txn *sql.Tx, owner, contract, tokenID string) (uint64, error) {
// 	return m.GetBalanceFn(txn, owner, contract, tokenID)
// }
// func (m *mockOwnerDb) GetAllOwners(rq db.QueryRunner, pageSize int, page int) (total int, owners []ethdb.NFTOwner, err error) {
// 	return m.GetAllOwnersFn(rq, pageSize, page)
// }
// func (m *mockOwnerDb) GetOwnersForContract(rq db.QueryRunner, contract string, pageSize int, page int) (total int, owners []ethdb.NFTOwner, err error) {
// 	return m.GetOwnersForContractFn(rq, contract, pageSize, page)
// }
// func (m *mockOwnerDb) GetOwnersForContractToken(rq db.QueryRunner, contract string, tokenID string, pageSize int, page int) (total int, owners []ethdb.NFTOwner, err error) {
// 	return m.GetOwnersForContractTokenFn(rq, contract, tokenID, pageSize, page)
// }

// type mockTransferDb struct {
// 	StoreTransferFn                func(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error
// 	DeleteTransferFn               func(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error
// 	GetTransfersAfterCheckpointFn  func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error)
// 	GetLatestTransferFn            func(tx *sql.Tx) (*ethdb.NFTTransfer, error)
// 	GetPaginatedResponseForQueryFn func(rq db.QueryRunner, queryOptions db.QueryOptions, queryParams []interface{}) (total int, transfers []ethdb.NFTTransfer, err error)
// }

// func (m *mockTransferDb) StoreTransfer(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error {
// 	return m.StoreTransferFn(tx, transfer, tokenUniqueID)
// }
// func (m *mockTransferDb) GetTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 	return m.GetTransfersAfterCheckpointFn(tx, blockNumber, txIndex, logIndex)
// }
// func (m *mockTransferDb) DeleteTransfer(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 	return m.DeleteTransferFn(tx, transfer, tokenUniqueID)
// }
// func (m *mockTransferDb) GetLatestTransfer(tx *sql.Tx) (*ethdb.NFTTransfer, error) {
// 	return m.GetLatestTransferFn(tx)
// }
// func (m *mockTransferDb) GetPaginatedResponseForQuery(rq db.QueryRunner, queryOptions db.QueryOptions, queryParams []interface{}) (total int, transfers []ethdb.NFTTransfer, err error) {
// 	return m.GetPaginatedResponseForQueryFn(rq, queryOptions, queryParams)
// }

// // Utility to quickly create the DefaultTdhTransfersReceivedAction with mocks:
// func newActionWithMocks(
// 	t *testing.T,
// 	nftDb ethdb.NFTDb,
// 	ownerDb ethdb.NFTOwnerDb,
// 	transferDb ethdb.NFTTransferDb,
// 	trackerDb ethdb.TdhIdxTrackerDb,
// ) *DefaultTdhTransfersReceivedAction {
// 	ctx := context.Background()

// 	// We'll pass in a real DB or a stub. For error coverage, you typically only need a real *sql.DB
// 	// if you’re testing real SQL logic. If you only want to ensure that tdh_transfers_received_action
// 	// properly returns errors from the underlying interfaces, you can pass a nil DB
// 	// and override getLastSavedCheckpoint, etc. But let's do a memory DB for the sake of transaction calls.
// 	inMemoryDB, err := sql.Open("sqlite3", ":memory:")
// 	require.NoError(t, err, "Should open an in-memory DB for the test")

// 	originalGetLastSavedCheckpoint := getLastSavedCheckpoint
// 	getLastSavedCheckpoint = func(q db.QueryRunner) (uint64, uint64, uint64, error) {
// 		return 0, 0, 0, nil
// 	}

// 	originalUpdateCheckpoint := updateCheckpoint
// 	updateCheckpoint = func(tx *sql.Tx, blockNumber uint64, txIndex uint64, logIndex uint64) error {
// 		return nil
// 	}

// 	t.Cleanup(func() {
// 		// restore
// 		getLastSavedCheckpoint = originalGetLastSavedCheckpoint
// 		updateCheckpoint = originalUpdateCheckpoint
// 		_ = inMemoryDB.Close()
// 	})

// 	action := NewTdhTransfersReceivedActionImpl(
// 		ctx,
// 		inMemoryDB,
// 		transferDb,
// 		ownerDb,
// 		nftDb,
// 		trackerDb,
// 	)
// 	require.NotNil(t, action)
// 	return action
// }

// func TestDefaultTdhTransfersReceivedAction_NoTransfers(t *testing.T) {
// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	defer deps.cleanup()

// 	o := newDefaultTdhTransfersReceivedAction(t, deps)

// 	// Provide an empty batch
// 	batch := models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{},
// 		BlockNumber: 999,
// 	}
// 	err := o.Handle(batch)
// 	require.NoError(t, err, "Handling an empty batch should not fail")

// 	// Final progress should be updated to 999 (the blockNumber in the batch),
// 	// but the orchestrator calls SetProgress only if there's at least 1 transfer.
// 	// The code does "if len(transfersBatch.Transfers) == 0 => return nil" before
// 	// calling progressTracker.SetProgress. So we expect no progress update.
// 	progress, dbErr := deps.progressTracker.GetProgress()
// 	require.NoError(t, dbErr)
// 	assert.Equal(t, uint64(0), progress, "No transfers => no progress update")
// }

// func TestDefaultTdhTransfersReceivedAction_SingleTransfer_Mint(t *testing.T) {
// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	defer deps.cleanup()

// 	o := newDefaultTdhTransfersReceivedAction(t, deps)

// 	transfer := models.TokenTransfer{
// 		From:             "", // typical for mint
// 		To:               "0xMintReceiver",
// 		Contract:         "0xMintContract",
// 		TokenID:          "TokenA",
// 		Amount:           1, // For MINT, we might have multiple minted in one go
// 		BlockNumber:      100,
// 		TransactionIndex: 0,
// 		LogIndex:         0,
// 		BlockTime:        123456,
// 		Type:             models.MINT,
// 	}

// 	batch := models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{transfer},
// 		BlockNumber: 100,
// 	}

// 	err := o.Handle(batch)
// 	require.NoError(t, err)

// 	// Check progress
// 	progress, err := deps.progressTracker.GetProgress()
// 	require.NoError(t, err)
// 	assert.Equal(t, uint64(100), progress, "Progress should match batch.BlockNumber")

// 	// Verify DB side effects
// 	// 1) NFT supply => Should be 1
// 	txCheck, err := deps.db.BeginTx(context.Background(), nil)
// 	require.NoError(t, err)

// 	nft, nftErr := deps.nftDb.GetNft(txCheck, "0xMintContract", "TokenA")
// 	require.NoError(t, nftErr)
// 	require.NotNil(t, nft, "NFT should be created for minted token")
// 	assert.Equal(t, uint64(1), nft.Supply, "Supply should be 1 after a single mint")

// 	// 2) Ownership => 0xMintReceiver
// 	uniqueID, uidErr := deps.ownerDb.GetUniqueID(txCheck, "0xMintContract", "TokenA", "0xMintReceiver")
// 	require.NoError(t, uidErr)
// 	assert.Equal(t, uint64(1), uniqueID, "UniqueID should match the minted supply (1)")

// 	// 3) Transfer record
// 	xfers, afterErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
// 	require.NoError(t, afterErr)
// 	require.Len(t, xfers, 1, "Expect exactly 1 transfer stored")
// 	assert.Equal(t, "0xMintReceiver", xfers[0].To)

// 	_ = txCheck.Rollback()
// }

// func TestDefaultTdhTransfersReceivedAction_SingleTransfer_Burn(t *testing.T) {
// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	defer deps.cleanup()

// 	o := newDefaultTdhTransfersReceivedAction(t, deps)

// 	// First, we need to own an NFT so we can burn it
// 	// We'll do a quick mint or airdrop manually using the orchestrator or a direct DB approach.
// 	mintXfer := models.TokenTransfer{
// 		From:             "",
// 		To:               "0xBurner",
// 		Contract:         "0xBurnContract",
// 		TokenID:          "TokenB",
// 		Amount:           1,
// 		BlockNumber:      50,
// 		TransactionIndex: 0,
// 		LogIndex:         0,
// 		BlockTime:        1000,
// 		Type:             models.MINT,
// 	}
// 	err := o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{mintXfer}, BlockNumber: 50})
// 	require.NoError(t, err)

// 	// Now we do a BURN
// 	burnXfer := models.TokenTransfer{
// 		From:             "0xBurner",
// 		To:               "", // often burning sends it to zero address
// 		Contract:         "0xBurnContract",
// 		TokenID:          "TokenB",
// 		Amount:           1,
// 		BlockNumber:      55,
// 		TransactionIndex: 1,
// 		LogIndex:         0,
// 		BlockTime:        2000,
// 		Type:             models.BURN,
// 	}
// 	err = o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{burnXfer}, BlockNumber: 55})
// 	require.NoError(t, err)

// 	// check progress
// 	progress, err := deps.progressTracker.GetProgress()
// 	require.NoError(t, err)
// 	assert.Equal(t, uint64(55), progress)

// 	// check burnt supply
// 	txCheck, err := deps.db.BeginTx(context.Background(), nil)
// 	require.NoError(t, err)

// 	nft, err := deps.nftDb.GetNft(txCheck, "0xBurnContract", "TokenB")
// 	require.NoError(t, err)
// 	assert.Equal(t, uint64(1), nft.Supply, "Supply remains 1 (once minted, doesn't necessarily decrement on burn in your code logic)")
// 	assert.Equal(t, uint64(1), nft.BurntSupply, "Burnt supply should be incremented by 1")

// 	// check ownership => burner shouldn't have it anymore
// 	_, getUidErr := deps.ownerDb.GetUniqueID(txCheck, "0xBurnContract", "TokenB", "0xBurner")
// 	assert.Error(t, getUidErr, "The burner no longer owns it")

// 	_ = txCheck.Rollback()
// }

// func TestDefaultTdhTransfersReceivedAction_ChunkedTransfers(t *testing.T) {
// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	defer deps.cleanup()

// 	o := newDefaultTdhTransfersReceivedAction(t, deps)

// 	// We exceed the batchSize=100 => 120 transfers => 2 sub-batches
// 	var bigTransfers []models.TokenTransfer
// 	for i := 0; i < 120; i++ {
// 		bigTransfers = append(bigTransfers, models.TokenTransfer{
// 			From:             "", // empty for MINT
// 			To:               fmt.Sprintf("0xReceiver%d", i),
// 			Contract:         "0xBatchContract",
// 			TokenID:          "TokenXYZ",
// 			Amount:           1,
// 			BlockNumber:      200,
// 			TransactionIndex: 0,
// 			LogIndex:         uint64(i),
// 			BlockTime:        99999,
// 			Type:             models.MINT, // <-- CHANGED to MINT
// 		})
// 	}

// 	batch := models.TokenTransferBatch{
// 		Transfers:   bigTransfers,
// 		BlockNumber: 200,
// 	}
// 	err := o.Handle(batch)
// 	require.NoError(t, err)

// 	// Final progress should be 200
// 	progress, progErr := deps.progressTracker.GetProgress()
// 	require.NoError(t, progErr)
// 	assert.Equal(t, uint64(200), progress)

// 	// Verify that 120 transfers were stored
// 	txCheck, err := deps.db.BeginTx(context.Background(), nil)
// 	require.NoError(t, err)
// 	xfers, xferErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
// 	require.NoError(t, xferErr)
// 	assert.Len(t, xfers, 120, "All 120 should be stored")

// 	_ = txCheck.Rollback()
// }

// func TestDefaultTdhTransfersReceivedAction_ResetDueToCheckpointMismatch(t *testing.T) {
// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	defer deps.cleanup()

// 	o := newDefaultTdhTransfersReceivedAction(t, deps)

// 	// 1) Insert a transfer with blockNumber=100 => checkpoint=100
// 	t1 := models.TokenTransfer{
// 		From:             "",
// 		To:               "0xFirstOwner",
// 		Contract:         "0xResetTest",
// 		TokenID:          "T1",
// 		Amount:           1,
// 		BlockNumber:      100,
// 		TransactionIndex: 0,
// 		LogIndex:         0,
// 		BlockTime:        1111,
// 		Type:             models.MINT,
// 	}
// 	err := o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{t1}, BlockNumber: 100})
// 	require.NoError(t, err)

// 	// 2) Next transfer has blockNumber=99 => behind last checkpoint => triggers reset
// 	t2 := models.TokenTransfer{
// 		From:             "",
// 		To:               "0xOwnerButInPast",
// 		Contract:         "0xResetTest",
// 		TokenID:          "T2",
// 		Amount:           1,
// 		BlockNumber:      99,
// 		TransactionIndex: 0,
// 		LogIndex:         0,
// 		BlockTime:        2222,
// 		Type:             models.MINT,
// 	}
// 	err = o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{t2}, BlockNumber: 99})
// 	require.NoError(t, err)

// 	// The orchestrator should detect blockNumber 99 < last checkpoint 100 => reset everything after block=99
// 	// That includes the old transfer at block=100. So we expect the DB to remove it and revert ownership.

// 	// final progress => after a successful Handle, the code sets the progress = batch.BlockNumber => 99
// 	progress, progErr := deps.progressTracker.GetProgress()
// 	require.NoError(t, progErr)
// 	assert.Equal(t, uint64(99), progress, "Progress should revert to 99 after reset logic")

// 	// check DB => the original minted token at block=100 should have been undone
// 	txCheck, err := deps.db.BeginTx(context.Background(), nil)
// 	require.NoError(t, err)

// 	// nft_transfers should contain only the new one (block=99) or none at all if it was inserted after the reset
// 	xfers, xferErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
// 	require.NoError(t, xferErr)

// 	// Because the code first does the reset *before* storing the new ones in the same transaction,
// 	// we expect the old ones to be reversed/deleted, then the new transfer is applied. So let's confirm:
// 	if assert.Len(t, xfers, 1, "Should only have the second minted transfer in DB") {
// 		assert.Equal(t, uint64(99), xfers[0].BlockNumber)
// 		assert.Equal(t, "0xOwnerButInPast", xfers[0].To)
// 	}

// 	// Confirm the first minted NFT is undone
// 	_, uidErr := deps.ownerDb.GetUniqueID(txCheck, "0xResetTest", "T1", "0xFirstOwner")
// 	assert.Error(t, uidErr, "Should be gone after reset")

// 	// The second minted NFT should be present
// 	uid2, uid2Err := deps.ownerDb.GetUniqueID(txCheck, "0xResetTest", "T2", "0xOwnerButInPast")
// 	require.NoError(t, uid2Err)
// 	assert.Equal(t, uint64(1), uid2)

// 	_ = txCheck.Rollback()
// }

// func TestDefaultTdhTransfersReceivedAction_ErrorDuringApplyTransfer(t *testing.T) {
// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	defer deps.cleanup()

// 	o := newDefaultTdhTransfersReceivedAction(t, deps)

// 	// We’ll artificially force an error scenario. For instance, if you do a BURN
// 	// but there's no prior ownership. "ownerDb.UpdateOwnership" might fail
// 	// because current owner is not found. That should cause the entire transaction
// 	// to roll back.

// 	burnNoOwner := models.TokenTransfer{
// 		From:             "0xNotActualOwner",
// 		To:               "",
// 		Contract:         "0xBadBurnContract",
// 		TokenID:          "BadToken1",
// 		Amount:           1,
// 		BlockNumber:      123,
// 		TransactionIndex: 0,
// 		LogIndex:         0,
// 		BlockTime:        3333,
// 		Type:             models.BURN,
// 	}

// 	err := o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{burnNoOwner}, BlockNumber: 123})
// 	assert.Error(t, err, "We expect an error because there's no prior ownership to burn")

// 	// Check that nothing was committed
// 	txCheck, err := deps.db.BeginTx(context.Background(), nil)
// 	require.NoError(t, err)

// 	xfers, xferErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
// 	require.NoError(t, xferErr)
// 	assert.Empty(t, xfers, "No transfer should be stored since we expect a rollback")

// 	// Progress also should remain 0
// 	p, pErr := deps.progressTracker.GetProgress()
// 	require.NoError(t, pErr)
// 	assert.Equal(t, uint64(0), p, "No progress update because everything rolled back")

// 	_ = txCheck.Rollback()
// }

// func TestDefaultTdhTransfersReceivedAction_MultipleAmountsInOneTransfer(t *testing.T) {
// 	// The code does `for i := int64(0); i < transfer.Amount; i++ { ... }`
// 	// We test that each "unit" of NFT is minted, stored, and has unique supply/ownership.

// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	defer deps.cleanup()

// 	o := newDefaultTdhTransfersReceivedAction(t, deps)

// 	transfer := models.TokenTransfer{
// 		From:             "",
// 		To:               "0xMultiMintReceiver",
// 		Contract:         "0xMultiMintContract",
// 		TokenID:          "MultiToken",
// 		Amount:           3,
// 		BlockNumber:      300,
// 		TransactionIndex: 2,
// 		LogIndex:         5,
// 		BlockTime:        99999,
// 		Type:             models.MINT,
// 	}

// 	err := o.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{transfer},
// 		BlockNumber: 300,
// 	})
// 	require.NoError(t, err)

// 	// Check supply => should be 3
// 	txCheck, err := deps.db.BeginTx(context.Background(), nil)
// 	require.NoError(t, err)

// 	nft, nftErr := deps.nftDb.GetNft(txCheck, "0xMultiMintContract", "MultiToken")
// 	require.NoError(t, nftErr)
// 	assert.Equal(t, uint64(3), nft.Supply, "Should have minted 3 total")

// 	// The code calls `tokenUniqueID = newSupply` for each minted item. That means:
// 	//  1st iteration => newSupply=1
// 	//  2nd iteration => newSupply=2
// 	//  3rd iteration => newSupply=3
// 	// For ownership, each iteration calls `UpdateOwnership(..., tokenUniqueID)`.
// 	// In practice, multiple identical owner rows can happen unless your code logic merges them.
// 	// We expect 3 separate calls to StoreTransfer with unique tokenUniqueIDs=1,2,3
// 	// but the same “to” address. This is an edge case for ERC1155-like multi-mint or a single-edition NFT.
// 	// The tests confirm the code does not crash.

// 	xfers, xferErr := deps.transferDb.GetTransfersAfterCheckpoint(txCheck, 0, 0, 0)
// 	require.NoError(t, xferErr)
// 	assert.Len(t, xfers, 3, "Should have 3 stored transfers (one per minted item)")

// 	_ = txCheck.Rollback()
// }

// func TestDefaultTdhTransfersReceivedAction_ClosedDbAtConstruction(t *testing.T) {
// 	// If we close the DB before constructing the orchestrator, we won't even get a valid orchestrator.
// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	deps.cleanup() // closes the DB

// 	ctx := context.Background()
// 	o := NewTdhTransfersReceivedActionImpl(
// 		ctx,
// 		deps.db,
// 		deps.transferDb,
// 		deps.ownerDb,
// 		deps.nftDb,
// 		deps.progressTracker,
// 	)
// 	assert.Nil(t, o, "Should fail to create orchestrator because beginTx likely fails and logs an error")
// }

// func TestDefaultTdhTransfersReceivedAction_ClosedDbDuringHandle(t *testing.T) {
// 	// We'll create a valid orchestrator but close the DB *after* creation, then call Handle.
// 	deps := setupTestDefaultTdhTransfersReceivedActionDeps(t)
// 	o := newDefaultTdhTransfersReceivedAction(t, deps)

// 	deps.cleanup() // closes DB

// 	transfer := models.TokenTransfer{
// 		From:             "",
// 		To:               "0xReceiver",
// 		Contract:         "0xClosedDBContract",
// 		TokenID:          "ClosedDBToken",
// 		Amount:           1,
// 		BlockNumber:      500,
// 		TransactionIndex: 0,
// 		LogIndex:         0,
// 		BlockTime:        9999,
// 		Type:             models.MINT,
// 	}
// 	err := o.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{transfer}, BlockNumber: 500})
// 	assert.Error(t, err, "Should fail because DB is closed mid-handle")
// }

// func TestNewTdhTransfersReceivedActionImpl_CheckpointError(t *testing.T) {
// 	mockDb, mock, err := sqlmock.New()
// 	if err != nil {
// 		t.Fatalf("error creating sqlmock: %v", err)
// 	}
// 	defer mockDb.Close()

// 	ctx := context.Background()

// 	// Override getLastSavedCheckpoint to simulate an error.
// 	origGetCheckpoint := getLastSavedCheckpoint
// 	getLastSavedCheckpoint = func(q db.QueryRunner) (uint64, uint64, uint64, error) {
// 		return 0, 0, 0, errors.New("checkpoint error")
// 	}
// 	defer func() { getLastSavedCheckpoint = origGetCheckpoint }()

// 	action := NewTdhTransfersReceivedActionImpl(ctx, mockDb, nil, nil, nil, nil)
// 	if action != nil {
// 		t.Errorf("expected nil action when checkpoint error occurs, got: %#v", action)
// 	}

// 	if err := mock.ExpectationsWereMet(); err != nil {
// 		t.Errorf("unfulfilled expectations: %s", err)
// 	}
// }

// func TestNewTdhTransfersReceivedActionImpl_Success(t *testing.T) {
// 	// Create a sqlmock DB.
// 	mockDb, mock, err := sqlmock.New()
// 	if err != nil {
// 		t.Fatalf("error creating sqlmock: %v", err)
// 	}
// 	defer mockDb.Close()

// 	// Ensure that the DB used by your dependencies is this sqlmock DB.
// 	deps := setupTestDefaultTdhTransfersReceivedActionDepsUsingDB(mockDb)
// 	defer deps.cleanup()

// 	origGetCheckpoint := getLastSavedCheckpoint
// 	getLastSavedCheckpoint = func(q db.QueryRunner) (uint64, uint64, uint64, error) {
// 		return 100, 1, 2, nil
// 	}
// 	defer func() { getLastSavedCheckpoint = origGetCheckpoint }()

// 	// Create the action using the sqlmock DB.
// 	action := newDefaultTdhTransfersReceivedAction(t, deps)

// 	if action == nil {
// 		t.Errorf("expected non-nil action on success")
// 	}

// 	if err := mock.ExpectationsWereMet(); err != nil {
// 		t.Errorf("unfulfilled expectations: %s", err)
// 	}
// }

// // ------------------------------
// // Tests for error paths
// // ------------------------------

// // 1) applyTransfer => error from nftDb.UpdateSupply
// func TestDefaultTdhTransfersReceivedAction_ApplyTransfer_ErrorUpdateSupply(t *testing.T) {
// 	nftDbMock := &mockNFTDb{
// 		UpdateSupplyFn: func(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 			return 0, errors.New("UpdateSupply forced error")
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{}
// 	transferDbMock := &mockTransferDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	// We do a MINT transfer which calls nftDb.UpdateSupply
// 	transfer := models.TokenTransfer{
// 		Type:        models.MINT,
// 		Contract:    "0xBad",
// 		TokenID:     "Token123",
// 		BlockNumber: 10,
// 		Amount:      1,
// 	}

// 	// We call Handle with a single transfer.
// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{transfer},
// 		BlockNumber: 10,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "UpdateSupply forced error")
// }

// // 2) applyTransfer => error from nftDb.UpdateBurntSupply (for BURN)
// func TestDefaultTdhTransfersReceivedAction_ApplyTransfer_ErrorUpdateBurntSupply(t *testing.T) {
// 	nftDbMock := &mockNFTDb{
// 		UpdateBurntSupplyFn: func(txn *sql.Tx, contract, tokenID string) error {
// 			return errors.New("UpdateBurntSupply forced error")
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{
// 		// We must also ensure GetUniqueID returns something valid, or else we’d fail earlier.
// 		GetUniqueIDFn: func(txn *sql.Tx, contract, tokenID, address string) (uint64, error) {
// 			return 42, nil
// 		},
// 	}
// 	transferDbMock := &mockTransferDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	// BURN => calls nftDb.UpdateBurntSupply
// 	transfer := models.TokenTransfer{
// 		Type:        models.BURN,
// 		Contract:    "0xBurn",
// 		TokenID:     "BurnToken",
// 		From:        "0xOwner",
// 		BlockNumber: 20,
// 		Amount:      1,
// 	}

// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{transfer},
// 		BlockNumber: 20,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "UpdateBurntSupply forced error")
// }

// // 3) applyTransfer => error from ownerDb.GetUniqueID (for non-mint/airdrop)
// func TestDefaultTdhTransfersReceivedAction_ApplyTransfer_ErrorGetUniqueID(t *testing.T) {
// 	nftDbMock := &mockNFTDb{
// 		// For any non-mint, we skip UpdateSupply.
// 		// For BURN we do UpdateBurntSupply, but let's do e.g. a SEND/SALE transfer
// 		UpdateBurntSupplyFn: func(txn *sql.Tx, contract, tokenID string) error {
// 			return nil
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{
// 		GetUniqueIDFn: func(txn *sql.Tx, contract, tokenID, address string) (uint64, error) {
// 			return 0, errors.New("GetUniqueID forced error")
// 		},
// 	}
// 	transferDbMock := &mockTransferDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	// For a SEND/SALE: Type=SEND means from->to, we rely on ownerDb.GetUniqueID to find the tokenUniqueID
// 	transfer := models.TokenTransfer{
// 		Type:        models.SEND,
// 		Contract:    "0xSend",
// 		TokenID:     "SendToken",
// 		From:        "0xFrom",
// 		To:          "0xTo",
// 		BlockNumber: 30,
// 		Amount:      1,
// 	}

// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{transfer},
// 		BlockNumber: 30,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "GetUniqueID forced error")
// }

// // 4) applyTransfer => error from transferDb.StoreTransfer
// func TestDefaultTdhTransfersReceivedAction_ApplyTransfer_ErrorStoreTransfer(t *testing.T) {
// 	nftDbMock := &mockNFTDb{
// 		UpdateSupplyFn: func(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 			// Return 1 for minted supply
// 			return 1, nil
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{}
// 	transferDbMock := &mockTransferDb{
// 		StoreTransferFn: func(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error {
// 			return errors.New("StoreTransfer forced error")
// 		},
// 	}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	// MINT => calls nftDb.UpdateSupply => success => then calls transferDb.StoreTransfer => error
// 	transfer := models.TokenTransfer{
// 		Type:        models.MINT,
// 		Contract:    "0xMint",
// 		TokenID:     "MintToken",
// 		BlockNumber: 40,
// 		Amount:      1,
// 	}

// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{transfer},
// 		BlockNumber: 40,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "StoreTransfer forced error")
// }

// // 5) applyTransfer => error from ownerDb.UpdateOwnership
// func TestDefaultTdhTransfersReceivedAction_ApplyTransfer_ErrorUpdateOwnership(t *testing.T) {
// 	nftDbMock := &mockNFTDb{
// 		UpdateSupplyFn: func(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 			return 1, nil
// 		},
// 	}
// 	transferDbMock := &mockTransferDb{
// 		StoreTransferFn: func(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error {
// 			return nil
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{
// 		UpdateOwnershipFn: func(txn *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error {
// 			return errors.New("UpdateOwnership forced error")
// 		},
// 	}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	xfer := models.TokenTransfer{
// 		Type:        models.MINT,
// 		Contract:    "0xMint2",
// 		TokenID:     "Mint2ID",
// 		BlockNumber: 50,
// 		Amount:      1,
// 	}
// 	err := action.Handle(models.TokenTransferBatch{Transfers: []models.TokenTransfer{xfer}, BlockNumber: 50})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "UpdateOwnership forced error")
// }

// // 6) applyTransferReverse => error from ownerDb.GetUniqueID
// func TestDefaultTdhTransfersReceivedAction_ApplyTransferReverse_ErrorGetUniqueID(t *testing.T) {
// 	nftDbMock := &mockNFTDb{}
// 	ownerDbMock := &mockOwnerDb{
// 		GetUniqueIDFn: func(txn *sql.Tx, contract, tokenID, address string) (uint64, error) {
// 			return 0, errors.New("GetUniqueID forced error in reverse")
// 		},
// 	}
// 	transferDbMock := &mockTransferDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	// Force applyTransferReverse by calling reset, which calls applyTransferReverse
// 	// Or we can manually call action.applyTransferReverse in a test if we refactor it to be public.
// 	// For simpler coverage, let's use reset logic, which enumerates transfers and calls applyTransferReverse.
// 	// We'll mock GetTransfersAfterCheckpoint to return 1 NFTTransfer so that reset will revert it.
// 	transferDbMock.GetTransfersAfterCheckpointFn = func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 		return []ethdb.NFTTransfer{
// 			{
// 				Type:     models.BURN,
// 				Contract: "0xReverseBurn",
// 				TokenID:  "ReverseToken",
// 				To:       "0xTheBurnAddress",
// 			},
// 		}, nil
// 	}
// 	// Other needed mocks
// 	transferDbMock.DeleteTransferFn = func(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 		return nil
// 	}
// 	transferDbMock.GetLatestTransferFn = func(tx *sql.Tx) (*ethdb.NFTTransfer, error) {
// 		return nil, nil
// 	}

// 	err := action.reset(nil /*tx*/, 999, 0, 0) // we pass nil for *sql.Tx in this contrived scenario
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "GetUniqueID forced error in reverse")
// }

// // 7) applyTransferReverse => error from nftDb.UpdateBurntSupplyReverse
// func TestDefaultTdhTransfersReceivedAction_ApplyTransferReverse_ErrorUpdateBurntSupplyReverse(t *testing.T) {
// 	nftDbMock := &mockNFTDb{
// 		UpdateBurntSupplyRevFn: func(txn *sql.Tx, contract, tokenID string) error {
// 			return errors.New("UpdateBurntSupplyReverse forced error")
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{
// 		GetUniqueIDFn: func(txn *sql.Tx, contract, tokenID, address string) (uint64, error) {
// 			return 10, nil
// 		},
// 	}
// 	transferDbMock := &mockTransferDb{
// 		// We'll feed one BURN transfer to reset
// 		GetTransfersAfterCheckpointFn: func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 			return []ethdb.NFTTransfer{
// 				{
// 					Type:     models.BURN,
// 					Contract: "0xReverseBurn",
// 					TokenID:  "ReverseToken",
// 					To:       "0xTheBurnAddress",
// 				},
// 			}, nil
// 		},
// 		DeleteTransferFn: func(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 			return nil
// 		},
// 		GetLatestTransferFn: func(tx *sql.Tx) (*ethdb.NFTTransfer, error) {
// 			return nil, nil
// 		},
// 	}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	err := action.reset(nil, 10, 0, 0)
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "UpdateBurntSupplyReverse forced error")
// }

// // 8) applyTransferReverse => error from nftDb.UpdateSupplyReverse (for MINT or AIRDROP)
// func TestDefaultTdhTransfersReceivedAction_ApplyTransferReverse_ErrorUpdateSupplyReverse(t *testing.T) {
// 	nftDbMock := &mockNFTDb{
// 		UpdateSupplyReverseFn: func(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 			return 0, errors.New("UpdateSupplyReverse forced error")
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{
// 		GetUniqueIDFn: func(txn *sql.Tx, contract, tokenID, address string) (uint64, error) {
// 			return 10, nil
// 		},
// 	}
// 	transferDbMock := &mockTransferDb{
// 		GetTransfersAfterCheckpointFn: func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 			return []ethdb.NFTTransfer{
// 				{
// 					Type:     models.MINT,
// 					Contract: "0xReverseMint",
// 					TokenID:  "ReverseToken",
// 					To:       "0xReceiver",
// 				},
// 			}, nil
// 		},
// 		DeleteTransferFn: func(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 			return nil
// 		},
// 		GetLatestTransferFn: func(tx *sql.Tx) (*ethdb.NFTTransfer, error) {
// 			return nil, nil
// 		},
// 	}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	err := action.reset(nil, 10, 0, 0)
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "UpdateSupplyReverse forced error")
// }

// // 9) applyTransferReverse => error from ownerDb.UpdateOwnershipReverse
// func TestDefaultTdhTransfersReceivedAction_ApplyTransferReverse_ErrorUpdateOwnershipReverse(t *testing.T) {
// 	nftDbMock := &mockNFTDb{}
// 	ownerDbMock := &mockOwnerDb{
// 		GetUniqueIDFn: func(txn *sql.Tx, contract, tokenID, address string) (uint64, error) {
// 			return 11, nil
// 		},
// 		UpdateOwnershipRevFn: func(txn *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 			return errors.New("UpdateOwnershipReverse forced error")
// 		},
// 	}
// 	transferDbMock := &mockTransferDb{
// 		GetTransfersAfterCheckpointFn: func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 			return []ethdb.NFTTransfer{
// 				{
// 					Type:     models.SEND,
// 					Contract: "0xC",
// 					TokenID:  "TID",
// 					To:       "0xReceiver",
// 					From:     "0xSender",
// 				},
// 			}, nil
// 		},
// 		DeleteTransferFn: func(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 			return nil
// 		},
// 		GetLatestTransferFn: func(tx *sql.Tx) (*ethdb.NFTTransfer, error) {
// 			return nil, nil
// 		},
// 	}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	err := action.reset(nil, 10, 0, 0)
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "UpdateOwnershipReverse forced error")
// }

// // 10) reset => error from transferDb.GetTransfersAfterCheckpoint
// func TestDefaultTdhTransfersReceivedAction_Reset_ErrorGetTransfersAfterCheckpoint(t *testing.T) {
// 	nftDbMock := &mockNFTDb{}
// 	ownerDbMock := &mockOwnerDb{}
// 	transferDbMock := &mockTransferDb{
// 		GetTransfersAfterCheckpointFn: func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 			return nil, errors.New("GetTransfersAfterCheckpoint forced error")
// 		},
// 	}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	err := action.reset(nil, 999, 0, 0)
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "GetTransfersAfterCheckpoint forced error")
// }

// // 11) reset => error from applyTransferReverse (already covered above in #6..#9 scenarios).
// func TestDefaultTdhTransfersReceivedAction_Reset_ErrorDuringApplyTransferReverse(t *testing.T) {
// 	// Mock transferDb to return a single NFTTransfer
// 	transferDbMock := &mockTransferDb{
// 		GetTransfersAfterCheckpointFn: func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 			return []ethdb.NFTTransfer{
// 				{
// 					Type:     models.BURN,
// 					Contract: "0xC",
// 					TokenID:  "T",
// 					To:       "0xBurnAddr",
// 				},
// 			}, nil
// 		},
// 		DeleteTransferFn: func(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 			return nil
// 		},
// 		GetLatestTransferFn: func(tx *sql.Tx) (*ethdb.NFTTransfer, error) {
// 			return nil, nil
// 		},
// 	}

// 	nftDbMock := &mockNFTDb{
// 		UpdateBurntSupplyRevFn: func(txn *sql.Tx, contract, tokenID string) error {
// 			return errors.New("forced error in revert burn")
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{
// 		GetUniqueIDFn: func(txn *sql.Tx, contract, tokenID, address string) (uint64, error) {
// 			return 1, nil
// 		},
// 	}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	err := action.reset(nil, 100, 0, 0)
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "forced error in revert burn")
// }

// // 12) reset => error from transferDb.DeleteTransfer
// func TestDefaultTdhTransfersReceivedAction_Reset_ErrorDeleteTransfers(t *testing.T) {
// 	transferDbMock := &mockTransferDb{
// 		GetTransfersAfterCheckpointFn: func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 			return []ethdb.NFTTransfer{
// 				{
// 					Type:     models.SEND,
// 					Contract: "0xC",
// 					TokenID:  "T",
// 				},
// 			}, nil
// 		},
// 		DeleteTransferFn: func(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 			return errors.New("DeleteTransfer forced error")
// 		},
// 		GetLatestTransferFn: func(tx *sql.Tx) (*ethdb.NFTTransfer, error) {
// 			return nil, nil
// 		},
// 	}
// 	nftDbMock := &mockNFTDb{}
// 	ownerDbMock := &mockOwnerDb{
// 		GetUniqueIDFn: func(txn *sql.Tx, contract, tokenID, address string) (uint64, error) {
// 			return 1, nil
// 		},
// 		UpdateOwnershipRevFn: func(txn *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 			return nil
// 		},
// 	}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	err := action.reset(nil, 100, 0, 0)
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "DeleteTransfer forced error")
// }

// // 13) reset => error from transferDb.GetLatestTransfer
// func TestDefaultTdhTransfersReceivedAction_Reset_ErrorGetLatestTransfer(t *testing.T) {
// 	transferDbMock := &mockTransferDb{
// 		GetTransfersAfterCheckpointFn: func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 			return []ethdb.NFTTransfer{}, nil
// 		},
// 		DeleteTransferFn: func(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 			return nil
// 		},
// 		GetLatestTransferFn: func(tx *sql.Tx) (*ethdb.NFTTransfer, error) {
// 			return nil, errors.New("GetLatestTransfer forced error")
// 		},
// 	}
// 	nftDbMock := &mockNFTDb{}
// 	ownerDbMock := &mockOwnerDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	err := action.reset(nil, 100, 0, 0)
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "GetLatestTransfer forced error")
// }

// // 14) reset => error from updateCheckpoint
// // We can force that by mocking the updateCheckpoint function. Or we can override it similarly
// // to how we override getLastSavedCheckpoint. For brevity, here’s an example overriding updateCheckpoint:
// func TestDefaultTdhTransfersReceivedAction_Reset_ErrorUpdateCheckpoint(t *testing.T) {
// 	transferDbMock := &mockTransferDb{
// 		GetTransfersAfterCheckpointFn: func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 			return []ethdb.NFTTransfer{}, nil
// 		},
// 		DeleteTransferFn: func(tx *sql.Tx, transfer ethdb.NFTTransfer, tokenUniqueID uint64) error {
// 			return nil
// 		},
// 		GetLatestTransferFn: func(tx *sql.Tx) (*ethdb.NFTTransfer, error) {
// 			return &ethdb.NFTTransfer{
// 				BlockNumber:      100,
// 				TransactionIndex: 0,
// 				LogIndex:         0,
// 			}, nil
// 		},
// 	}
// 	nftDbMock := &mockNFTDb{}
// 	ownerDbMock := &mockOwnerDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)
// 	origUpdateCheckpoint := updateCheckpoint
// 	updateCheckpoint = func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) error {
// 		return errors.New("updateCheckpoint forced error")
// 	}
// 	defer func() { updateCheckpoint = origUpdateCheckpoint }()

// 	err := action.reset(nil, 100, 0, 0)
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "updateCheckpoint forced error")
// }

// // 15) Handle => error from BeginTx
// // For a true unit test, you might override the DB’s BeginTx using a sqlmock or some technique.
// // Or you can override action.db with a mock that returns an error. Let’s do a minimal approach:
// func TestDefaultTdhTransfersReceivedAction_Handle_ErrorBeginTx(t *testing.T) {
// 	// We'll create an action with a DB that is already closed so BeginTx fails:
// 	closedDB, _ := sql.Open("sqlite3", ":memory:")
// 	closedDB.Close()

// 	// We can pass real mocks for everything else:
// 	nftDbMock := &mockNFTDb{}
// 	ownerDbMock := &mockOwnerDb{}
// 	transferDbMock := &mockTransferDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	ctx := context.Background()
// 	action := &DefaultTdhTransfersReceivedAction{
// 		ctx:             ctx,
// 		db:              closedDB, // closed so that BeginTx fails
// 		progressTracker: trackerDbMock,
// 		transferDb:      transferDbMock,
// 		ownerDb:         ownerDbMock,
// 		nftDb:           nftDbMock,
// 	}

// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{{Type: models.MINT}},
// 		BlockNumber: 999,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "database is closed")
// }

// // 16) Handle => error from getLastSavedCheckpoint
// func TestDefaultTdhTransfersReceivedAction_Handle_ErrorGetLastSavedCheckpoint(t *testing.T) {
// 	// Minimal setup
// 	nftDbMock := &mockNFTDb{}
// 	ownerDbMock := &mockOwnerDb{}
// 	transferDbMock := &mockTransferDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	// We'll override getLastSavedCheckpoint to return an error:
// 	origGetLastSavedCheckpoint := getLastSavedCheckpoint
// 	getLastSavedCheckpoint = func(q db.QueryRunner) (uint64, uint64, uint64, error) {
// 		return 0, 0, 0, errors.New("forced checkpoint error")
// 	}
// 	defer func() { getLastSavedCheckpoint = origGetLastSavedCheckpoint }()

// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{{Type: models.MINT, BlockNumber: 100, Amount: 1}},
// 		BlockNumber: 100,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "forced checkpoint error")
// }

// // 17) Handle => error from reset (when checkpoint mismatch occurs)
// func TestDefaultTdhTransfersReceivedAction_Handle_ErrorFromReset(t *testing.T) {
// 	// Force an error inside reset, e.g. from GetTransfersAfterCheckpoint
// 	transferDbMock := &mockTransferDb{
// 		GetTransfersAfterCheckpointFn: func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]ethdb.NFTTransfer, error) {
// 			return nil, errors.New("forced error in reset")
// 		},
// 	}
// 	nftDbMock := &mockNFTDb{}
// 	ownerDbMock := &mockOwnerDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	// We'll force a mismatch so that reset is called. Then inside reset, we force an error.
// 	// Suppose the last saved checkpoint is block=200. We'll override getLastSavedCheckpoint:
// 	origGetLastSavedCheckpoint := getLastSavedCheckpoint
// 	getLastSavedCheckpoint = func(q db.QueryRunner) (uint64, uint64, uint64, error) {
// 		return 200, 0, 0, nil
// 	}
// 	defer func() { getLastSavedCheckpoint = origGetLastSavedCheckpoint }()

// 	// Now we pass in a batch with blockNumber=100 which is less than the last checkpoint=200 => triggers reset
// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{{Type: models.MINT, BlockNumber: 100, Amount: 1}},
// 		BlockNumber: 100,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "forced error in reset")
// }

// // 18) Handle => error from applyTransfer
// func TestDefaultTdhTransfersReceivedAction_Handle_ErrorFromApplyTransfer(t *testing.T) {
// 	// We'll just force an error from applyTransfer by mocking nftDb.UpdateSupply
// 	nftDbMock := &mockNFTDb{
// 		UpdateSupplyFn: func(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 			return 0, errors.New("some forced applyTransfer error")
// 		},
// 	}
// 	transferDbMock := &mockTransferDb{}
// 	ownerDbMock := &mockOwnerDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers: []models.TokenTransfer{
// 			{Type: models.MINT, BlockNumber: 300, Amount: 1},
// 		},
// 		BlockNumber: 300,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "some forced applyTransfer error")
// }

// // 19) Handle => error from updateCheckpoint (after applyTransfer)
// func TestDefaultTdhTransfersReceivedAction_Handle_ErrorUpdateCheckpoint(t *testing.T) {
// 	// Setup minimal mocks
// 	nftDbMock := &mockNFTDb{
// 		UpdateSupplyFn: func(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 			return 1, nil
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{}
// 	transferDbMock := &mockTransferDb{}
// 	trackerDbMock := new(mocks.TdhIdxTrackerDb)

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, trackerDbMock)

// 	origUpdateCheckpoint := updateCheckpoint
// 	updateCheckpoint = func(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) error {
// 		return errors.New("forced updateCheckpoint error")
// 	}
// 	defer func() { updateCheckpoint = origUpdateCheckpoint }()

// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{{Type: models.MINT, BlockNumber: 400}},
// 		BlockNumber: 400,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "forced updateCheckpoint error")
// }

// // 20) Handle => error from tx.Commit
// // We can simulate this by wrapping the DB or using a DB that fails on commit.
// // One trick is to use a driver or a sqlmock that triggers an error on commit.
// // For brevity, we might simply pass a DB that is forcibly closed inside the transaction.
// func TestDefaultTdhTransfersReceivedAction_Handle_ErrorCommit(t *testing.T) {
// 	// We'll do it by hooking the DB after we begin the transaction:
// 	// This can be tricky to simulate exactly in normal code without sqlmock.
// 	// Let’s show a concept with a real DB that we forcibly close after we start the transaction.

// 	inMemoryDB, err := sql.Open("sqlite3", ":memory:")
// 	require.NoError(t, err)
// 	defer inMemoryDB.Close()

// 	// Create the action that uses inMemoryDB:
// 	ctx := context.Background()
// 	action := &DefaultTdhTransfersReceivedAction{
// 		ctx:             ctx,
// 		db:              inMemoryDB,
// 		progressTracker: new(mocks.TdhIdxTrackerDb),
// 		transferDb:      &mockTransferDb{},
// 		ownerDb:         &mockOwnerDb{},
// 		nftDb: &mockNFTDb{
// 			UpdateSupplyFn: func(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 				return 1, nil
// 			},
// 		},
// 	}

// 	// We can open a transaction, close the DB, and then cause the real .Handle to attempt commit, which fails.
// 	_, err = inMemoryDB.BeginTx(ctx, nil)
// 	require.NoError(t, err)

// 	// Now forcibly close the DB (the transaction is still open).
// 	_ = inMemoryDB.Close()

// 	// Actually calling .Handle => it will do BeginTx (which might fail or succeed if there's some leftover state).
// 	// If it succeeds, the final commit will fail because the DB is closed.

// 	transfers := []models.TokenTransfer{
// 		{Type: models.MINT, BlockNumber: 500},
// 	}
// 	batch := models.TokenTransferBatch{
// 		Transfers:   transfers,
// 		BlockNumber: 500,
// 	}

// 	errHandle := action.Handle(batch)
// 	require.Error(t, errHandle)
// 	assert.Contains(t, errHandle.Error(), "database is closed")
// }

// // 21) Handle => error from progressTracker.SetProgress
// func TestDefaultTdhTransfersReceivedAction_Handle_ErrorSetProgress(t *testing.T) {
// 	mIdxTracker := new(mocks.TdhIdxTrackerDb)
// 	mIdxTracker.On("SetProgress", uint64(900), mock.Anything).Return(errors.New("forced SetProgress error"))
// 	mIdxTracker.On("GetProgress").Return(uint64(0), nil)

// 	nftDbMock := &mockNFTDb{
// 		UpdateSupplyFn: func(txn *sql.Tx, contract, tokenID string) (uint64, error) {
// 			return 1, nil
// 		},
// 	}
// 	ownerDbMock := &mockOwnerDb{
// 		UpdateOwnershipFn: func(txn *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error {
// 			// Return nil to simulate successful ownership update.
// 			return nil
// 		},
// 	}
// 	transferDbMock := &mockTransferDb{
// 		StoreTransferFn: func(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error {
// 			return nil
// 		},
// 	}

// 	action := newActionWithMocks(t, nftDbMock, ownerDbMock, transferDbMock, mIdxTracker)
// 	xfer := models.TokenTransfer{
// 		Type:        models.MINT,
// 		Contract:    "0xAny",
// 		TokenID:     "any",
// 		BlockNumber: 900,
// 		Amount:      1,
// 	}

// 	err := action.Handle(models.TokenTransferBatch{
// 		Transfers:   []models.TokenTransfer{xfer},
// 		BlockNumber: 900,
// 	})
// 	require.Error(t, err)
// 	assert.Contains(t, err.Error(), "forced SetProgress error")
// }
