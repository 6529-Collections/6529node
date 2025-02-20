package eth

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/6529-Collections/6529node/internal/eth/mocks"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockTransferDb struct{ mock.Mock }

func (m *mockTransferDb) StoreTransfer(tx *sql.Tx, transfer models.TokenTransfer) error {
	args := m.Called(tx, transfer)
	return args.Error(0)
}
func (m *mockTransferDb) GetTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]models.TokenTransfer, error) {
	args := m.Called(tx, blockNumber, txIndex, logIndex)
	return args.Get(0).([]models.TokenTransfer), args.Error(1)
}
func (m *mockTransferDb) DeleteTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) error {
	args := m.Called(tx, blockNumber, txIndex, logIndex)
	return args.Error(0)
}
func (m *mockTransferDb) GetLatestTransfer(tx *sql.Tx) (*models.TokenTransfer, error) {
	args := m.Called(tx)
	// you can return (*models.TokenTransfer)(nil) if first is nil
	return args.Get(0).(*models.TokenTransfer), args.Error(1)
}

type mockOwnerDb struct{ mock.Mock }

func (m *mockOwnerDb) UpdateOwnership(tx *sql.Tx, from, to, contract, tokenID string, amount int64) error {
	args := m.Called(tx, from, to, contract, tokenID, amount)
	return args.Error(0)
}
func (m *mockOwnerDb) UpdateOwnershipReverse(tx *sql.Tx, from, to, contract, tokenID string, amount int64) error {
	args := m.Called(tx, from, to, contract, tokenID, amount)
	return args.Error(0)
}
func (m *mockOwnerDb) GetBalance(tx *sql.Tx, owner, contract, tokenID string) (int64, error) {
	args := m.Called(tx, owner, contract, tokenID)
	return args.Get(0).(int64), args.Error(1)
}

type mockNFTDb struct{ mock.Mock }

func (m *mockNFTDb) UpdateSupply(tx *sql.Tx, contract, tokenID string, delta int64) error {
	args := m.Called(tx, contract, tokenID, delta)
	return args.Error(0)
}
func (m *mockNFTDb) UpdateSupplyReverse(tx *sql.Tx, contract, tokenID string, delta int64) error {
	args := m.Called(tx, contract, tokenID, delta)
	return args.Error(0)
}
func (m *mockNFTDb) UpdateBurntSupply(tx *sql.Tx, contract, tokenID string, delta int64) error {
	args := m.Called(tx, contract, tokenID, delta)
	return args.Error(0)
}
func (m *mockNFTDb) UpdateBurntSupplyReverse(tx *sql.Tx, contract, tokenID string, delta int64) error {
	args := m.Called(tx, contract, tokenID, delta)
	return args.Error(0)
}
func (m *mockNFTDb) GetNft(tx *sql.Tx, contract, tokenID string) (*models.NFT, error) {
	args := m.Called(tx, contract, tokenID)
	return args.Get(0).(*models.NFT), args.Error(1)
}

// --------------------------------
// Test for NewTdhTransfersReceivedActionImpl
// --------------------------------
func TestNewTdhTransfersReceivedActionImpl_BeginTxFails(t *testing.T) {
	// 1) Setup sqlmock so that db.BeginTx fails
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// On begin => error
	mockDB.ExpectBegin().WillReturnError(errors.New("mock begin fail"))

	// 2) Prepare other mocks
	tracker := mocks.NewTdhIdxTrackerDb(t)
	xferDb := &mockTransferDb{}
	ownerDb := &mockOwnerDb{}
	nftDb := &mockNFTDb{}

	// 3) Call constructor
	action := NewTdhTransfersReceivedActionImpl(
		context.Background(),
		db,
		xferDb,
		ownerDb,
		nftDb,
		tracker,
	)

	// 4) Expect action is nil due to error
	assert.Nil(t, action, "Should return nil if BeginTx fails")

	// 5) Check expectations
	err = mockDB.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestNewTdhTransfersReceivedActionImpl_GetCheckpointFails(t *testing.T) {
	// We'll succeed on BeginTx but fail the checkpoint query
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// 1) begin -> success
	mockDB.ExpectBegin()

	// 2) Expect a query to get checkpoint => return error
	mockDB.ExpectQuery("SELECT block_number, transaction_index, log_index FROM token_transfers_checkpoint").
		WillReturnError(errors.New("checkpoint query fail"))

	tracker := mocks.NewTdhIdxTrackerDb(t)
	xferDb := &mockTransferDb{}
	ownerDb := &mockOwnerDb{}
	nftDb := &mockNFTDb{}

	action := NewTdhTransfersReceivedActionImpl(
		context.Background(),
		db,
		xferDb,
		ownerDb,
		nftDb,
		tracker,
	)
	assert.Nil(t, action)

	err = mockDB.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestNewTdhTransfersReceivedActionImpl_SuccessNoCheckpoint(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// 1) On begin => success
	mockDB.ExpectBegin()
	// 2) The code does "SELECT ... FROM token_transfers_checkpoint"
	// if no row => returns sql.ErrNoRows => we mock that:
	rows := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"}).
		// no rows => meaning sql.ErrNoRows
		FromCSVString("")
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(rows)

	tracker := mocks.NewTdhIdxTrackerDb(t)
	xferDb := &mockTransferDb{}
	ownerDb := &mockOwnerDb{}
	nftDb := &mockNFTDb{}

	action := NewTdhTransfersReceivedActionImpl(
		context.Background(),
		db,
		xferDb,
		ownerDb,
		nftDb,
		tracker,
	)
	assert.NotNil(t, action, "Should succeed when there's simply no checkpoint row")

	err = mockDB.ExpectationsWereMet()
	assert.NoError(t, err)
}

// --------------------------------
// Test applyTransfer
// --------------------------------
func TestDefaultTdhTransfersReceivedAction_applyTransfer(t *testing.T) {
	// We won't fully mock BeginTx since it's inside tests for applyTransfer only
	// We'll pass a *sql.Tx = nil or something, but let's do a real-ish approach with sqlmock
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// We'll begin a transaction that always works
	mockDB.ExpectBegin()
	tx, _ := db.Begin()

	// Prepare mocks
	tracker := mocks.NewTdhIdxTrackerDb(t)
	xferDb := &mockTransferDb{}
	ownerDb := &mockOwnerDb{}
	nftDb := &mockNFTDb{}

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	// We'll test MINT scenario
	xfer := models.TokenTransfer{
		Type:     models.MINT,
		Contract: "0xContract",
		TokenID:  "123",
		Amount:   10,
		From:     "0x0000000000000000000000000000000000000000", // "null" address
		To:       "0xUser",
	}

	// Expect calls:
	// 1) StoreTransfer
	xferDb.On("StoreTransfer", mock.AnythingOfType("*sql.Tx"), xfer).Return(nil).Once()
	// 2) Because Type=MINT => nftDb.UpdateSupply
	nftDb.On("UpdateSupply", mock.AnythingOfType("*sql.Tx"), xfer.Contract, xfer.TokenID, xfer.Amount).Return(nil).Once()
	// 3) Then ownerDb.UpdateOwnership
	ownerDb.On("UpdateOwnership", mock.AnythingOfType("*sql.Tx"), xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount).
		Return(nil).Once()

	// apply
	err = action.applyTransfer(tx, xfer)
	require.NoError(t, err)

	// check expectations
	xferDb.AssertExpectations(t)
	nftDb.AssertExpectations(t)
	ownerDb.AssertExpectations(t)

	// We'll do the actual commit or rollback
	mockDB.ExpectRollback() // We didn't define any real statements, so let's just rollback
	_ = tx.Rollback()
	err = mockDB.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestDefaultTdhTransfersReceivedAction_applyTransfer_Burn(t *testing.T) {
	// Similar approach as above, but Type=BURN => calls nftDb.UpdateBurntSupply
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	tx, _ := db.Begin()

	tracker := mocks.NewTdhIdxTrackerDb(t)
	xferDb := &mockTransferDb{}
	ownerDb := &mockOwnerDb{}
	nftDb := &mockNFTDb{}

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{
		Type:     models.BURN,
		Contract: "0xC",
		TokenID:  "Tburn",
		Amount:   5,
		From:     "0xUser",
		To:       "0x0000000000000000000000000000000000000000",
	}

	// expectations
	xferDb.On("StoreTransfer", mock.AnythingOfType("*sql.Tx"), xfer).Return(nil).Once()
	nftDb.On("UpdateBurntSupply", mock.AnythingOfType("*sql.Tx"), xfer.Contract, xfer.TokenID, xfer.Amount).Return(nil).Once()
	ownerDb.On("UpdateOwnership", mock.AnythingOfType("*sql.Tx"), xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount).
		Return(nil).Once()

	err = action.applyTransfer(tx, xfer)
	require.NoError(t, err)

	xferDb.AssertExpectations(t)
	nftDb.AssertExpectations(t)
	ownerDb.AssertExpectations(t)

	mockDB.ExpectRollback()
	_ = tx.Rollback()
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_applyTransfer_StoreTransferFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	tx, _ := db.Begin()

	tracker := mocks.NewTdhIdxTrackerDb(t)
	xferDb := &mockTransferDb{}
	ownerDb := &mockOwnerDb{}
	nftDb := &mockNFTDb{}

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{
		Type:     models.MINT,
		Contract: "0xC",
		TokenID:  "T1",
		Amount:   1,
	}

	xferDb.On("StoreTransfer", mock.AnythingOfType("*sql.Tx"), xfer).Return(errors.New("store fail")).Once()

	err = action.applyTransfer(tx, xfer)
	assert.ErrorContains(t, err, "failed to store transfer")

	// No calls to nftDb or ownerDb are expected because we fail early
	xferDb.AssertExpectations(t)
	nftDb.AssertNotCalled(t, "UpdateSupply", mock.Anything)
	ownerDb.AssertNotCalled(t, "UpdateOwnership", mock.Anything)

	mockDB.ExpectRollback()
	_ = tx.Rollback()
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

// --------------------------------
// Test applyTransferReverse
// --------------------------------
func TestDefaultTdhTransfersReceivedAction_applyTransferReverse_Burn(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	tx, _ := db.Begin()

	tracker := mocks.NewTdhIdxTrackerDb(t)
	xferDb := &mockTransferDb{}
	ownerDb := &mockOwnerDb{}
	nftDb := &mockNFTDb{}

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{
		Type:     models.BURN,
		Contract: "0xC",
		TokenID:  "T2",
		Amount:   5,
	}

	// For a forward BURN => applyReverse calls UpdateBurntSupplyReverse + UpdateOwnershipReverse
	nftDb.On("UpdateBurntSupplyReverse", tx, xfer.Contract, xfer.TokenID, xfer.Amount).Return(nil).Once()
	ownerDb.On("UpdateOwnershipReverse", tx, xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount).Return(nil).Once()

	err = action.applyTransferReverse(tx, xfer)
	require.NoError(t, err)

	nftDb.AssertExpectations(t)
	ownerDb.AssertExpectations(t)

	mockDB.ExpectRollback()
	_ = tx.Rollback()
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

// We can do similar for MINT => applyTransferReverse calls UpdateSupplyReverse + UpdateOwnershipReverse

// --------------------------------
// Test reset
// --------------------------------
func TestDefaultTdhTransfersReceivedAction_reset(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Expect begin success
	mockDB.ExpectBegin()

	// We'll gather all transfers after checkpoint => return a slice with 2 transfers
	xferDb := &mockTransferDb{}
	xfer1 := models.TokenTransfer{BlockNumber: 10, TxHash: "0x1", Type: models.MINT}
	xfer2 := models.TokenTransfer{BlockNumber: 12, TxHash: "0x2", Type: models.BURN}
	xferDb.On("GetTransfersAfterCheckpoint", mock.Anything, uint64(10), uint64(2), uint64(3)).
		Return([]models.TokenTransfer{xfer1, xfer2}, nil).Once()

	// We expect them to be sorted descending. Our code sorts them, then applyTransferReverse each
	// For xfer1 => MINT => calls nftDb.UpdateSupplyReverse, ownerDb.UpdateOwnershipReverse
	// For xfer2 => BURN => calls nftDb.UpdateBurntSupplyReverse, ownerDb.UpdateOwnershipReverse
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	// Reverse xfer1 => MINT
	nftDb.On("UpdateSupplyReverse", mock.Anything, xfer1.Contract, xfer1.TokenID, xfer1.Amount).Return(nil).Once()
	ownerDb.On("UpdateOwnershipReverse", mock.Anything, xfer1.From, xfer1.To, xfer1.Contract, xfer1.TokenID, xfer1.Amount).Return(nil).Once()

	// Reverse xfer2 => BURN
	nftDb.On("UpdateBurntSupplyReverse", mock.Anything, xfer2.Contract, xfer2.TokenID, xfer2.Amount).Return(nil).Once()
	ownerDb.On("UpdateOwnershipReverse", mock.Anything, xfer2.From, xfer2.To, xfer2.Contract, xfer2.TokenID, xfer2.Amount).Return(nil).Once()

	// Then DeleteTransfersAfterCheckpoint
	xferDb.On("DeleteTransfersAfterCheckpoint", mock.Anything, uint64(10), uint64(2), uint64(3)).Return(nil).Once()

	// Then getLatestTransfer => returns nil => so we do "DELETE FROM token_transfers_checkpoint"
	xferDb.On("GetLatestTransfer", mock.Anything).Return((*models.TokenTransfer)(nil), nil).Once()

	// Expect we run that DELETE
	mockDB.ExpectExec("DELETE FROM token_transfers_checkpoint").WillReturnResult(sqlmock.NewResult(0, 0))

	// Then we commit
	mockDB.ExpectCommit()

	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	err = action.reset(10, 2, 3)
	require.NoError(t, err)

	// Verify
	xferDb.AssertExpectations(t)
	nftDb.AssertExpectations(t)
	ownerDb.AssertExpectations(t)
	err = mockDB.ExpectationsWereMet()
	assert.NoError(t, err)
}

// --------------------------------
// Test Handle
// --------------------------------
func TestDefaultTdhTransfersReceivedAction_Handle_NoTransfers(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	tracker := mocks.NewTdhIdxTrackerDb(t)
	xferDb := &mockTransferDb{}
	ownerDb := &mockOwnerDb{}
	nftDb := &mockNFTDb{}

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	err = action.Handle(models.TokenTransferBatch{
		BlockNumber: 12,
		Transfers:   nil, // empty
	})
	require.NoError(t, err)

	// No progress set, no calls to xferDb
	err = mockDB.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestDefaultTdhTransfersReceivedAction_Handle_Success(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// We'll have 2 transfers in the batch => single chunk
	// 1) BeginTx => success
	mockDB.ExpectBegin()

	// 2) getLastSavedCheckpoint => no row => returns 0,0,0
	rows := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"}).FromCSVString("")
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(rows)

	// We'll apply each transfer => calls to xferDb, nftDb, ownerDb
	// Then update checkpoint => let's allow success
	mockDB.ExpectExec("INSERT INTO token_transfers_checkpoint").WillReturnResult(sqlmock.NewResult(1, 1))

	// End => commit
	mockDB.ExpectCommit()

	tracker := mocks.NewTdhIdxTrackerDb(t)
	tracker.On("SetProgress", uint64(12), mock.Anything).
		Return(nil).
		Once()

	// Now let's define the TransferDb / OwnerDb / NFTDb mocks
	xferDb := &mockTransferDb{}
	// We have 2 transfers => store each
	xfer1 := models.TokenTransfer{BlockNumber: 12, TransactionIndex: 0, LogIndex: 0, TxHash: "0xAAA", Type: models.MINT, Contract: "0xC", TokenID: "T1", Amount: 1}
	xfer2 := models.TokenTransfer{BlockNumber: 12, TransactionIndex: 0, LogIndex: 1, TxHash: "0xAAA", Type: models.BURN, Contract: "0xC", TokenID: "T2", Amount: 2}
	xferDb.On("StoreTransfer", mock.AnythingOfType("*sql.Tx"), xfer1).Return(nil).Once()
	xferDb.On("StoreTransfer", mock.AnythingOfType("*sql.Tx"), xfer2).Return(nil).Once()

	// no calls to GetTransfersAfterCheckpoint or DeleteTransfersAfterCheckpoint or GetLatestTransfer in normal flow
	// (unless there's a mismatch)

	ownerDb := &mockOwnerDb{}
	// for xfer1 => MINT => calls nftDb.UpdateSupply + ownerDb.UpdateOwnership
	// for xfer2 => BURN => calls nftDb.UpdateBurntSupply + ownerDb.UpdateOwnership
	nftDb := &mockNFTDb{}
	nftDb.On("UpdateSupply", mock.Anything, xfer1.Contract, xfer1.TokenID, xfer1.Amount).Return(nil).Once()
	ownerDb.On("UpdateOwnership", mock.Anything, xfer1.From, xfer1.To, xfer1.Contract, xfer1.TokenID, xfer1.Amount).Return(nil).Once()

	nftDb.On("UpdateBurntSupply", mock.Anything, xfer2.Contract, xfer2.TokenID, xfer2.Amount).Return(nil).Once()
	ownerDb.On("UpdateOwnership", mock.Anything, xfer2.From, xfer2.To, xfer2.Contract, xfer2.TokenID, xfer2.Amount).Return(nil).Once()

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	batch := models.TokenTransferBatch{
		BlockNumber: 12,
		Transfers:   []models.TokenTransfer{xfer1, xfer2},
	}
	err = action.Handle(batch)
	require.NoError(t, err)

	// Check all mocks
	xferDb.AssertExpectations(t)
	nftDb.AssertExpectations(t)
	ownerDb.AssertExpectations(t)
	tracker.AssertExpectations(t)

	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_CheckpointMismatchResets(t *testing.T) {
	// We'll test that if the first new transfer is behind the last checkpoint => reset is called
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// 1) Begin => success
	mockDB.ExpectBegin()
	// 2) getLastSavedCheckpoint => let's say we have block=10, txIndex=5, logIndex=2
	row := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"}).
		AddRow(10, 5, 2)
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(row)
	// => mismatch with a new transfer block=10, txIndex=5, logIndex=1 => behind => triggers reset
	// The code calls a.reset(...) => that leads to a.db.BeginTx => let's set up that second transaction

	// second tx for reset
	mockDB.ExpectBegin()
	// => calls xferDb.GetTransfersAfterCheckpoint(10,5,1) => return some list
	xferDb := &mockTransferDb{}
	xferDb.On("GetTransfersAfterCheckpoint", mock.AnythingOfType("*sql.Tx"), uint64(10), uint64(5), uint64(1)).
		Return([]models.TokenTransfer{}, nil).Once() // suppose none exist, so no reversals

	// => calls xferDb.DeleteTransfersAfterCheckpoint => success
	xferDb.On("DeleteTransfersAfterCheckpoint", mock.AnythingOfType("*sql.Tx"), uint64(10), uint64(5), uint64(1)).
		Return(nil).Once()

	// => calls xferDb.GetLatestTransfer => returns nil => so "DELETE FROM token_transfers_checkpoint"
	xferDb.On("GetLatestTransfer", mock.AnythingOfType("*sql.Tx")).
		Return((*models.TokenTransfer)(nil), nil).Once()

	// Expect a DELETE statement on token_transfers_checkpoint
	mockDB.ExpectExec("DELETE FROM token_transfers_checkpoint").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// => then commit
	mockDB.ExpectCommit()

	// back to the original transaction => we continue in the `Handle` method => we apply the new transfer
	// Actually the code calls applyTransfer for each chunk => let's define that chunk has 1 xfer
	// We'll see "StoreTransfer" ...
	// Then "updateCheckpoint" => an Exec => then commit
	mockDB.ExpectExec("INSERT INTO token_transfers_checkpoint").
		WillReturnResult(sqlmock.NewResult(1, 1)) // success

	mockDB.ExpectCommit()

	// After commit => it calls tracker.SetProgress(10)
	tracker := mocks.NewTdhIdxTrackerDb(t)
	tracker.On("SetProgress", uint64(10), mock.Anything).Return(nil).Once()

	// mocks for NFTDb/OwnerDb usage
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}

	// We'll have 1 xfer => MINT => calls xferDb.StoreTransfer => UpdateSupply => UpdateOwnership
	newXfer := models.TokenTransfer{
		BlockNumber: 10, TransactionIndex: 5, LogIndex: 1, Type: models.MINT, Amount: 1,
	}
	xferDb.On("StoreTransfer", mock.AnythingOfType("*sql.Tx"), newXfer).Return(nil).Once()
	nftDb.On("UpdateSupply", mock.AnythingOfType("*sql.Tx"), newXfer.Contract, newXfer.TokenID, newXfer.Amount).Return(nil).Once()
	ownerDb.On("UpdateOwnership", mock.AnythingOfType("*sql.Tx"), newXfer.From, newXfer.To, newXfer.Contract, newXfer.TokenID, newXfer.Amount).Return(nil).Once()

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	batch := models.TokenTransferBatch{
		BlockNumber: 10,
		Transfers:   []models.TokenTransfer{newXfer},
	}

	err = action.Handle(batch)
	require.NoError(t, err)

	xferDb.AssertExpectations(t)
	nftDb.AssertExpectations(t)
	ownerDb.AssertExpectations(t)
	tracker.AssertExpectations(t)
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_ErrorInSetProgress(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// 1) begin => success
	mockDB.ExpectBegin()
	// 2) get checkpoint => no row => 0,0,0
	rows := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"})
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(rows)
	// 3) we have 1 xfer => store it => update checkpoint => then commit
	mockDB.ExpectExec("INSERT INTO token_transfers_checkpoint").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectCommit()

	// Then setProgress fails
	tracker := mocks.NewTdhIdxTrackerDb(t)
	tracker.On("SetProgress", uint64(5), mock.Anything).
		Return(errors.New("progress fail")).
		Once()

	xferDb := &mockTransferDb{}
	ownerDb := &mockOwnerDb{}
	nftDb := &mockNFTDb{}

	// The transfer => Type=MINT => calls xferDb.StoreTransfer, nftDb.UpdateSupply, ownerDb.UpdateOwnership
	transfer := models.TokenTransfer{
		BlockNumber: 5,
		Type:        models.MINT,
		Amount:      2,
	}
	xferDb.On("StoreTransfer", mock.AnythingOfType("*sql.Tx"), transfer).Return(nil).Once()
	nftDb.On("UpdateSupply", mock.AnythingOfType("*sql.Tx"), transfer.Contract, transfer.TokenID, transfer.Amount).Return(nil).Once()
	ownerDb.On("UpdateOwnership", mock.AnythingOfType("*sql.Tx"), transfer.From, transfer.To, transfer.Contract, transfer.TokenID, transfer.Amount).Return(nil).Once()

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	batch := models.TokenTransferBatch{
		BlockNumber: 5,
		Transfers:   []models.TokenTransfer{transfer},
	}

	err = action.Handle(batch)
	assert.ErrorContains(t, err, "progress fail")

	// The DB transaction is already committed when setProgress is called => so data is persisted
	xferDb.AssertExpectations(t)
	nftDb.AssertExpectations(t)
	ownerDb.AssertExpectations(t)
	tracker.AssertExpectations(t)
	assert.NoError(t, mockDB.ExpectationsWereMet())
}
