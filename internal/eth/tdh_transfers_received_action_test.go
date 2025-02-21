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

//---------------------------------
// Mock definitions for TransferDb, OwnerDb, NFTDb
//---------------------------------

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

//---------------------------------
// Test for NewTdhTransfersReceivedActionImpl
//---------------------------------

func TestNewTdhTransfersReceivedActionImpl_BeginTxFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Expect begin to fail
	mockDB.ExpectBegin().WillReturnError(errors.New("boom begin tx"))

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
	assert.Nil(t, action, "Expected nil if BeginTx fails")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestNewTdhTransfersReceivedActionImpl_GetCheckpointFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectQuery("SELECT block_number").WillReturnError(errors.New("get checkpoint fail"))

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
	assert.Nil(t, action, "Should be nil on checkpoint error")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestNewTdhTransfersReceivedActionImpl_SuccessNoCheckpoint(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	rows := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"}).
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
	require.NotNil(t, action, "Should succeed if no checkpoint row")

	assert.NoError(t, mockDB.ExpectationsWereMet())
}

//---------------------------------
// applyTransfer tests
//---------------------------------

func TestDefaultTdhTransfersReceivedAction_applyTransfer_StoreFails(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{Type: models.MINT, Contract: "0xC", TokenID: "T1", Amount: 1}

	// match the tx argument by a function that returns true (no reflection on it)
	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		mock.Anything,
	).Return(errors.New("store fail"))

	err = action.applyTransfer(nil, xfer)
	assert.ErrorContains(t, err, "failed to store transfer")

	// We are NOT calling xferDb.AssertExpectations(t) because that triggers reflection
}

func TestDefaultTdhTransfersReceivedAction_applyTransfer_MintSupplyFails(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{Type: models.MINT, Contract: "0xC", TokenID: "T2", Amount: 5}

	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		mock.Anything,
	).Return(nil)

	nftDb.On("UpdateSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(errors.New("supply fail"))

	err = action.applyTransfer(nil, xfer)
	assert.ErrorContains(t, err, "failed to update NFT supply")
}

func TestDefaultTdhTransfersReceivedAction_applyTransfer_BurnSupplyFails(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{Type: models.BURN, Contract: "0xC", TokenID: "BurnMe", Amount: 2}

	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		mock.Anything,
	).Return(nil)

	nftDb.On("UpdateBurntSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(errors.New("burn fail"))

	err = action.applyTransfer(nil, xfer)
	assert.ErrorContains(t, err, "failed to update NFT burnt supply")
}

func TestDefaultTdhTransfersReceivedAction_applyTransfer_OwnershipFails(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{Type: models.MINT, Contract: "0xC", TokenID: "Ow1", Amount: 10}

	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		mock.Anything,
	).Return(nil)

	nftDb.On("UpdateSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	ownerDb.On("UpdateOwnership",
		mock.AnythingOfType("*sql.Tx"),
		xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(errors.New("ownership fail"))

	err = action.applyTransfer(nil, xfer)
	assert.ErrorContains(t, err, "failed to update ownership")
}

func TestDefaultTdhTransfersReceivedAction_applyTransfer_Success(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

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
		TokenID:  "SuccessT",
		Amount:   3,
		From:     "0x0000000000000000000000000000000000000000",
		To:       "0xSomeUser",
	}

	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		mock.Anything,
	).Return(nil)

	nftDb.On("UpdateSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	ownerDb.On("UpdateOwnership",
		mock.AnythingOfType("*sql.Tx"),
		xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	err = action.applyTransfer(nil, xfer)
	assert.NoError(t, err)
}

//---------------------------------
// applyTransferReverse tests
//---------------------------------

func TestDefaultTdhTransfersReceivedAction_applyTransferReverse_BurnReverseFails(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{Type: models.BURN, Contract: "0xC", TokenID: "burnR", Amount: 6}

	nftDb.On("UpdateBurntSupplyReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(errors.New("burn reverse fail"))

	err = action.applyTransferReverse(nil, xfer)
	assert.ErrorContains(t, err, "failed to revert burnt supply")
}

func TestDefaultTdhTransfersReceivedAction_applyTransferReverse_MintReverseFails(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{Type: models.MINT, Contract: "0xC", TokenID: "mintR", Amount: 2}

	nftDb.On("UpdateSupplyReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(errors.New("supply reverse fail"))

	err = action.applyTransferReverse(nil, xfer)
	assert.ErrorContains(t, err, "failed to revert minted supply")
}

func TestDefaultTdhTransfersReceivedAction_applyTransferReverse_OwnerReverseFails(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{Type: models.BURN, Contract: "0xC", TokenID: "owRev", Amount: 10}

	nftDb.On("UpdateBurntSupplyReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	ownerDb.On("UpdateOwnershipReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(errors.New("reverse ownership fail"))

	err = action.applyTransferReverse(nil, xfer)
	assert.ErrorContains(t, err, "failed to revert ownership")
}

func TestDefaultTdhTransfersReceivedAction_applyTransferReverse_Success(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	xfer := models.TokenTransfer{Type: models.BURN, Contract: "0xC", TokenID: "RevOk", Amount: 5}

	nftDb.On("UpdateBurntSupplyReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	ownerDb.On("UpdateOwnershipReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	err = action.applyTransferReverse(nil, xfer)
	assert.NoError(t, err)
}

//---------------------------------
// reset tests
//---------------------------------

func TestDefaultTdhTransfersReceivedAction_reset_GetTransfersFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	xferDb := &mockTransferDb{}
	xferDb.On("GetTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(10), uint64(2), uint64(3),
	).Return([]models.TokenTransfer{}, errors.New("get after fail"))

	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	err = action.reset(tx, 10, 2, 3)
	assert.ErrorContains(t, err, "failed to get transfers")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_reset_ApplyReverseFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	xferDb := &mockTransferDb{}
	xfer1 := models.TokenTransfer{Type: models.MINT, Amount: 5}

	xferDb.On("GetTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(1), uint64(2), uint64(3),
	).Return([]models.TokenTransfer{xfer1}, nil)

	nftDb := &mockNFTDb{}
	// MINT => UpdateSupplyReverse => fail
	nftDb.On("UpdateSupplyReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer1.Contract, xfer1.TokenID, xfer1.Amount,
	).Return(errors.New("reverse supply fail"))

	mockDB.ExpectRollback()

	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	err = action.reset(tx, 1, 2, 3)
	assert.ErrorContains(t, err, "reverse supply fail")
}

func TestDefaultTdhTransfersReceivedAction_reset_DeleteTransfersFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	tx := &sql.Tx{}

	xferDb := &mockTransferDb{}
	xfer1 := models.TokenTransfer{BlockNumber: 10}

	xferDb.On("GetTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(5), uint64(6), uint64(7),
	).Return([]models.TokenTransfer{xfer1}, nil)

	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}

	nftDb.On("UpdateSupplyReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer1.Contract, xfer1.TokenID, xfer1.Amount,
	).Return(nil)
	ownerDb.On("UpdateOwnershipReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer1.From, xfer1.To, xfer1.Contract, xfer1.TokenID, xfer1.Amount,
	).Return(nil)

	xferDb.On("DeleteTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(5), uint64(6), uint64(7),
	).Return(errors.New("delete fail"))

	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	err = action.reset(tx, 5, 6, 7)
	assert.ErrorContains(t, err, "failed to delete transfers")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_reset_GetLatestFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	xferDb := &mockTransferDb{}
	xferDb.On("GetTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(10), uint64(0), uint64(1),
	).Return([]models.TokenTransfer{}, nil)

	xferDb.On("DeleteTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(10), uint64(0), uint64(1),
	).Return(nil)

	xferDb.On("GetLatestTransfer",
		mock.AnythingOfType("*sql.Tx"),
	).Return((*models.TokenTransfer)(nil), errors.New("get latest fail"))

	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	err = action.reset(tx, 10, 0, 1)
	assert.ErrorContains(t, err, "failed to get latest transfer")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_reset_UpdateCheckpointFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	xferDb := &mockTransferDb{}
	xferDb.On("GetTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(4), uint64(5), uint64(6),
	).Return([]models.TokenTransfer{}, nil)

	xferDb.On("DeleteTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(4), uint64(5), uint64(6),
	).Return(nil)

	xfer := &models.TokenTransfer{BlockNumber: 99}
	xferDb.On("GetLatestTransfer",
		mock.AnythingOfType("*sql.Tx"),
	).Return(xfer, nil)

	mockDB.ExpectExec("INSERT INTO token_transfers_checkpoint").
		WillReturnError(errors.New("update ckp fail"))

	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	err = action.reset(tx, 4, 5, 6)
	assert.ErrorContains(t, err, "failed to update checkpoint")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_reset_Success(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	xfer1 := models.TokenTransfer{BlockNumber: 10, Type: models.MINT, Amount: 1}
	xfer2 := models.TokenTransfer{BlockNumber: 12, Type: models.BURN, Amount: 2}

	xferDb.On("GetTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(9), uint64(0), uint64(0),
	).Return([]models.TokenTransfer{xfer1, xfer2}, nil)

	nftDb.On("UpdateSupplyReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer1.Contract, xfer1.TokenID, xfer1.Amount,
	).Return(nil)
	ownerDb.On("UpdateOwnershipReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer1.From, xfer1.To, xfer1.Contract, xfer1.TokenID, xfer1.Amount,
	).Return(nil)

	nftDb.On("UpdateBurntSupplyReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer2.Contract, xfer2.TokenID, xfer2.Amount,
	).Return(nil)
	ownerDb.On("UpdateOwnershipReverse",
		mock.AnythingOfType("*sql.Tx"),
		xfer2.From, xfer2.To, xfer2.Contract, xfer2.TokenID, xfer2.Amount,
	).Return(nil)

	xferDb.On("DeleteTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(9), uint64(0), uint64(0),
	).Return(nil)

	xferDb.On("GetLatestTransfer",
		mock.AnythingOfType("*sql.Tx"),
	).Return((*models.TokenTransfer)(nil), nil)

	mockDB.ExpectExec("DELETE FROM token_transfers_checkpoint").
		WillReturnResult(sqlmock.NewResult(0, 0))

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	err = action.reset(tx, 9, 0, 0)
	require.NoError(t, err)
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

//---------------------------------
// Handle tests
//---------------------------------

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
	err = action.Handle(models.TokenTransferBatch{Transfers: nil})
	require.NoError(t, err)

	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_BeginTxFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin().WillReturnError(errors.New("boom begin handle"))

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

	batch := models.TokenTransferBatch{BlockNumber: 10, Transfers: []models.TokenTransfer{{}}}
	err = action.Handle(batch)
	assert.ErrorContains(t, err, "failed to begin transaction")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_GetCheckpointFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectQuery("SELECT block_number").WillReturnError(errors.New("ckp fail"))
	mockDB.ExpectRollback()

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

	batch := models.TokenTransferBatch{Transfers: []models.TokenTransfer{{}}}
	err = action.Handle(batch)
	assert.ErrorContains(t, err, "failed to get last saved checkpoint")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_CheckpointMismatchResetFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	row := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"}).
		AddRow(10, 5, 2)
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(row)

	// // second Begin => fails
	// mockDB.ExpectBegin().WillReturnError(errors.New("reset begin fail"))

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

	batch := models.TokenTransferBatch{
		BlockNumber: 10,
		Transfers: []models.TokenTransfer{
			{BlockNumber: 10, TransactionIndex: 5, LogIndex: 1},
		},
	}

	xferDb.On("GetTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(10), uint64(5), uint64(1),
	).Return([]models.TokenTransfer{}, errors.New("get transfers fail"))

	err = action.Handle(batch)
	assert.ErrorContains(t, err, "get transfers fail")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_CheckpointMismatchResetSuccess(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// first begin => success
	mockDB.ExpectBegin()
	row := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"}).
		AddRow(10, 5, 2)
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(row)

	xferDb := &mockTransferDb{}
	xferDb.On("GetTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(10), uint64(5), uint64(1),
	).Return([]models.TokenTransfer{}, nil)
	xferDb.On("DeleteTransfersAfterCheckpoint",
		mock.AnythingOfType("*sql.Tx"),
		uint64(10), uint64(5), uint64(1),
	).Return(nil)
	xferDb.On("GetLatestTransfer",
		mock.AnythingOfType("*sql.Tx"),
	).Return((*models.TokenTransfer)(nil), nil)

	mockDB.ExpectExec("DELETE FROM token_transfers_checkpoint").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// now re-apply chunk
	xfer := models.TokenTransfer{BlockNumber: 10, TransactionIndex: 5, LogIndex: 1, Type: models.MINT, Amount: 1}
	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		mock.Anything,
	).Return(nil)

	nftDb := &mockNFTDb{}
	nftDb.On("UpdateSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	ownerDb := &mockOwnerDb{}
	ownerDb.On("UpdateOwnership",
		mock.AnythingOfType("*sql.Tx"),
		xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	mockDB.ExpectExec("INSERT INTO token_transfers_checkpoint").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectCommit()

	tracker := mocks.NewTdhIdxTrackerDb(t)
	tracker.On("SetProgress", uint64(10), mock.Anything).Return(nil)

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
		Transfers:   []models.TokenTransfer{xfer},
	}

	err = action.Handle(batch)
	require.NoError(t, err)

	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_ApplyTransferFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	row := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"})
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(row)
	mockDB.ExpectRollback()

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

	xfer := models.TokenTransfer{BlockNumber: 5, TxHash: "0xX", Type: models.BURN, Amount: 1}
	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		mock.Anything,
	).Return(errors.New("store err"))

	batch := models.TokenTransferBatch{
		BlockNumber: 5,
		Transfers:   []models.TokenTransfer{xfer},
	}
	err = action.Handle(batch)
	assert.ErrorContains(t, err, "failed to store transfer")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_UpdateCheckpointFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	empty := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"})
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(empty)

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	xfer := models.TokenTransfer{BlockNumber: 5, Type: models.MINT, Amount: 2}
	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		mock.Anything,
	).Return(nil)

	nftDb.On("UpdateSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	ownerDb.On("UpdateOwnership",
		mock.AnythingOfType("*sql.Tx"),
		xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	mockDB.ExpectExec("INSERT INTO token_transfers_checkpoint").
		WillReturnError(errors.New("checkpoint fail"))
	mockDB.ExpectRollback()

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}
	batch := models.TokenTransferBatch{BlockNumber: 5, Transfers: []models.TokenTransfer{xfer}}

	err = action.Handle(batch)
	assert.ErrorContains(t, err, "checkpoint fail")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_CommitFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	row := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"})
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(row)

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	xfer := models.TokenTransfer{BlockNumber: 5, Type: models.BURN, Amount: 2}
	xferDb.On("StoreTransfer", mock.AnythingOfType("*sql.Tx"), mock.Anything).
		Return(nil)
	nftDb.On("UpdateBurntSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)
	ownerDb.On("UpdateOwnership",
		mock.AnythingOfType("*sql.Tx"),
		xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	mockDB.ExpectExec("INSERT INTO token_transfers_checkpoint").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectCommit().WillReturnError(errors.New("commit fail"))

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}
	batch := models.TokenTransferBatch{BlockNumber: 5, Transfers: []models.TokenTransfer{xfer}}
	err = action.Handle(batch)
	assert.ErrorContains(t, err, "failed to commit transaction")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_SetProgressFails(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	row := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"})
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(row)

	mockDB.ExpectExec("INSERT INTO token_transfers_checkpoint").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectCommit()

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	xfer := models.TokenTransfer{BlockNumber: 7, Type: models.MINT, Amount: 3, TxHash: "0xAbc"}

	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		mock.Anything,
	).Return(nil)
	nftDb.On("UpdateSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)
	ownerDb.On("UpdateOwnership",
		mock.AnythingOfType("*sql.Tx"),
		xfer.From, xfer.To, xfer.Contract, xfer.TokenID, xfer.Amount,
	).Return(nil)

	tracker.On("SetProgress", uint64(7), mock.Anything).Return(errors.New("progress fail"))

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}
	batch := models.TokenTransferBatch{BlockNumber: 7, Transfers: []models.TokenTransfer{xfer}}

	err = action.Handle(batch)
	assert.ErrorContains(t, err, "progress fail")
	assert.NoError(t, mockDB.ExpectationsWereMet())
}

func TestDefaultTdhTransfersReceivedAction_Handle_Success(t *testing.T) {
	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectBegin()
	row := sqlmock.NewRows([]string{"block_number", "transaction_index", "log_index"})
	mockDB.ExpectQuery("SELECT block_number").WillReturnRows(row)

	xfer1 := models.TokenTransfer{BlockNumber: 10, TransactionIndex: 1, LogIndex: 0, Type: models.MINT, Amount: 1}
	xfer2 := models.TokenTransfer{BlockNumber: 10, TransactionIndex: 1, LogIndex: 1, Type: models.BURN, Amount: 2}

	xferDb := &mockTransferDb{}
	nftDb := &mockNFTDb{}
	ownerDb := &mockOwnerDb{}
	tracker := mocks.NewTdhIdxTrackerDb(t)

	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		xfer1,
	).Return(nil)
	nftDb.On("UpdateSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer1.Contract, xfer1.TokenID, xfer1.Amount,
	).Return(nil)
	ownerDb.On("UpdateOwnership",
		mock.AnythingOfType("*sql.Tx"),
		xfer1.From, xfer1.To, xfer1.Contract, xfer1.TokenID, xfer1.Amount,
	).Return(nil)

	xferDb.On("StoreTransfer",
		mock.AnythingOfType("*sql.Tx"),
		xfer2, // or mock.Anything
	).Return(nil)
	nftDb.On("UpdateBurntSupply",
		mock.AnythingOfType("*sql.Tx"),
		xfer2.Contract, xfer2.TokenID, xfer2.Amount,
	).Return(nil)
	ownerDb.On("UpdateOwnership",
		mock.AnythingOfType("*sql.Tx"),
		xfer2.From, xfer2.To, xfer2.Contract, xfer2.TokenID, xfer2.Amount,
	).Return(nil)

	mockDB.ExpectExec("INSERT INTO token_transfers_checkpoint").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectCommit()

	tracker.On("SetProgress", uint64(10), mock.Anything).Return(nil)

	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		db:              db,
		progressTracker: tracker,
		transferDb:      xferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}

	batch := models.TokenTransferBatch{BlockNumber: 10, Transfers: []models.TokenTransfer{xfer1, xfer2}}
	err = action.Handle(batch)
	require.NoError(t, err)
	assert.NoError(t, mockDB.ExpectationsWereMet())
}
