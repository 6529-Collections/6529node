package eth

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/6529-Collections/6529node/internal/eth/mocks"
	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------
// Basic Helpers Tests
// -----------------------

func TestCheckpointHelpers(t *testing.T) {
	transfer := tokens.TokenTransfer{
		BlockNumber:      123,
		TransactionIndex: 45,
		LogIndex:         6,
	}

	val := checkpointValue(transfer)
	assert.Equal(t, "123:45:6", val)

	block, txi, log, err := parseCheckpoint("123:45:6")
	require.NoError(t, err)
	assert.Equal(t, uint64(123), block)
	assert.Equal(t, uint64(45), txi)
	assert.Equal(t, uint64(6), log)

	_, _, _, err = parseCheckpoint("not_valid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid checkpoint format")
}

func TestGetLastSavedCheckpoint(t *testing.T) {
	db := setupTestInMemoryDB(t)
	err := db.View(func(txn *badger.Txn) error {
		val, err := getLastSavedCheckpoint(txn)
		require.NoError(t, err)
		assert.Equal(t, "0:0:0", string(val))
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(actionsReceivedCheckpointKey), []byte("invalid_data"))
	})
	require.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		val, err := getLastSavedCheckpoint(txn)
		require.NoError(t, err)
		assert.Equal(t, "invalid_data", string(val))
		return nil
	})
	require.NoError(t, err)
}

func TestNewTdhTransfersReceivedActionImpl_EmptyDB(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	action := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, action)
}

func TestNewTdhTransfersReceivedActionImpl_SomeTransfers(t *testing.T) {
	db := setupTestInMemoryDB(t)
	err := db.Update(func(txn *badger.Txn) error {
		transferDb := NewTransferDb()
		transfers := []tokens.TokenTransfer{
			{
				TxHash:           "0xabc",
				BlockNumber:      2,
				TransactionIndex: 0,
				LogIndex:         0,
				Contract:         constants.MEMES_CONTRACT,
			},
			{
				TxHash:           "0xdef",
				BlockNumber:      1,
				TransactionIndex: 0,
				LogIndex:         1,
				Contract:         constants.GRADIENTS_CONTRACT,
			},
		}
		for _, tr := range transfers {
			if e := transferDb.StoreTransfer(txn, tr); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err)

	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	action := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, action)
}

func TestDefaultTdhTransfersReceivedAction_NoTransfers(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, orchestrator)

	err := orchestrator.Handle(tokens.TokenTransferBatch{
		Transfers:   []tokens.TokenTransfer{},
		BlockNumber: 0,
	})
	require.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(actionsReceivedCheckpointKey))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return fmt.Errorf("expected no checkpoint key, got something else: %v", err)
	})
	require.NoError(t, err)
}

func TestDefaultTdhTransfersReceivedAction_BasicFlow(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, orchestrator)

	transfers := []tokens.TokenTransfer{
		{
			From:             constants.NULL_ADDRESS,
			To:               "0xUser1",
			Contract:         "0xNFT",
			TokenID:          "10",
			Amount:           3,
			BlockNumber:      1,
			TransactionIndex: 0,
			LogIndex:         0,
			TxHash:           "0xMintTx",
			Type:             tokens.MINT,
		},
		{
			From:             "0xUser1",
			To:               "0xUser2",
			Contract:         "0xNFT",
			TokenID:          "10",
			Amount:           1,
			BlockNumber:      1,
			TransactionIndex: 0,
			LogIndex:         1,
			TxHash:           "0xTransferTx1",
		},
		{
			From:             "0xUser1",
			To:               "0xUser2",
			Contract:         "0xNFT",
			TokenID:          "10",
			Amount:           2,
			BlockNumber:      1,
			TransactionIndex: 0,
			LogIndex:         2,
			TxHash:           "0xTransferTx2",
		},
	}

	transfersBatch := tokens.TokenTransferBatch{
		Transfers:   transfers,
		BlockNumber: 1,
	}

	progressTracker.On("SetProgress", uint64(1)).Return(nil)
	err := orchestrator.Handle(transfersBatch)
	require.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		ownerDb := NewOwnerDb()
		nftDb := NewNFTDb()

		balUser1, err := ownerDb.GetBalance(txn, "0xUser1", "0xNFT", "10")
		require.NoError(t, err)
		assert.Equal(t, int64(0), balUser1)

		balUser2, err := ownerDb.GetBalance(txn, "0xUser2", "0xNFT", "10")
		require.NoError(t, err)
		assert.Equal(t, int64(3), balUser2)

		nftRec, nftErr := nftDb.GetNFT(txn, "0xNFT", "10")
		require.NoError(t, nftErr)
		require.NotNil(t, nftRec)
		assert.Equal(t, int64(3), nftRec.Supply)
		assert.Equal(t, int64(0), nftRec.BurntSupply)

		item, getErr := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, getErr)
		checkpointVal, copyErr := item.ValueCopy(nil)
		require.NoError(t, copyErr)
		assert.Equal(t, "1:0:2", string(checkpointVal))

		return nil
	})
	require.NoError(t, err)
}

func TestDefaultTdhTransfersReceivedAction_OutOfOrderReset(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, orchestrator)

	// 1) Insert block=10
	batch1 := []tokens.TokenTransfer{
		{
			From:             constants.NULL_ADDRESS,
			To:               "0xUserA",
			Contract:         "0xNFT",
			TokenID:          "101",
			Amount:           5,
			BlockNumber:      10,
			TransactionIndex: 0,
			LogIndex:         0,
			TxHash:           "0xTx1",
			Type:             tokens.MINT,
		},
	}
	transfersBatch := tokens.TokenTransferBatch{
		Transfers:   batch1,
		BlockNumber: 10,
	}
	progressTracker.On("SetProgress", uint64(10)).Return(nil)
	require.NoError(t, orchestrator.Handle(transfersBatch))

	// 2) Next batch is block=9 => triggers a reset
	batch2 := []tokens.TokenTransfer{
		{
			From:             constants.NULL_ADDRESS,
			To:               "0xUserB",
			Contract:         "0xNFT",
			TokenID:          "102",
			Amount:           2,
			BlockNumber:      9,
			TransactionIndex: 1,
			LogIndex:         0,
			TxHash:           "0xOutOfOrder",
			Type:             tokens.MINT,
		},
	}

	progressTracker.On("SetProgress", uint64(9)).Return(nil)
	transfersBatch = tokens.TokenTransferBatch{
		Transfers:   batch2,
		BlockNumber: 9,
	}
	err := orchestrator.Handle(transfersBatch)
	require.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		ownerDb := NewOwnerDb()

		balA, _ := ownerDb.GetBalance(txn, "0xUserA", "0xNFT", "101")
		assert.Equal(t, int64(0), balA) // reset removed block=10

		balB, _ := ownerDb.GetBalance(txn, "0xUserB", "0xNFT", "102")
		assert.Equal(t, int64(2), balB) // minted at block=9

		item, err2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, err2)
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "9:1:0", string(val))
		return nil
	})
	require.NoError(t, err)
}

func TestDefaultTdhTransfersReceivedAction_BurnScenario(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, orchestrator)

	allTransfers := []tokens.TokenTransfer{
		{
			From:             constants.NULL_ADDRESS,
			To:               "0xUserC",
			Contract:         "0xNFT",
			TokenID:          "50",
			Amount:           5,
			BlockNumber:      7,
			TransactionIndex: 1,
			LogIndex:         0,
			TxHash:           "0xMintC",
			Type:             tokens.MINT,
		},
		{
			From:             "0xUserC",
			To:               constants.NULL_ADDRESS,
			Contract:         "0xNFT",
			TokenID:          "50",
			Amount:           2,
			BlockNumber:      7,
			TransactionIndex: 1,
			LogIndex:         1,
			TxHash:           "0xBurnC",
			Type:             tokens.BURN,
		},
		{
			From:             "0xUserC",
			To:               "0xUserD",
			Contract:         "0xNFT",
			TokenID:          "50",
			Amount:           3,
			BlockNumber:      7,
			TransactionIndex: 1,
			LogIndex:         2,
			TxHash:           "0xTransferCD",
		},
	}

	progressTracker.On("SetProgress", uint64(7)).Return(nil)
	transfersBatch := tokens.TokenTransferBatch{
		Transfers:   allTransfers,
		BlockNumber: 7,
	}
	require.NoError(t, orchestrator.Handle(transfersBatch))

	err := db.View(func(txn *badger.Txn) error {
		ownerDb := NewOwnerDb()
		nftDb := NewNFTDb()

		cBal, _ := ownerDb.GetBalance(txn, "0xUserC", "0xNFT", "50")
		assert.Equal(t, int64(0), cBal)

		dBal, _ := ownerDb.GetBalance(txn, "0xUserD", "0xNFT", "50")
		assert.Equal(t, int64(3), dBal)

		nftRec, err := nftDb.GetNFT(txn, "0xNFT", "50")
		require.NoError(t, err)
		require.NotNil(t, nftRec)
		assert.Equal(t, int64(5), nftRec.Supply)
		assert.Equal(t, int64(2), nftRec.BurntSupply)

		item, err2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, err2)
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "7:1:2", string(val))
		return nil
	})
	require.NoError(t, err)
}

func TestDefaultTdhTransfersReceivedAction_InsufficientBalance(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, orchestrator)

	mint := tokens.TokenTransfer{
		From:             constants.NULL_ADDRESS,
		To:               "0xUserX",
		Contract:         "0xNFT",
		TokenID:          "777",
		Amount:           1,
		BlockNumber:      5,
		TransactionIndex: 0,
		LogIndex:         0,
		TxHash:           "0xMintX",
		Type:             tokens.MINT,
	}

	progressTracker.On("SetProgress", uint64(5)).Return(nil)
	batch := tokens.TokenTransferBatch{
		Transfers:   []tokens.TokenTransfer{mint},
		BlockNumber: 5,
	}
	err := orchestrator.Handle(batch)
	require.NoError(t, err)

	badTransfer := tokens.TokenTransfer{
		From:             "0xUserX",
		To:               "0xUserY",
		Contract:         "0xNFT",
		TokenID:          "777",
		Amount:           2, // userX only has 1
		BlockNumber:      6,
		TransactionIndex: 0,
		LogIndex:         0,
		TxHash:           "0xTxInsufficient",
	}

	batch = tokens.TokenTransferBatch{
		Transfers:   []tokens.TokenTransfer{badTransfer},
		BlockNumber: 6,
	}
	err = orchestrator.Handle(batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient balance")

	err = db.View(func(txn *badger.Txn) error {
		ownerDb := NewOwnerDb()
		balUserX, _ := ownerDb.GetBalance(txn, "0xUserX", "0xNFT", "777")
		assert.Equal(t, int64(1), balUserX)

		balUserY, _ := ownerDb.GetBalance(txn, "0xUserY", "0xNFT", "777")
		assert.Equal(t, int64(0), balUserY)

		item, err2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, err2)
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "5:0:0", string(val))
		return nil
	})
	require.NoError(t, err)
}

func TestDefaultTdhTransfersReceivedAction_InvalidCheckpointData(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, orchestrator)

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(actionsReceivedCheckpointKey), []byte("some_bad_data"))
	})
	require.NoError(t, err)

	transfers := []tokens.TokenTransfer{
		{
			From:             constants.NULL_ADDRESS,
			To:               "0xUserABC",
			Contract:         "0xNFT",
			TokenID:          "999",
			Amount:           1,
			BlockNumber:      10,
			TransactionIndex: 0,
			LogIndex:         0,
			TxHash:           "0xNormalTx",
			Type:             tokens.MINT,
		},
	}

	batch := tokens.TokenTransferBatch{
		Transfers:   transfers,
		BlockNumber: 10,
	}
	err = orchestrator.Handle(batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid checkpoint format")
}

func TestDefaultTdhTransfersReceivedAction_MultipleBatches(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, orchestrator)

	var transfers []tokens.TokenTransfer
	for i := 0; i < 250; i++ {
		tfr := tokens.TokenTransfer{
			From:             constants.NULL_ADDRESS,
			To:               fmt.Sprintf("0xUser%d", i),
			Contract:         "0xNFT",
			TokenID:          "BATCH",
			Amount:           1,
			BlockNumber:      1,
			TransactionIndex: 0,
			LogIndex:         uint64(i),
			TxHash:           fmt.Sprintf("0xTx%d", i),
			Type:             tokens.MINT,
		}
		transfers = append(transfers, tfr)
	}

	transfersBatch := tokens.TokenTransferBatch{
		Transfers:   transfers,
		BlockNumber: 1,
	}
	progressTracker.On("SetProgress", uint64(1)).Return(nil)
	err := orchestrator.Handle(transfersBatch)
	require.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		item, err2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, err2)
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "1:0:249", string(val))
		return nil
	})
	require.NoError(t, err)
}

func TestNoCheckpointKeyFound(t *testing.T) {
	db := setupTestInMemoryDB(t)
	err := db.View(func(txn *badger.Txn) error {
		val, err := getLastSavedCheckpoint(txn)
		require.NoError(t, err)
		assert.Equal(t, "0:0:0", string(val))
		return nil
	})
	require.NoError(t, err)
}

func TestSingleMint(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, orchestrator)

	transfer := tokens.TokenTransfer{
		From:             constants.NULL_ADDRESS,
		To:               "0xMinter",
		Contract:         "0xNFT",
		TokenID:          "123",
		Amount:           3,
		BlockNumber:      10,
		TransactionIndex: 1,
		LogIndex:         0,
		TxHash:           "0xMint",
		Type:             tokens.MINT,
	}

	progressTracker.On("SetProgress", uint64(10)).Return(nil)
	batch := tokens.TokenTransferBatch{
		Transfers:   []tokens.TokenTransfer{transfer},
		BlockNumber: 10,
	}
	err := orchestrator.Handle(batch)
	require.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		ownerDb := NewOwnerDb()
		nftDb := NewNFTDb()

		bal, _ := ownerDb.GetBalance(txn, "0xMinter", "0xNFT", "123")
		assert.Equal(t, int64(3), bal)

		rec, err := nftDb.GetNFT(txn, "0xNFT", "123")
		require.NoError(t, err)
		assert.Equal(t, int64(3), rec.Supply)
		assert.Equal(t, int64(0), rec.BurntSupply)

		item, e2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, e2)
		cp, _ := item.ValueCopy(nil)
		assert.Equal(t, "10:1:0", string(cp))

		return nil
	})
	require.NoError(t, err)
}

func TestBurnToDeadAddress(t *testing.T) {
	db := setupTestInMemoryDB(t)
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(context.Background(), progressTracker, db)
	require.NotNil(t, orchestrator)

	mint := tokens.TokenTransfer{
		From:             constants.NULL_ADDRESS,
		To:               "0xUser",
		Contract:         "0xNFT",
		TokenID:          "999",
		Amount:           2,
		BlockNumber:      10,
		TransactionIndex: 0,
		LogIndex:         0,
		TxHash:           "0xMint",
		Type:             tokens.MINT,
	}
	burn := tokens.TokenTransfer{
		From:             "0xUser",
		To:               constants.DEAD_ADDRESS,
		Contract:         "0xNFT",
		TokenID:          "999",
		Amount:           2,
		BlockNumber:      11,
		TransactionIndex: 0,
		LogIndex:         1,
		TxHash:           "0xDeadBurn",
		Type:             tokens.BURN,
	}
	progressTracker.On("SetProgress", uint64(10)).Return(nil)

	batch := tokens.TokenTransferBatch{
		Transfers:   []tokens.TokenTransfer{mint, burn},
		BlockNumber: 10,
	}
	err := orchestrator.Handle(batch)
	require.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		balance, err := orchestrator.ownerDb.GetBalance(txn, "0xUser", "0xNFT", "999")
		require.NoError(t, err)
		assert.Equal(t, int64(0), balance)

		nft, err := orchestrator.nftDb.GetNFT(txn, "0xNFT", "999")
		require.NoError(t, err)
		assert.Equal(t, int64(2), nft.BurntSupply)

		return nil
	})
	require.NoError(t, err)
}

func TestParseCheckpointValid(t *testing.T) {
	block, txIndex, logIndex, err := parseCheckpoint("100:2:5")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), block)
	assert.Equal(t, uint64(2), txIndex)
	assert.Equal(t, uint64(5), logIndex)
}

func TestParseCheckpointInvalid(t *testing.T) {
	_, _, _, err := parseCheckpoint("invalid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid checkpoint format")
}

// ----------------------------------------------------------------
// 1) Mocks & helper stubs
//    (Adjusted to remove references to ResetOwners.)
// ----------------------------------------------------------------

type failingTransferDb struct {
	storeErr                          error
	getAllTransfersErr                error
	getTransfersContractErr           error
	getTransfersAddressErr            error
	getTransfersAfterCheckpointErr    error
	deleteTransfersAfterCheckpointErr error
}

func (f *failingTransferDb) StoreTransfer(*badger.Txn, tokens.TokenTransfer) error {
	if f.storeErr != nil {
		return f.storeErr
	}
	return nil
}
func (f *failingTransferDb) GetLatestTransfer(*badger.Txn) (*tokens.TokenTransfer, error) {
	return nil, nil
}
func (f *failingTransferDb) GetAllTransfers(*badger.Txn) ([]tokens.TokenTransfer, error) {
	if f.getAllTransfersErr != nil {
		return nil, f.getAllTransfersErr
	}
	return nil, nil
}
func (f *failingTransferDb) GetTransfersByContract(*badger.Txn, string) ([]tokens.TokenTransfer, error) {
	if f.getTransfersContractErr != nil {
		return nil, f.getTransfersContractErr
	}
	return nil, nil
}
func (f *failingTransferDb) GetTransfersByAddress(*badger.Txn, string) ([]tokens.TokenTransfer, error) {
	if f.getTransfersAddressErr != nil {
		return nil, f.getTransfersAddressErr
	}
	return nil, nil
}
func (f *failingTransferDb) GetTransfersAfterCheckpoint(*badger.Txn, uint64, uint64, uint64) ([]tokens.TokenTransfer, error) {
	if f.getTransfersAfterCheckpointErr != nil {
		return nil, f.getTransfersAfterCheckpointErr
	}
	return nil, nil
}
func (f *failingTransferDb) DeleteTransfersAfterCheckpoint(*badger.Txn, uint64, uint64, uint64) error {
	if f.deleteTransfersAfterCheckpointErr != nil {
		return f.deleteTransfersAfterCheckpointErr
	}
	return nil
}
func (f *failingTransferDb) GetTransfersByBlockMax(*badger.Txn, uint64) ([]tokens.TokenTransfer, error) {
	return nil, nil
}
func (f *failingTransferDb) GetTransfersByBlockNumber(*badger.Txn, uint64) ([]tokens.TokenTransfer, error) {
	return nil, nil
}
func (f *failingTransferDb) GetTransfersByNft(*badger.Txn, string, string) ([]tokens.TokenTransfer, error) {
	return nil, nil
}
func (f *failingTransferDb) GetTransfersByTxHash(*badger.Txn, string) ([]tokens.TokenTransfer, error) {
	return nil, nil
}

type failingOwnerDb struct {
	updateOwnershipErr error
}

func (f *failingOwnerDb) GetBalance(*badger.Txn, string, string, string) (int64, error) {
	return 0, nil
}
func (f *failingOwnerDb) UpdateOwnership(*badger.Txn, string, string, string, string, int64) error {
	if f.updateOwnershipErr != nil {
		return f.updateOwnershipErr
	}
	return nil
}
func (f *failingOwnerDb) UpdateOwnershipReverse(*badger.Txn, string, string, string, string, int64) error {
	if f.updateOwnershipErr != nil {
		return f.updateOwnershipErr
	}
	return nil
}
func (f *failingOwnerDb) GetAllOwners(*badger.Txn) (map[string]int64, error) {
	return nil, nil
}
func (f *failingOwnerDb) GetOwnersByNft(*badger.Txn, string, string) (map[string]int64, error) {
	return nil, nil
}

type failingNFTDb struct {
	updateSupplyErr      error
	updateBurntSupplyErr error
}

func (f *failingNFTDb) UpdateSupply(*badger.Txn, string, string, int64) error {
	if f.updateSupplyErr != nil {
		return f.updateSupplyErr
	}
	return nil
}
func (f *failingNFTDb) UpdateSupplyReverse(*badger.Txn, string, string, int64) error {
	if f.updateSupplyErr != nil {
		return f.updateSupplyErr
	}
	return nil
}
func (f *failingNFTDb) UpdateBurntSupply(*badger.Txn, string, string, int64) error {
	if f.updateBurntSupplyErr != nil {
		return f.updateBurntSupplyErr
	}
	return nil
}
func (f *failingNFTDb) UpdateBurntSupplyReverse(*badger.Txn, string, string, int64) error {
	if f.updateBurntSupplyErr != nil {
		return f.updateBurntSupplyErr
	}
	return nil
}
func (f *failingNFTDb) GetNFT(*badger.Txn, string, string) (*NFT, error) {
	return nil, nil
}
func (f *failingNFTDb) GetNftsByOwnerAddress(*badger.Txn, string) ([]NFT, error) {
	return nil, nil
}

func newActionWithMocks(
	db *badger.DB,
	transferDb TransferDb,
	ownerDb OwnerDb,
	nftDb NFTDb,
) *DefaultTdhTransfersReceivedAction {
	return &DefaultTdhTransfersReceivedAction{
		db:         db,
		transferDb: transferDb,
		ownerDb:    ownerDb,
		nftDb:      nftDb,
		ctx:        context.Background(),
	}
}

// ----------------------------------------------------------------
// 2) Tests specifically for reset(...) errors
// ----------------------------------------------------------------

// a) If GetTransfersAfterCheckpoint fails
func TestReset_GetTransfersAfterCheckpointError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	fTransDb := &failingTransferDb{
		getTransfersAfterCheckpointErr: errors.New("mock getTransfersAfterCheckpoint error"),
	}
	action := newActionWithMocks(db, fTransDb, NewOwnerDb(), NewNFTDb())

	err := action.reset(0, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mock getTransfersAfterCheckpoint error")
	assert.Contains(t, err.Error(), "failed to get transfers")
}

// b) If DeleteTransfersAfterCheckpoint fails
func TestReset_DeleteTransfersAfterCheckpointError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	fTransDb := &failingTransferDb{
		deleteTransfersAfterCheckpointErr: errors.New("mock deleteTransfers error"),
	}
	action := newActionWithMocks(db, fTransDb, NewOwnerDb(), NewNFTDb())

	err := action.reset(0, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mock deleteTransfers error")
	assert.Contains(t, err.Error(), "failed to delete transfers after checkpoint")
}

// ----------------------------------------------------------------
// 3) Tests for applyTransfer(...) error coverage
// ----------------------------------------------------------------

func TestApplyTransfer_StoreTransferError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	fTransDb := &failingTransferDb{
		storeErr: errors.New("mock store error"),
	}
	action := newActionWithMocks(db, fTransDb, NewOwnerDb(), NewNFTDb())

	txErr := db.Update(func(txn *badger.Txn) error {
		transfer := tokens.TokenTransfer{
			BlockNumber:      10,
			TransactionIndex: 0,
			LogIndex:         0,
			From:             constants.NULL_ADDRESS,
			To:               "0xUser",
			Contract:         "0xNFT",
			TokenID:          "1",
			Amount:           1,
			Type:             tokens.MINT,
		}
		return action.applyTransfer(txn, transfer, true)
	})
	require.Error(t, txErr)
	assert.Contains(t, txErr.Error(), "mock store error")
	assert.Contains(t, txErr.Error(), "failed to store transfer")
}

func TestApplyTransfer_UpdateSupplyError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	fNftDb := &failingNFTDb{
		updateSupplyErr: errors.New("mock updateSupply error"),
	}
	action := newActionWithMocks(db, NewTransferDb(), NewOwnerDb(), fNftDb)

	txErr := db.Update(func(txn *badger.Txn) error {
		transfer := tokens.TokenTransfer{
			From:     constants.NULL_ADDRESS,
			To:       "0xUser",
			Contract: "0xNFT",
			TokenID:  "1",
			Amount:   10,
			Type:     tokens.MINT,
		}
		return action.applyTransfer(txn, transfer, false)
	})
	require.Error(t, txErr)
	assert.Contains(t, txErr.Error(), "mock updateSupply error")
	assert.Contains(t, txErr.Error(), "failed to update NFT supply")
}

func TestApplyTransfer_UpdateBurntSupplyError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	fNftDb := &failingNFTDb{
		updateBurntSupplyErr: errors.New("mock burntSupply error"),
	}
	action := newActionWithMocks(db, NewTransferDb(), NewOwnerDb(), fNftDb)

	txErr := db.Update(func(txn *badger.Txn) error {
		transfer := tokens.TokenTransfer{
			From:     "0xUser",
			To:       constants.NULL_ADDRESS, // triggers burn
			Contract: "0xNFT",
			TokenID:  "1",
			Amount:   2,
			Type:     tokens.BURN,
		}
		return action.applyTransfer(txn, transfer, false)
	})
	require.Error(t, txErr)
	assert.Contains(t, txErr.Error(), "mock burntSupply error")
	assert.Contains(t, txErr.Error(), "failed to update NFT burnt supply")
}

func TestApplyTransfer_UpdateOwnershipError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	fOwnerDb := &failingOwnerDb{
		updateOwnershipErr: errors.New("mock updateOwnership error"),
	}
	action := newActionWithMocks(db, NewTransferDb(), fOwnerDb, NewNFTDb())

	txErr := db.Update(func(txn *badger.Txn) error {
		transfer := tokens.TokenTransfer{
			From:     "0xUser",
			To:       "0xUser2",
			Contract: "0xNFT",
			TokenID:  "1",
			Amount:   1,
			Type:     tokens.SEND,
		}
		return action.applyTransfer(txn, transfer, false)
	})
	require.Error(t, txErr)
	assert.Contains(t, txErr.Error(), "mock updateOwnership error")
	assert.Contains(t, txErr.Error(), "failed to update ownership")
}

// ----------------------------------------------------------------
// 4) Tests for applyTransferReverse(...) error coverage
// ----------------------------------------------------------------

func TestApplyTransferReverse_UpdateSupplyError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	fNftDb := &failingNFTDb{
		updateSupplyErr: errors.New("mock updateSupply error"),
	}
	action := newActionWithMocks(db, NewTransferDb(), NewOwnerDb(), fNftDb)

	txErr := db.Update(func(txn *badger.Txn) error {
		transfer := tokens.TokenTransfer{
			From:     "0xUser",
			To:       constants.NULL_ADDRESS, // triggers burn
			Contract: "0xNFT",
			TokenID:  "1",
			Amount:   2,
			Type:     tokens.MINT,
		}
		return action.applyTransferReverse(txn, transfer)
	})
	require.Error(t, txErr)
	assert.Contains(t, txErr.Error(), "mock updateSupply error")
	assert.Contains(t, txErr.Error(), "failed to revert minted supply")
}

func TestApplyTransferReverse_UpdateBurntSupplyError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	fNftDb := &failingNFTDb{
		updateBurntSupplyErr: errors.New("mock burntSupply error"),
	}
	action := newActionWithMocks(db, NewTransferDb(), NewOwnerDb(), fNftDb)

	txErr := db.Update(func(txn *badger.Txn) error {
		transfer := tokens.TokenTransfer{
			From:     "0xUser",
			To:       constants.NULL_ADDRESS, // triggers burn
			Contract: "0xNFT",
			TokenID:  "1",
			Amount:   2,
			Type:     tokens.BURN,
		}
		return action.applyTransferReverse(txn, transfer)
	})
	require.Error(t, txErr)
	assert.Contains(t, txErr.Error(), "mock burntSupply error")
	assert.Contains(t, txErr.Error(), "failed to revert burnt supply")
}

func TestApplyTransferReverse_UpdateOwnershipError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	fOwnerDb := &failingOwnerDb{
		updateOwnershipErr: errors.New("mock updateOwnership error"),
	}
	action := newActionWithMocks(db, NewTransferDb(), fOwnerDb, NewNFTDb())

	txErr := db.Update(func(txn *badger.Txn) error {
		transfer := tokens.TokenTransfer{
			From:     "0xUser",
			To:       "0xUser2",
			Contract: "0xNFT",
			TokenID:  "1",
			Amount:   1,
			Type:     tokens.SEND,
		}
		return action.applyTransferReverse(txn, transfer)
	})
	require.Error(t, txErr)
	assert.Contains(t, txErr.Error(), "mock updateOwnership error")
	assert.Contains(t, txErr.Error(), "failed to revert ownership")
}
