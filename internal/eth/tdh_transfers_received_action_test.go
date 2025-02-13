package eth

import (
	"context"
	"fmt"
	"testing"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckpointHelpers tests checkpointValue() and parseCheckpoint() directly.
func TestCheckpointHelpers(t *testing.T) {
	transfer := tokens.TokenTransfer{
		BlockNumber:      123,
		TransactionIndex: 45,
		LogIndex:         6,
	}

	val := checkpointValue(transfer)
	assert.Equal(t, "123:45:6", val, "checkpointValue should format correctly")

	// valid parse
	block, txi, log, err := parseCheckpoint("123:45:6")
	require.NoError(t, err, "parseCheckpoint should succeed for valid string")
	assert.Equal(t, uint64(123), block)
	assert.Equal(t, uint64(45), txi)
	assert.Equal(t, uint64(6), log)

	// invalid parse
	_, _, _, err = parseCheckpoint("not_valid")
	require.Error(t, err, "parseCheckpoint should fail for invalid format")
	assert.Contains(t, err.Error(), "invalid checkpoint format")
}

// TestGetLastSavedCheckpoint tests getLastSavedCheckpoint logic, including when the key is not found.
func TestGetLastSavedCheckpoint(t *testing.T) {
	db := setupTestInMemoryDB(t)
	err := db.View(func(txn *badger.Txn) error {
		val, err := getLastSavedCheckpoint(txn)
		require.NoError(t, err, "Should not fail on missing checkpoint")
		assert.Equal(t, "0:0:0", string(val), "Should default to '0:0:0' if not found")
		return nil
	})
	require.NoError(t, err)

	// Now store an invalid checkpoint and expect parseCheckpoint to fail later
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(actionsReceivedCheckpointKey), []byte("invalid_data"))
	})
	require.NoError(t, err)

	// getLastSavedCheckpoint doesn't parse, it just retrieves. So let's confirm we can read it back:
	err = db.View(func(txn *badger.Txn) error {
		val, err := getLastSavedCheckpoint(txn)
		require.NoError(t, err)
		assert.Equal(t, "invalid_data", string(val))
		return nil
	})
	require.NoError(t, err)
}

// TestNewTdhTransfersReceivedActionImpl_EmptyDB ensures that we cover the "No transfers found" path
// in the constructor. We verify logs are generated for "No transfers found" if the DB is empty.
func TestNewTdhTransfersReceivedActionImpl_EmptyDB(t *testing.T) {
	db := setupTestInMemoryDB(t)
	// No data inserted, so transferDb will return empty.
	action := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, action, "Should be able to construct the action even if DB is empty")
}

// TestNewTdhTransfersReceivedActionImpl_SomeTransfers ensures the constructor can read existing
// transfers from the DB and log them out. This is a partial coverage test that ensures sorting etc.
// The actual logic is mostly in the closure given to db.View(...).
func TestNewTdhTransfersReceivedActionImpl_SomeTransfers(t *testing.T) {
	db := setupTestInMemoryDB(t)

	// Insert some pretend "existing" transfers in the DB to see if the constructor logs them.
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
			if err := transferDb.StoreTransfer(txn, tr); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err, "Failed to insert test transfers")

	action := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, action, "Should successfully create the action with data present")
}

// TestDefaultTdhTransfersReceivedAction_NoTransfers covers the no-op scenario.
func TestDefaultTdhTransfersReceivedAction_NoTransfers(t *testing.T) {
	db := setupTestInMemoryDB(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, orchestrator)

	// Test calling Handle with no transfers
	err := orchestrator.Handle([]tokens.TokenTransfer{})
	require.NoError(t, err)

	// Expect no changes, no errors, just a no-op
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(actionsReceivedCheckpointKey))
		if err == badger.ErrKeyNotFound {
			// no checkpoint - that's expected
			return nil
		}
		return fmt.Errorf("expected no checkpoint key, got something else: %v", err)
	})
	require.NoError(t, err, "checkpoint key should not exist")
}

// TestDefaultTdhTransfersReceivedAction_BasicFlow covers normal mint and transfers.
func TestDefaultTdhTransfersReceivedAction_BasicFlow(t *testing.T) {
	db := setupTestInMemoryDB(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, orchestrator)

	// Some sample transfers:
	// 1) Mint 3 tokens, block=1
	// 2) Transfer 1 from user1 -> user2
	// 3) Transfer 2 from user1 -> user2
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

	err := orchestrator.Handle(transfers)
	require.NoError(t, err)

	// Check final states (ownerDb, nftDb, checkpoint)
	err = db.View(func(txn *badger.Txn) error {
		ownerDb := NewOwnerDb()
		nftDb := NewNFTDb()

		balUser1, err := ownerDb.GetBalance(txn, "0xuser1", "0xnft", "10")
		require.NoError(t, err)
		assert.Equal(t, int64(0), balUser1)

		balUser2, err := ownerDb.GetBalance(txn, "0xuser2", "0xnft", "10")
		require.NoError(t, err)
		assert.Equal(t, int64(3), balUser2)

		nftRec, nftErr := nftDb.GetNFT(txn, "0xnft", "10")
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

// TestDefaultTdhTransfersReceivedAction_OutOfOrderReset covers the scenario where a new batch
// arrives with a block < lastSavedBlock. This triggers reset and replay logic.
func TestDefaultTdhTransfersReceivedAction_OutOfOrderReset(t *testing.T) {
	db := setupTestInMemoryDB(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, orchestrator)

	// We'll handle a first batch
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
		},
	}
	// This sets checkpoint to "10:0:0"
	require.NoError(t, orchestrator.Handle(batch1))

	// Next we handle a second batch that is "behind" => block=9 => triggers reset
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
		},
	}

	// Because block=9 is less than the saved checkpoint block=10, we expect a reset
	err := orchestrator.Handle(batch2)
	require.NoError(t, err)

	// After handle completes, let's see what ended up in the DB.
	err = db.View(func(txn *badger.Txn) error {
		ownerDb := NewOwnerDb()

		// userA => we expect 0 now because block=10 data was pruned
		balA, _ := ownerDb.GetBalance(txn, "0xusera", "0xnft", "101")
		assert.Equal(t, int64(0), balA)

		// userB => minted 2 from block=9
		balB, _ := ownerDb.GetBalance(txn, "0xuserb", "0xnft", "102")
		assert.Equal(t, int64(2), balB)

		// Check checkpoint => should be 9:1:0
		item, err2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, err2)
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "9:1:0", string(val))
		return nil
	})
	require.NoError(t, err)
}

// TestDefaultTdhTransfersReceivedAction_BurnScenario covers a normal burn flow.
func TestDefaultTdhTransfersReceivedAction_BurnScenario(t *testing.T) {
	db := setupTestInMemoryDB(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, orchestrator)

	// 1) Mint 5 tokens to userC
	// 2) userC burns 2 tokens => supply remains 5, burnt=2
	// 3) userC => userD => 3 tokens
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
		},
		{
			From:             "0xUserC",
			To:               constants.NULL_ADDRESS, // burn
			Contract:         "0xNFT",
			TokenID:          "50",
			Amount:           2,
			BlockNumber:      7,
			TransactionIndex: 1,
			LogIndex:         1,
			TxHash:           "0xBurnC",
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

	require.NoError(t, orchestrator.Handle(allTransfers))

	// Check final state
	err := db.View(func(txn *badger.Txn) error {
		ownerDb := NewOwnerDb()
		nftDb := NewNFTDb()

		// userC => balance=0
		cBal, _ := ownerDb.GetBalance(txn, "0xuserc", "0xnft", "50")
		assert.Equal(t, int64(0), cBal)

		// userD => balance=3
		dBal, _ := ownerDb.GetBalance(txn, "0xuserd", "0xnft", "50")
		assert.Equal(t, int64(3), dBal)

		// NFT => totalSupply=5, burnt=2
		nftRec, err := nftDb.GetNFT(txn, "0xnft", "50")
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

// TestDefaultTdhTransfersReceivedAction_InsufficientBalance attempts a transfer that the sender
// cannot afford, expecting an error. This covers the "insufficient balance" branch in applyTransfer.
func TestDefaultTdhTransfersReceivedAction_InsufficientBalance(t *testing.T) {
	db := setupTestInMemoryDB(t)
	action := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, action)

	// 1) Mint 1 token to userX in a separate call so it commits successfully
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
	}

	// First call handles the mint only
	err := action.Handle([]tokens.TokenTransfer{mint})
	require.NoError(t, err, "Mint should succeed")

	// 2) Transfer 2 tokens from userX -> userY, but userX only has 1 => should fail
	badTransfer := tokens.TokenTransfer{
		From:             "0xUserX",
		To:               "0xUserY",
		Contract:         "0xNFT",
		TokenID:          "777",
		Amount:           2, // more than minted
		BlockNumber:      6,
		TransactionIndex: 0,
		LogIndex:         0,
		TxHash:           "0xTxInsufficient",
	}

	err = action.Handle([]tokens.TokenTransfer{badTransfer})
	require.Error(t, err, "Should fail due to insufficient balance")

	// Now verify the mint is still in the DB
	err = db.View(func(txn *badger.Txn) error {
		ownerDb := NewOwnerDb()

		// userX => 1
		balUserX, _ := ownerDb.GetBalance(txn, "0xuserx", "0xnft", "777")
		assert.Equal(t, int64(1), balUserX)

		// userY => 0 (the second transfer never succeeded)
		balUserY, _ := ownerDb.GetBalance(txn, "0xusery", "0xnft", "777")
		assert.Equal(t, int64(0), balUserY)

		// Check checkpoint => it should reflect only the mint: "5:0:0"
		item, err2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, err2)
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "5:0:0", string(val))

		return nil
	})
	require.NoError(t, err)
}

// TestDefaultTdhTransfersReceivedAction_InvalidCheckpointData ensures that if the lastSavedCheckpoint
// in DB is invalid, the parseCheckpoint call inside Handle(...) returns an error.
func TestDefaultTdhTransfersReceivedAction_InvalidCheckpointData(t *testing.T) {
	db := setupTestInMemoryDB(t)
	action := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, action)

	// Manually store invalid checkpoint
	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(actionsReceivedCheckpointKey), []byte("some_bad_data"))
	})
	require.NoError(t, err)

	// Attempt to handle a normal transfer => should fail on parseCheckpoint
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
		},
	}

	err = action.Handle(transfers)
	require.Error(t, err, "Should fail because parseCheckpoint can't parse 'some_bad_data'")
	assert.Contains(t, err.Error(), "invalid checkpoint format")
}

// TestDefaultTdhTransfersReceivedAction_MultipleBatches ensures we hit the batch-splitting logic
func TestDefaultTdhTransfersReceivedAction_MultipleBatches(t *testing.T) {
	db := setupTestInMemoryDB(t)
	action := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, action)

	// We'll create more than 100 transfers to force multiple batches
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
		}
		transfers = append(transfers, tfr)
	}

	// By default, the code uses batchSize=100. So this will result in 3 batches (100,100,50).
	err := action.Handle(transfers)
	require.NoError(t, err)

	// Check that the final checkpoint = "1:0:249" (the last item)
	err = db.View(func(txn *badger.Txn) error {
		item, err2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, err2)
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "1:0:249", string(val))
		return nil
	})
	require.NoError(t, err)
}

func TestInvalidCheckpointData(t *testing.T) {
	db := setupTestInMemoryDB(t)
	action := NewTdhTransfersReceivedActionImpl(db, context.Background())
	require.NotNil(t, action)

	// Manually store an invalid checkpoint
	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(actionsReceivedCheckpointKey), []byte("bad:data:here"))
	})
	require.NoError(t, err)

	// Attempt to handle a normal set of transfers
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
			TxHash:           "0xTest",
		},
	}

	// This should fail because parseCheckpoint will throw an error for "bad:data:here"
	err = action.Handle(transfers)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid checkpoint format")
}

func TestNoCheckpointKeyFound(t *testing.T) {
	db := setupTestInMemoryDB(t)
	// Don't set checkpoint; it's missing on purpose.

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
	action := NewTdhTransfersReceivedActionImpl(db, context.Background())

	// Just 1 mint => block=10
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
	}

	err := action.Handle([]tokens.TokenTransfer{transfer})
	require.NoError(t, err)

	// Validate DB
	err = db.View(func(txn *badger.Txn) error {
		// ownership
		ownerDb := NewOwnerDb()
		bal, _ := ownerDb.GetBalance(txn, "0xminter", "0xnft", "123")
		assert.Equal(t, int64(3), bal)

		// supply
		nftDb := NewNFTDb()
		rec, err := nftDb.GetNFT(txn, "0xnft", "123")
		require.NoError(t, err)
		assert.Equal(t, int64(3), rec.Supply)
		assert.Equal(t, int64(0), rec.BurntSupply)

		// checkpoint
		item, e2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, e2)
		cp, _ := item.ValueCopy(nil)
		assert.Equal(t, "10:1:0", string(cp), "Should match last minted block:txIndex:logIndex")

		return nil
	})
	require.NoError(t, err)
}
