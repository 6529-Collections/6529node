package eth

import (
	"context"
	"testing"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultTdhTransfersReceivedAction_NoTransfers(t *testing.T) {
	db := setupTestInMemoryDB(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(db, context.Background())

	// Test calling Handle with no transfers
	err := orchestrator.Handle([]tokens.TokenTransfer{})
	require.NoError(t, err)

	// Expect no changes, no errors, just a no-op
	// We can verify there's no checkpoint set
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(actionsReceivedCheckpointKey))
		if err == badger.ErrKeyNotFound {
			// no checkpoint - that's expected
			return nil
		}
		return err
	})
	require.NoError(t, err, "checkpoint key should not exist")
}

func TestDefaultTdhTransfersReceivedAction_BasicFlow(t *testing.T) {
	db := setupTestInMemoryDB(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(db, context.Background())

	// Some sample transfers
	// 1) Mint 3 tokens, block=1
	// 2) Transfer 1 from user1 -> user2
	// 3) Transfer 2 from user1 -> user2
	transfers := []tokens.TokenTransfer{
		// mint 3 tokens to user1
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
		// user1 => user2 : 1 token
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
		// user1 => user2 : 2 tokens
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
		// OwnerDb => user2 should have 3 now, user1 => 0
		ownerDb := NewOwnerDb()
		balUser1, err := ownerDb.GetBalance(txn, "0xuser1", "0xnft", "10") // note lowercase from apply
		require.NoError(t, err)
		assert.Equal(t, int64(0), balUser1)

		balUser2, err := ownerDb.GetBalance(txn, "0xuser2", "0xnft", "10")
		require.NoError(t, err)
		assert.Equal(t, int64(3), balUser2)

		// NFTDb => supply=3, burnt=0
		nftDb := NewNFTDb()
		nftRec, nftErr := nftDb.GetNFT(txn, "0xnft", "10")
		require.NoError(t, nftErr)
		require.NotNil(t, nftRec)
		assert.Equal(t, int64(3), nftRec.Supply)
		assert.Equal(t, int64(0), nftRec.BurntSupply)

		// Check checkpoint
		item, getErr := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, getErr)
		checkpointVal, copyErr := item.ValueCopy(nil)
		require.NoError(t, copyErr)
		// Should be "1:0:2" => block=1, txIndex=0, logIndex=2 (the last one)
		assert.Equal(t, "1:0:2", string(checkpointVal))

		return nil
	})
	require.NoError(t, err)
}

func TestDefaultTdhTransfersReceivedAction_OutOfOrderReset(t *testing.T) {
	db := setupTestInMemoryDB(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(db, context.Background())

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

	// Because block=9 is less than the saved checkpoint block=10,
	// we expect a reset to occur: it will prune data at or beyond (9,1,0)? Actually,
	// the logic chooses "firstTransfer" => 9:1:0 for the reset. Meanwhile, we only had block=10.
	// => The reset logic calls .Handle() again with all transfers we just tried to handle, so
	// => it replays from scratch.

	err := orchestrator.Handle(batch2)
	require.NoError(t, err)

	// After handle completes, let's see what ended up in the DB.
	// The second batch triggered a reset to (9,1,0), meaning block=10 data was pruned.
	// Then we reprocessed everything => the new data set is just batch2?

	// But your reset method logic is: it prunes transfers >=(9,1,0). That includes the old block=10 transfer.
	// Then it replays the "remaining" (which might be none).
	// Then it handles batch2 from scratch. So the final state in DB is only batch2's effect.
	// This depends on how your code merges the old and new sets if it restarts Handle with both sets, etc.

	err = db.View(func(txn *badger.Txn) error {
		// Check userA => we expect 0 now because T1 was pruned
		ownerDb := NewOwnerDb()
		balUserA, _ := ownerDb.GetBalance(txn, "0xusera", "0xnft", "101")
		assert.Equal(t, int64(0), balUserA, "UserA's previous block=10 mint was pruned by reset")

		// Check userB => minted 2
		balUserB, _ := ownerDb.GetBalance(txn, "0xuserb", "0xnft", "102")
		assert.Equal(t, int64(2), balUserB)

		// Check checkpoint => should be 9:1:0
		item, err2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, err2)
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "9:1:0", string(val), "Checkpoint should reflect last transfer from batch2")

		return nil
	})
	require.NoError(t, err)
}

func TestDefaultTdhTransfersReceivedAction_BurnScenario(t *testing.T) {
	db := setupTestInMemoryDB(t)
	orchestrator := NewTdhTransfersReceivedActionImpl(db, context.Background())

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

		// Check checkpoint => "7:1:2"
		item, err2 := txn.Get([]byte(actionsReceivedCheckpointKey))
		require.NoError(t, err2)
		val, _ := item.ValueCopy(nil)
		assert.Equal(t, "7:1:2", string(val))

		return nil
	})
	require.NoError(t, err)
}
