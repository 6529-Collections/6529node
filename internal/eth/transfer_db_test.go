package eth

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransferDb_StoreAndGetAllTransfers(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// Create a sample transfer
	sample := tokens.TokenTransfer{
		From:             "0xFrom",
		To:               "0xTo",
		Contract:         "0xContract",
		TokenID:          "123",
		TxHash:           "0xTxA",
		BlockNumber:      100,
		TransactionIndex: 0,
		LogIndex:         2,
		Amount:           1,
	}

	// Insert in a write transaction
	err := db.Update(func(txn *badger.Txn) error {
		return transferDb.StoreTransfer(txn, sample)
	})
	require.NoError(t, err)

	// Retrieve all transfers
	var all []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		all, e = transferDb.GetAllTransfers(txn)
		return e
	})
	require.NoError(t, err)
	assert.Len(t, all, 1)

	got := all[0]
	assert.True(t, strings.EqualFold(sample.From, got.From))
	assert.True(t, strings.EqualFold(sample.To, got.To))
	assert.True(t, strings.EqualFold(sample.Contract, got.Contract))
	assert.True(t, strings.EqualFold(sample.TxHash, got.TxHash))

	assert.Equal(t, sample.TokenID, got.TokenID)
	assert.Equal(t, sample.BlockNumber, got.BlockNumber)
	assert.Equal(t, sample.TransactionIndex, got.TransactionIndex)
	assert.Equal(t, sample.LogIndex, got.LogIndex)
	assert.Equal(t, sample.Amount, got.Amount)
}

func TestTransferDb_GetTransfersByTxHash(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	transfers := []tokens.TokenTransfer{
		{
			TxHash:           "0xTxA",
			BlockNumber:      1,
			TransactionIndex: 0,
			LogIndex:         0,
			Contract:         "0xC",
			TokenID:          "100",
			From:             "0xF",
			To:               "0xG",
		},
		{
			TxHash:           "0xTxA",
			BlockNumber:      1,
			TransactionIndex: 0,
			LogIndex:         1,
			Contract:         "0xC",
			TokenID:          "101",
			From:             "0xF2",
			To:               "0xG2",
		},
		{
			TxHash:           "0xTxB",
			BlockNumber:      2,
			TransactionIndex: 0,
			LogIndex:         0,
			Contract:         "0xC2",
			TokenID:          "999",
			From:             "0xA",
			To:               "0xB",
		},
	}

	// Insert
	err := db.Update(func(txn *badger.Txn) error {
		for _, tr := range transfers {
			if e := transferDb.StoreTransfer(txn, tr); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Query by TxHash "0xTxA"
	var txA []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		txA, e = transferDb.GetTransfersByTxHash(txn, "0xTxA")
		return e
	})
	require.NoError(t, err)
	assert.Len(t, txA, 2)

	// Query by "0xTxB"
	var txB []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		txB, _ = transferDb.GetTransfersByTxHash(txn, "0xTxB")
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, txB, 1)

	// Query unknown txhash => empty
	var txUnknown []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		txUnknown, _ = transferDb.GetTransfersByTxHash(txn, "0xNope")
		return nil
	})
	require.NoError(t, err)
	assert.Empty(t, txUnknown)
}

func TestTransferDb_GetTransfersByBlockNumber(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// Insert some with different block numbers
	data := []tokens.TokenTransfer{
		{BlockNumber: 10, TransactionIndex: 1, LogIndex: 0, TxHash: "0xA"},
		{BlockNumber: 10, TransactionIndex: 1, LogIndex: 1, TxHash: "0xB"},
		{BlockNumber: 11, TransactionIndex: 0, LogIndex: 0, TxHash: "0xC"},
	}
	err := db.Update(func(txn *badger.Txn) error {
		for _, d := range data {
			if e := transferDb.StoreTransfer(txn, d); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Query block 10
	var block10 []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		block10, e = transferDb.GetTransfersByBlockNumber(txn, 10)
		return e
	})
	require.NoError(t, err)
	assert.Len(t, block10, 2)

	// Query block 11
	var block11 []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		block11, _ = transferDb.GetTransfersByBlockNumber(txn, 11)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, block11, 1)

	// Query non-existent block
	var block99 []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		block99, _ = transferDb.GetTransfersByBlockNumber(txn, 99)
		return nil
	})
	require.NoError(t, err)
	assert.Empty(t, block99)
}

func TestTransferDb_GetTransfersByNft(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// Insert some data
	items := []tokens.TokenTransfer{
		{
			Contract:         "0xNFT_A",
			TokenID:          "10",
			BlockNumber:      5,
			TransactionIndex: 0,
			LogIndex:         0,
			TxHash:           "0xTx1",
		},
		{
			Contract:         "0xNFT_A",
			TokenID:          "10",
			BlockNumber:      5,
			TransactionIndex: 0,
			LogIndex:         1,
			TxHash:           "0xTx2",
		},
		{
			Contract:         "0xNFT_B",
			TokenID:          "999",
			BlockNumber:      6,
			TransactionIndex: 2,
			LogIndex:         3,
			TxHash:           "0xTx3",
		},
	}

	err := db.Update(func(txn *badger.Txn) error {
		for _, it := range items {
			if e := transferDb.StoreTransfer(txn, it); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Query NFT_A / 10
	var nftA10 []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		nftA10, e = transferDb.GetTransfersByNft(txn, "0xNFT_A", "10")
		return e
	})
	require.NoError(t, err)
	assert.Len(t, nftA10, 2)

	// Query NFT_B / 999
	var nftB999 []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		nftB999, _ = transferDb.GetTransfersByNft(txn, "0xNFT_B", "999")
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, nftB999, 1)

	// Query unknown NFT => empty
	var emptyRes []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		emptyRes, _ = transferDb.GetTransfersByNft(txn, "0xNFT_Unknown", "7")
		return nil
	})
	require.NoError(t, err)
	assert.Empty(t, emptyRes)
}

func TestTransferDb_GetTransfersByAddress(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// Insert with different from/to addresses
	data := []tokens.TokenTransfer{
		{From: "0xAlice", To: "0xBob", TxHash: "0xA", BlockNumber: 1, TransactionIndex: 0, LogIndex: 0},
		{From: "0xBob", To: "0xCarol", TxHash: "0xB", BlockNumber: 2, TransactionIndex: 1, LogIndex: 1},
		{From: "0xBob", To: "0xBob", TxHash: "0xC", BlockNumber: 2, TransactionIndex: 2, LogIndex: 3},
	}

	err := db.Update(func(txn *badger.Txn) error {
		for _, d := range data {
			if e := transferDb.StoreTransfer(txn, d); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Check "0xAlice"
	var alice []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		alice, e = transferDb.GetTransfersByAddress(txn, "0xAlice")
		return e
	})
	require.NoError(t, err)
	// She appears in only one transfer (as `from`)
	assert.Len(t, alice, 1)

	// Check "0xBob"
	var bob []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		bob, _ = transferDb.GetTransfersByAddress(txn, "0xBob")
		return nil
	})
	require.NoError(t, err)
	// Bob is `to` in first, `from` in second, and from+to in third => total 3 references
	assert.Len(t, bob, 3)
}

func TestTransferDb_ResetToCheckpoint(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// We'll store multiple transfers across different (blockNumber, txIndex, logIndex).
	data := []tokens.TokenTransfer{
		{BlockNumber: 1, TransactionIndex: 0, LogIndex: 0, TxHash: "0xT1", From: "0xA", To: "0xB"},
		{BlockNumber: 1, TransactionIndex: 0, LogIndex: 1, TxHash: "0xT2", From: "0xC", To: "0xD"},
		{BlockNumber: 1, TransactionIndex: 1, LogIndex: 0, TxHash: "0xT3", From: "0xE", To: "0xF"},
		{BlockNumber: 2, TransactionIndex: 0, LogIndex: 0, TxHash: "0xT4", From: "0xG", To: "0xH"},
		{BlockNumber: 2, TransactionIndex: 1, LogIndex: 5, TxHash: "0xT5", From: "0xI", To: "0xJ"},
	}

	err := db.Update(func(txn *badger.Txn) error {
		for _, tr := range data {
			if e := transferDb.StoreTransfer(txn, tr); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Now let's pick a checkpoint => blockNumber=1, txIndex=1, logIndex=0
	// Everything at or beyond that is pruned.
	err = db.Update(func(txn *badger.Txn) error {
		return transferDb.ResetToCheckpoint(txn, 1, 1, 0)
	})
	require.NoError(t, err)

	// We expect only the first two transfers to remain:
	//   (1,0,0) => T1
	//   (1,0,1) => T2
	//
	// The ones at (1,1,0) and block=2 are pruned.

	// Check all transfers left
	var remaining []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		remaining, e = transferDb.GetAllTransfers(txn)
		return e
	})
	require.NoError(t, err)
	assert.Len(t, remaining, 2)

	// Check they are T1 & T2
	remainingHashes := []string{remaining[0].TxHash, remaining[1].TxHash}
	assert.Contains(t, remainingHashes, "0xt1")
	assert.Contains(t, remainingHashes, "0xt2")

	// Double-check via txHash index or address index
	err = db.View(func(txn *badger.Txn) error {
		t3Res, _ := transferDb.GetTransfersByTxHash(txn, "0xT3")
		assert.Empty(t, t3Res, "Expected T3 to be pruned")
		return nil
	})
	require.NoError(t, err)
}

func TestTransferDb_GetTransfersByContract(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// Insert multiple transfers across different contracts & token IDs
	data := []tokens.TokenTransfer{
		{
			Contract:         "0xContractA",
			TokenID:          "1",
			BlockNumber:      5,
			TransactionIndex: 0,
			LogIndex:         0,
			TxHash:           "0xTxAA",
		},
		{
			Contract:         "0xContractA",
			TokenID:          "2",
			BlockNumber:      6,
			TransactionIndex: 1,
			LogIndex:         1,
			TxHash:           "0xTxAB",
		},
		{
			Contract:         "0xContractB",
			TokenID:          "10",
			BlockNumber:      7,
			TransactionIndex: 0,
			LogIndex:         0,
			TxHash:           "0xTxBA",
		},
		{
			Contract:         "0xContractB",
			TokenID:          "11",
			BlockNumber:      8,
			TransactionIndex: 1,
			LogIndex:         2,
			TxHash:           "0xTxBB",
		},
	}

	// Insert them into DB
	err := db.Update(func(txn *badger.Txn) error {
		for _, tr := range data {
			if e := transferDb.StoreTransfer(txn, tr); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Query "0xContractA"
	var contractA []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		contractA, e = transferDb.GetTransfersByContract(txn, "0xContractA")
		return e
	})
	require.NoError(t, err)
	// We expect the first two items
	assert.Len(t, contractA, 2)
	assert.True(t, strings.EqualFold("0xContractA", contractA[0].Contract))
	assert.True(t, strings.EqualFold("0xContractA", contractA[1].Contract))

	// Query "0xContractB"
	var contractB []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		contractB, e = transferDb.GetTransfersByContract(txn, "0xContractB")
		return e
	})
	require.NoError(t, err)
	assert.Len(t, contractB, 2)
	assert.True(t, strings.EqualFold("0xContractB", contractB[0].Contract))
	assert.True(t, strings.EqualFold("0xContractB", contractB[1].Contract))

	// Query unknown contract
	var none []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		none, e = transferDb.GetTransfersByContract(txn, "0xUnknown")
		return e
	})
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestTransferDb_GetTransfersByBlockMax(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// Insert some transfers at different block numbers
	data := []tokens.TokenTransfer{
		{BlockNumber: 5, TransactionIndex: 0, LogIndex: 0, TxHash: "0xTxBlock5"},
		{BlockNumber: 6, TransactionIndex: 1, LogIndex: 1, TxHash: "0xTxBlock6"},
		{BlockNumber: 6, TransactionIndex: 2, LogIndex: 2, TxHash: "0xTxBlock6_2"},
		{BlockNumber: 10, TransactionIndex: 3, LogIndex: 0, TxHash: "0xTxBlock10"},
		{BlockNumber: 12, TransactionIndex: 0, LogIndex: 1, TxHash: "0xTxBlock12"},
	}

	err := db.Update(func(txn *badger.Txn) error {
		for _, tr := range data {
			if e := transferDb.StoreTransfer(txn, tr); e != nil {
				return e
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Query up to block 6 => should get block 5 and block 6 items
	var upTo6 []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		upTo6, e = transferDb.GetTransfersByBlockMax(txn, 6)
		return e
	})
	require.NoError(t, err)
	assert.Len(t, upTo6, 3, "Expected block#5 and block#6 (2 from block6)")

	// Query up to block 10 => should get block 5,6,10
	var upTo10 []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		upTo10, e = transferDb.GetTransfersByBlockMax(txn, 10)
		return e
	})
	require.NoError(t, err)
	assert.Len(t, upTo10, 4, "Should get everything except block12")

	// Query up to block 4 => should be none
	var upTo4 []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		upTo4, e = transferDb.GetTransfersByBlockMax(txn, 4)
		return e
	})
	require.NoError(t, err)
	assert.Empty(t, upTo4, "No transfers at or below block 4")

	// Query up to block 20 => should get all
	var upTo20 []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		upTo20, e = transferDb.GetTransfersByBlockMax(txn, 20)
		return e
	})
	require.NoError(t, err)
	assert.Len(t, upTo20, len(data), "Should get all transfers in the DB")
}

func TestTransferDb_StoreTransfer_ReadOnlyTxn(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	tr := tokens.TokenTransfer{
		TxHash:      "0xTest",
		BlockNumber: 1,
	}

	// Use db.View instead of db.Update => read-only txn
	err := db.View(func(txn *badger.Txn) error {
		// Attempting to store should fail
		return transferDb.StoreTransfer(txn, tr)
	})

	require.Error(t, err, "Expected an error since we're in a read-only transaction")
	t.Logf("Got expected error: %v", err)
}

func TestTransferDb_StoreTransfer_CorruptedTxHashIndex(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// We intentionally corrupt the "tdh:txhash:0xCorrupted" key
	err := db.Update(func(txn *badger.Txn) error {
		txHashKey := []byte(txHashPrefix + strings.ToLower("0xCorrupted"))
		// Store something that's invalid JSON
		return txn.Set(txHashKey, []byte("this-is-not-json"))
	})
	require.NoError(t, err)

	// Now try to store a transfer that has TxHash = "0xCorrupted"
	tr := tokens.TokenTransfer{
		TxHash:      "0xCorrupted",
		BlockNumber: 10,
		From:        "0xFrom",
		To:          "0xTo",
		Contract:    "0xContract",
		TokenID:     "1",
	}
	err = db.Update(func(txn *badger.Txn) error {
		return transferDb.StoreTransfer(txn, tr)
	})
	require.Error(t, err, "We expect an unmarshal error when reading existingKeys")
	assert.Contains(t, err.Error(), "invalid character", "Should fail parsing corrupted JSON")
}

func TestTransferDb_GetAllTransfers_CorruptedData(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// Insert a valid transfer
	err := db.Update(func(txn *badger.Txn) error {
		return transferDb.StoreTransfer(txn, tokens.TokenTransfer{
			TxHash:      "0xValid",
			BlockNumber: 1,
			From:        "0xA", To: "0xB",
		})
	})
	require.NoError(t, err)

	// Insert a corrupted record directly (bypassing StoreTransfer)
	err = db.Update(func(txn *badger.Txn) error {
		corruptedKey := []byte("tdh:transfer:0000000002:00000:00000:0xBad:0xBadContract:123")
		// Value is not valid JSON
		return txn.Set(corruptedKey, []byte("not-json"))
	})
	require.NoError(t, err)

	// Now try to read all transfers => we expect an error
	err = db.View(func(txn *badger.Txn) error {
		_, gerr := transferDb.GetAllTransfers(txn)
		return gerr
	})
	require.Error(t, err, "Should fail on corrupted JSON record")
	assert.Contains(t, err.Error(), "invalid character", "Unmarshal error expected")
}

func TestTransferDb_ResetToCheckpoint_CorruptedPrimaryRecord(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// Insert a valid record at (block=5, tx=0, log=0)
	err := db.Update(func(txn *badger.Txn) error {
		return transferDb.StoreTransfer(txn, tokens.TokenTransfer{
			TxHash:      "0xGood",
			BlockNumber: 5,
			LogIndex:    0,
		})
	})
	require.NoError(t, err)

	// Insert a corrupted record in the same or higher block (e.g. block=5)
	err = db.Update(func(txn *badger.Txn) error {
		key := []byte("tdh:transfer:0000000005:00000:00001:0xBad:0xContract:1")
		return txn.Set(key, []byte(`bad-json`))
	})
	require.NoError(t, err)

	// Now ResetToCheckpoint at block=5 => tries to prune or read block=5 transfers
	// => we should hit unmarshal error
	err = db.Update(func(txn *badger.Txn) error {
		return transferDb.ResetToCheckpoint(txn, 5, 0, 0)
	})
	require.Error(t, err, "Should fail due to corrupted record in the iteration")
	assert.Contains(t, err.Error(), "failed to unmarshal transfer")
}

func TestTransferDb_removePrimaryKeyFromList_SingleItem(t *testing.T) {
	db := setupTestInMemoryDB(t)

	err := db.Update(func(txn *badger.Txn) error {
		txHashKey := []byte(txHashPrefix + "0xSingle")
		// It has exactly one primaryKey in its list
		arr := []string{"tdh:transfer:0000000001:00000:00000:0xSingle:0xC:1"}
		raw, _ := json.Marshal(arr)
		if e := txn.Set(txHashKey, raw); e != nil {
			return e
		}

		// Now remove that primaryKey
		return removePrimaryKeyFromList(txn, "tdh:txhash:0xSingle", arr[0])
	})
	require.NoError(t, err)

	// Confirm the entire key was deleted
	err = db.View(func(txn *badger.Txn) error {
		_, gerr := txn.Get([]byte(txHashPrefix + "0xSingle"))
		assert.Equal(t, badger.ErrKeyNotFound, gerr)
		return nil
	})
	require.NoError(t, err)
}

func TestTransferDb_GetTransfersByBlockMax_CorruptedBlockNumber(t *testing.T) {
	db := setupTestInMemoryDB(t)
	transferDb := NewTransferDb()

	// Insert a normal record
	err := db.Update(func(txn *badger.Txn) error {
		return transferDb.StoreTransfer(txn, tokens.TokenTransfer{
			BlockNumber: 5,
			TxHash:      "0xGood",
		})
	})
	require.NoError(t, err)

	// Insert a bad key
	err = db.Update(func(txn *badger.Txn) error {
		key := []byte("tdh:transfer:badblocknum:00000:00000:0xBad:0xContract:1")
		val := []byte(`{"blockNumber":999}`)
		return txn.Set(key, val)
	})
	require.NoError(t, err)

	// Now call GetTransfersByBlockMax => should fail when it sees the "badblocknum" key
	var results []tokens.TokenTransfer
	err = db.View(func(txn *badger.Txn) error {
		var e error
		results, e = transferDb.GetTransfersByBlockMax(txn, 10)
		return e
	})
	require.Error(t, err, "Should fail on invalid block substring")
	assert.Contains(t, err.Error(), "ParseUint")
	assert.Empty(t, results, "No valid results expected because an error is thrown early")
}
