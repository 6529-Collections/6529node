package eth_test

import (
	"os"
	"testing"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/internal/eth"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Example mock usage (if you ever need it):
type MockExample struct {
    mock.Mock
}

func (m *MockExample) DoSomething(arg string) string {
    args := m.Called(arg)
    return args.String(0)
}

// Helper function to create an in-memory Badger DB
func newTestBadgerDB(t *testing.T) *badger.DB {
    // Setup in-memory BadgerDB
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	badgerDB, err := db.OpenBadger(tmpDir)
	require.NoError(t, err)

    // Ensure we close the DB when test is done
    t.Cleanup(func() {
        err := badgerDB.Close()
        require.NoError(t, err, "failed to close badger DB")
    })

    return badgerDB
}

func TestTransferDb(t *testing.T) {
    // Create an in-memory Badger DB
    db := newTestBadgerDB(t)

    // Initialize your TransferDb implementation
    transferDb := eth.NewTransferDb()

    // We'll store multiple test TokenTransfers
    transfer1 := tokens.TokenTransfer{
        BlockNumber:      100,
        TransactionIndex: 1,
        LogIndex:         0,
        TxHash:           "0xABC",
        From:             "0x111",
        To:               "0x222",
        Contract:         "0xCONTRACT",
        TokenID:          "1",
        Amount:           10,
    }
    transfer2 := tokens.TokenTransfer{
        BlockNumber:      100,
        TransactionIndex: 1,
        LogIndex:         1,
        TxHash:           "0xABC",
        From:             "0x111",
        To:               "0x333",
        Contract:         "0xCONTRACT",
        TokenID:          "2",
        Amount:           1,
    }
    transfer3 := tokens.TokenTransfer{
        BlockNumber:      101,
        TransactionIndex: 0,
        LogIndex:         0,
        TxHash:           "0xDEF",
        From:             "0x222",
        To:               "0x111",
        Contract:         "0xCONTRACT",
        TokenID:          "1",
        Amount:           5,
    }
    transfer4 := tokens.TokenTransfer{
        BlockNumber:      101,
        TransactionIndex: 2,
        LogIndex:         0,
        TxHash:           "0xXYZ",
        From:             "0x444",
        To:               "0x444", // same from & to
        Contract:         "0xOTHERCONTRACT",
        TokenID:          "777",
        Amount:           100,
    }

    // Store them in a single transaction
    err := db.Update(func(txn *badger.Txn) error {
        require.NoError(t, transferDb.StoreTransfer(txn, transfer1))
        require.NoError(t, transferDb.StoreTransfer(txn, transfer2))
        require.NoError(t, transferDb.StoreTransfer(txn, transfer3))
        require.NoError(t, transferDb.StoreTransfer(txn, transfer4))
        return nil
    })
    require.NoError(t, err, "failed to store transfers")

    t.Run("Test GetTransfersByBlockNumber", func(t *testing.T) {
        err := db.View(func(txn *badger.Txn) error {
            transfers100, err := transferDb.GetTransfersByBlockNumber(txn, 100)
            require.NoError(t, err)
            assert.Len(t, transfers100, 2, "there should be 2 transfers at block 100")

            transfers101, err := transferDb.GetTransfersByBlockNumber(txn, 101)
            require.NoError(t, err)
            assert.Len(t, transfers101, 2, "there should be 2 transfers at block 101")

            // Check an empty blockNumber
            transfers999, err := transferDb.GetTransfersByBlockNumber(txn, 999)
            require.NoError(t, err)
            assert.Empty(t, transfers999, "no transfers expected for block 999")

            return nil
        })
        require.NoError(t, err)
    })

    t.Run("Test GetTransfersByTxHash", func(t *testing.T) {
        err := db.View(func(txn *badger.Txn) error {
            transfersABC, err := transferDb.GetTransfersByTxHash(txn, "0xABC")
            require.NoError(t, err)
            assert.Len(t, transfersABC, 2, "should be 2 transfers with txHash 0xABC")

            // Check we get the right blockNumber or tokenIDs
            var hasTokenID1, hasTokenID2 bool
            for _, tr := range transfersABC {
                if tr.TokenID == "1" {
                    hasTokenID1 = true
                }
                if tr.TokenID == "2" {
                    hasTokenID2 = true
                }
            }
            assert.True(t, hasTokenID1)
            assert.True(t, hasTokenID2)

            // Another txHash
            transfersDEF, err := transferDb.GetTransfersByTxHash(txn, "0xDEF")
            require.NoError(t, err)
            assert.Len(t, transfersDEF, 1)

            // Non-existent txHash
            transfersNOPE, err := transferDb.GetTransfersByTxHash(txn, "0xNOPE")
            require.NoError(t, err)
            assert.Empty(t, transfersNOPE)

            return nil
        })
        require.NoError(t, err)
    })

    t.Run("Test GetTransfersByNft", func(t *testing.T) {
        err := db.View(func(txn *badger.Txn) error {
            // We'll test the contract "0xCONTRACT", tokenID "1"
            contract := "0xCONTRACT"
            tokenID := "1"

            trNft, err := transferDb.GetTransfersByNft(txn, contract, tokenID)
            require.NoError(t, err)
            // Should see 2 transfers for that NFT
            assert.Len(t, trNft, 2, "two transfers for 0xCONTRACT:1")

            // "0xCONTRACT":"2"
            trNft2, err := transferDb.GetTransfersByNft(txn, contract, "2")
            require.NoError(t, err)
            assert.Len(t, trNft2, 1)

            // "0xOTHERCONTRACT":"777" => 1 transfer
            trNft3, err := transferDb.GetTransfersByNft(txn, "0xOTHERCONTRACT", "777")
            require.NoError(t, err)
            assert.Len(t, trNft3, 1, "should have the self-transfer from 0x444 to 0x444")

            // Non-existent NFT
            trNope, err := transferDb.GetTransfersByNft(txn, "0xNOPE", "9999")
            require.NoError(t, err)
            assert.Empty(t, trNope)

            return nil
        })
        require.NoError(t, err)
    })

    t.Run("Test GetTransfersByAddress", func(t *testing.T) {
        err := db.View(func(txn *badger.Txn) error {
            // address 0x111 is 'from' for transfer1,2 and 'to' for transfer3
            // total 3 transfers. Let's see if we find them.
            tr111, err := transferDb.GetTransfersByAddress(txn, "0x111")
            require.NoError(t, err)
            assert.Len(t, tr111, 3, "0x111 is involved in 3 transfers")

            // address 0x444 is from & to in transfer4 => 1 transfer
            tr444, err := transferDb.GetTransfersByAddress(txn, "0x444")
            require.NoError(t, err)
            assert.Len(t, tr444, 1, "0x444 has exactly 1 self-transfer")

            // address 0x999 => none
            tr999, err := transferDb.GetTransfersByAddress(txn, "0x999")
            require.NoError(t, err)
            assert.Empty(t, tr999)

            return nil
        })
        require.NoError(t, err)
    })
}

func TestMockUsageExample(t *testing.T) {
    // Just a quick demonstration of how you might use testify/mock
    // in your code, though here it's not strictly necessary.
    mockObj := new(MockExample)
    mockObj.On("DoSomething", "hello").Return("world")

    result := mockObj.DoSomething("hello")
    assert.Equal(t, "world", result)
    mockObj.AssertExpectations(t)
}
