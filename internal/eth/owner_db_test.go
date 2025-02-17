package eth

import (
	"fmt"
	"testing"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOwnerDb_UpdateOwnership(t *testing.T) {
	db := setupTestInMemoryDB(t)
	defer db.Close()

	ownerDb := NewOwnerDb()

	// Let's create an initial balance for "0xAlice"
	err := db.Update(func(txn *badger.Txn) error {
		// 0xAlice mints 100 units (from null => alice)
		return ownerDb.UpdateOwnership(txn, constants.NULL_ADDRESS, "0xAlice", "contractX", "token123", 100)
	})
	require.NoError(t, err)

	// Check Alice's balance
	err = db.View(func(txn *badger.Txn) error {
		balance, err := ownerDb.GetBalance(txn, "0xAlice", "contractX", "token123")
		require.NoError(t, err)
		assert.Equal(t, int64(100), balance)
		return nil
	})
	require.NoError(t, err)

	// Transfer some from Alice to Bob
	err = db.Update(func(txn *badger.Txn) error {
		return ownerDb.UpdateOwnership(txn, "0xAlice", "0xBob", "contractX", "token123", 40)
	})
	require.NoError(t, err)

	// Confirm balances
	err = db.View(func(txn *badger.Txn) error {
		aliceBal, err := ownerDb.GetBalance(txn, "0xAlice", "contractX", "token123")
		require.NoError(t, err)
		bobBal, err := ownerDb.GetBalance(txn, "0xBob", "contractX", "token123")
		require.NoError(t, err)

		assert.Equal(t, int64(60), aliceBal)
		assert.Equal(t, int64(40), bobBal)
		return nil
	})
	require.NoError(t, err)

	// Attempt an overdraw
	err = db.Update(func(txn *badger.Txn) error {
		// Alice tries to transfer more than she has
		return ownerDb.UpdateOwnership(txn, "0xAlice", "0xBob", "contractX", "token123", 1000)
	})
	assert.Error(t, err, "Should fail with 'transfer error: insufficient balance'")

	// Confirm balances remain unchanged
	err = db.View(func(txn *badger.Txn) error {
		aliceBal, err := ownerDb.GetBalance(txn, "0xAlice", "contractX", "token123")
		require.NoError(t, err)
		bobBal, err := ownerDb.GetBalance(txn, "0xBob", "contractX", "token123")
		require.NoError(t, err)

		assert.Equal(t, int64(60), aliceBal)
		assert.Equal(t, int64(40), bobBal)
		return nil
	})
	require.NoError(t, err)

	// Transfer all from Alice to Bob => triggers key deletion for Alice
	err = db.Update(func(txn *badger.Txn) error {
		return ownerDb.UpdateOwnership(txn, "0xAlice", "0xBob", "contractX", "token123", 60)
	})
	require.NoError(t, err)

	// Confirm Alice's record is gone (balance=0) and Bob is 100
	err = db.View(func(txn *badger.Txn) error {
		aliceBal, err := ownerDb.GetBalance(txn, "0xAlice", "contractX", "token123")
		require.NoError(t, err)
		bobBal, err := ownerDb.GetBalance(txn, "0xBob", "contractX", "token123")
		require.NoError(t, err)

		assert.Equal(t, int64(0), aliceBal)
		assert.Equal(t, int64(100), bobBal)
		return nil
	})
	require.NoError(t, err)
}

// NEW test: reversing a previous transfer from->to.
func TestOwnerDb_UpdateOwnershipReverse(t *testing.T) {
	db := setupTestInMemoryDB(t)
	defer db.Close()
	ownerDb := NewOwnerDb()

	// 1) Mint 100 to Alice (null->alice)
	err := db.Update(func(txn *badger.Txn) error {
		return ownerDb.UpdateOwnership(txn, constants.NULL_ADDRESS, "0xAlice", "contractX", "token123", 100)
	})
	require.NoError(t, err)

	// 2) Forward transfer 40 from Alice->Bob
	err = db.Update(func(txn *badger.Txn) error {
		return ownerDb.UpdateOwnership(txn, "0xAlice", "0xBob", "contractX", "token123", 40)
	})
	require.NoError(t, err)

	// Check balances: Alice=60, Bob=40
	err = db.View(func(txn *badger.Txn) error {
		aliceBal, _ := ownerDb.GetBalance(txn, "0xAlice", "contractX", "token123")
		bobBal, _ := ownerDb.GetBalance(txn, "0xBob", "contractX", "token123")
		assert.Equal(t, int64(60), aliceBal)
		assert.Equal(t, int64(40), bobBal)
		return nil
	})
	require.NoError(t, err)

	// 3) Reverse that same transfer (Alice->Bob, 40).  Means Bob->Alice, 40
	err = db.Update(func(txn *badger.Txn) error {
		return ownerDb.UpdateOwnershipReverse(txn, "0xAlice", "0xBob", "contractX", "token123", 40)
	})
	require.NoError(t, err)

	// Now balances should be: Alice=100, Bob=0
	err = db.View(func(txn *badger.Txn) error {
		aliceBal, _ := ownerDb.GetBalance(txn, "0xAlice", "contractX", "token123")
		bobBal, _ := ownerDb.GetBalance(txn, "0xBob", "contractX", "token123")
		assert.Equal(t, int64(100), aliceBal)
		assert.Equal(t, int64(0), bobBal)
		return nil
	})
	require.NoError(t, err)

	// 4) Try reversing a transfer that is "bigger" than Bob's actual balance
	//    i.e. reverse from->to= (Alice->Bob, 99) but Bob doesn't have 99 now
	err = db.Update(func(txn *badger.Txn) error {
		return ownerDb.UpdateOwnershipReverse(txn, "0xAlice", "0xBob", "contractX", "token123", 99)
	})
	assert.Error(t, err, "reverse transfer error: insufficient balance at 'to' address")

	// Balances remain unchanged
	err = db.View(func(txn *badger.Txn) error {
		aliceBal, _ := ownerDb.GetBalance(txn, "0xAlice", "contractX", "token123")
		bobBal, _ := ownerDb.GetBalance(txn, "0xBob", "contractX", "token123")
		assert.Equal(t, int64(100), aliceBal)
		assert.Equal(t, int64(0), bobBal)
		return nil
	})
	require.NoError(t, err)
}

func TestOwnerDb_GetBalance(t *testing.T) {
	db := setupTestInMemoryDB(t)
	defer db.Close()

	ownerDb := NewOwnerDb()

	// By default, a non-existent key should return 0
	err := db.View(func(txn *badger.Txn) error {
		balance, err := ownerDb.GetBalance(txn, "0xNobody", "noContract", "noToken")
		require.NoError(t, err)
		assert.Equal(t, int64(0), balance)
		return nil
	})
	require.NoError(t, err)

	// Create a balance
	err = db.Update(func(txn *badger.Txn) error {
		return ownerDb.UpdateOwnership(txn, constants.NULL_ADDRESS, "0xSomebody", "contractA", "token99", 123)
	})
	require.NoError(t, err)

	// Check the balance
	err = db.View(func(txn *badger.Txn) error {
		balance, err := ownerDb.GetBalance(txn, "0xSomebody", "contractA", "token99")
		require.NoError(t, err)
		assert.Equal(t, int64(123), balance)
		return nil
	})
	require.NoError(t, err)
}

func TestOwnerDb_GetOwnersByNft(t *testing.T) {
	db := setupTestInMemoryDB(t)
	defer db.Close()

	ownerDb := NewOwnerDb()

	// Insert some ownership
	err := db.Update(func(txn *badger.Txn) error {
		// Mint to Alice & Bob
		if err := ownerDb.UpdateOwnership(txn, constants.NULL_ADDRESS, "0xAlice", "contractX", "tokenFoo", 50); err != nil {
			return err
		}
		if err := ownerDb.UpdateOwnership(txn, constants.NULL_ADDRESS, "0xBob", "contractX", "tokenFoo", 100); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	// Now retrieve owners for that NFT
	err = db.View(func(txn *badger.Txn) error {
		owners, err := ownerDb.GetOwnersByNft(txn, "contractX", "tokenFoo")
		require.NoError(t, err)

		fmt.Println("owners", owners)

		// Should see { "0xalice" -> 50, "0xbob" -> 100 }
		assert.Equal(t, int64(50), owners["0xAlice"])
		assert.Equal(t, int64(100), owners["0xBob"])
		return nil
	})
	require.NoError(t, err)
}

func TestOwnerDb_GetAllOwners(t *testing.T) {
	db := setupTestInMemoryDB(t)
	defer db.Close()

	ownerDb := NewOwnerDb()

	// Insert some ownership
	err := db.Update(func(txn *badger.Txn) error {
		// Just do direct setBalance for variety
		impl := ownerDb.(*OwnerDbImpl)
		if err := impl.setBalance(txn, "0xAlice", "contractX", "tokenFoo", 10); err != nil {
			return err
		}
		if err := impl.setBalance(txn, "0xBob", "contractX", "tokenFoo", 5); err != nil {
			return err
		}
		if err := impl.setBalance(txn, "0xCarol", "contractY", "tokenBar", 20); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	// Now retrieve all owners
	err = db.View(func(txn *badger.Txn) error {
		all, err := ownerDb.GetAllOwners(txn)
		require.NoError(t, err)

		// Keys in `all` will be something like:
		//   "0xAlice:contractX:tokenFoo" -> 10
		//   "0xBob:contractX:tokenFoo"   -> 5
		//   "0xCarol:contractY:tokenBar" -> 20

		assert.Equal(t, int64(10), all["0xAlice:contractX:tokenFoo"])
		assert.Equal(t, int64(5), all["0xBob:contractX:tokenFoo"])
		assert.Equal(t, int64(20), all["0xCarol:contractY:tokenBar"])

		return nil
	})
	require.NoError(t, err)
}

func TestUpdateOwnership_GetBalanceError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	txn := db.NewTransaction(true)
	defer txn.Discard()

	ownerDb := &OwnerDbImpl{}
	err := ownerDb.UpdateOwnership(txn, "0xFrom", "0xTo", "0xContract", "1", 100)

	require.Error(t, err)
	require.Contains(t, err.Error(), "transfer error: insufficient balance")
}

func TestUpdateOwnership_DeleteKeyError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	txn := db.NewTransaction(true)
	defer txn.Discard()

	ownerDb := &OwnerDbImpl{}
	_ = ownerDb.setBalance(txn, "0xFrom", "0xContract", "1", 100) // Set initial balance

	// Close DB to force delete failure
	db.Close()
	err := ownerDb.UpdateOwnership(txn, "0xFrom", "0xTo", "0xContract", "1", 100)

	require.Error(t, err)
}

func TestUpdateOwnership_SetBalanceError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	txn := db.NewTransaction(true)
	defer txn.Discard()

	ownerDb := &OwnerDbImpl{}

	// Close DB to force Set error
	db.Close()
	err := ownerDb.UpdateOwnership(txn, "0xNull", "0xTo", "0xContract", "1", 100)

	require.Error(t, err)
}

func TestGetBalance_KeyRetrievalError(t *testing.T) {
	db := setupTestInMemoryDB(t)
	db.Close() // Force an error

	txn := db.NewTransaction(false)
	defer txn.Discard()

	ownerDb := &OwnerDbImpl{}
	_, err := ownerDb.GetBalance(txn, "0xOwner", "0xContract", "1")

	require.Error(t, err)
}

func TestGetBalance_UnmarshalError(t *testing.T) {
	db := setupTestInMemoryDB(t)

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("tdh:owner:0xOwner:0xContract:1"), []byte("invalid_json"))
	})
	require.NoError(t, err)

	txn := db.NewTransaction(false)
	defer txn.Discard()

	ownerDb := &OwnerDbImpl{}
	_, err = ownerDb.GetBalance(txn, "0xOwner", "0xContract", "1")

	require.Error(t, err)
}

func TestGetOwnersByNft_UnmarshalError(t *testing.T) {
	db := setupTestInMemoryDB(t)

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("tdh:nftowners:0xContract:1:0xOwner"), []byte("invalid_json"))
	})
	require.NoError(t, err)

	txn := db.NewTransaction(false)
	defer txn.Discard()

	ownerDb := &OwnerDbImpl{}
	_, err = ownerDb.GetOwnersByNft(txn, "0xContract", "1")

	require.Error(t, err)
}

func TestGetAllOwners_UnmarshalError(t *testing.T) {
	db := setupTestInMemoryDB(t)

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("tdh:owner:0xOwner:0xContract:1"), []byte("invalid_json"))
	})
	require.NoError(t, err)

	txn := db.NewTransaction(false)
	defer txn.Discard()

	ownerDb := &OwnerDbImpl{}
	_, err = ownerDb.GetAllOwners(txn)

	require.Error(t, err)
}
