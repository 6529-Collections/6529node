package eth

import (
	"testing"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Reuse the same helper as in nft_db_test.go
// If your test files are in the same package, you can just call setupTestInMemoryDB(t).
// If they are in a different package, you can either make it public or duplicate it here.

func TestOwnerDb_ResetOwners(t *testing.T) {
	db := setupTestInMemoryDB(t)
	defer db.Close()

	ownerDb := NewOwnerDb()

	// Insert some ownership data
	err := db.Update(func(txn *badger.Txn) error {
		// We can directly call setBalance to create a record
		return ownerDb.(*OwnerDbImpl).setBalance(txn, "0xOwner1", "contractA", "token1", 50)
	})
	require.NoError(t, err)

	// Confirm it's in the DB
	err = db.View(func(txn *badger.Txn) error {
		balance, err := ownerDb.GetBalance(txn, "0xOwner1", "contractA", "token1")
		require.NoError(t, err)
		assert.Equal(t, int64(50), balance)
		return nil
	})
	require.NoError(t, err)

	// Reset
	err = ownerDb.ResetOwners(db)
	require.NoError(t, err)

	// Confirm ownership data is gone
	err = db.View(func(txn *badger.Txn) error {
		balance, err := ownerDb.GetBalance(txn, "0xOwner1", "contractA", "token1")
		require.NoError(t, err)
		assert.Equal(t, int64(0), balance)
		return nil
	})
	require.NoError(t, err)
}

func TestOwnerDb_UpdateOwnership(t *testing.T) {
	db := setupTestInMemoryDB(t)
	defer db.Close()

	ownerDb := NewOwnerDb()

	// Let's create an initial balance for "0xAlice"
	err := db.Update(func(txn *badger.Txn) error {
		// 0xAlice mints or is assigned 100 units
		// from = NULL_ADDRESS => this might be a "mint" scenario in your logic
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

		// Should see { "0xAlice" -> 50, "0xBob" -> 100 }
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
		if err := ownerDb.(*OwnerDbImpl).setBalance(txn, "0xAlice", "contractX", "tokenFoo", 10); err != nil {
			return err
		}
		if err := ownerDb.(*OwnerDbImpl).setBalance(txn, "0xBob", "contractX", "tokenFoo", 5); err != nil {
			return err
		}
		if err := ownerDb.(*OwnerDbImpl).setBalance(txn, "0xCarol", "contractY", "tokenBar", 20); err != nil {
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
