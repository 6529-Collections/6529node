package ethdb

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestNFTDb returns a fresh DB + an NFTDb impl + cleanup func
func setupTestNFTDb(t *testing.T) (*sql.DB, NFTDb, func()) {
	db, cleanup := testdb.SetupTestDB(t)
	nftDB := NewNFTDb()
	return db, nftDB, cleanup
}

func TestNFTDb_UpdateSupply_CreatesNewNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	supply, err := nftDB.UpdateSupply(tx, "0xContractA", "TokenA")
	require.NoError(t, err, "UpdateSupply should not fail on creating a new NFT")
	assert.Equal(t, uint64(1), supply, "Expected new NFT supply to be 1")

	require.NoError(t, tx.Commit())

	// Verify in DB
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractA", "TokenA")
	require.NoError(t, err, "GetNft should find newly created NFT")
	assert.Equal(t, uint64(1), nft.Supply, "Expected supply=1 after creation")
	assert.Equal(t, uint64(0), nft.BurntSupply, "Expected burnt_supply=0 after creation")
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateSupply_ExistingNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create an NFT
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractB", "TokenB")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Now increment supply again
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	supply, err := nftDB.UpdateSupply(tx, "0xContractB", "TokenB")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), supply)

	require.NoError(t, tx.Commit())

	// Confirm in DB
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractB", "TokenB")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), nft.Supply)
	assert.Equal(t, uint64(0), nft.BurntSupply)
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateBurntSupply_ExistingNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create NFT
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractC", "TokenC")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Burn
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	err = nftDB.UpdateBurntSupply(tx, "0xContractC", "TokenC")
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	// Check
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractC", "TokenC")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nft.Supply)
	assert.Equal(t, uint64(1), nft.BurntSupply)
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateBurntSupply_NonexistentNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	err = nftDB.UpdateBurntSupply(tx, "0xContractD", "TokenD")
	assert.Error(t, err, "Expected error when burning nonexistent NFT")

	_ = tx.Rollback()

	// Double-check
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractD", "TokenD")
	assert.Nil(t, nft, "Still not found")
	assert.Error(t, err, "Still not found")
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateSupplyReverse_BasicRevert(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// supply=2
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractE", "TokenE")
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractE", "TokenE")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// revert => supply=1
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	newSupply, err := nftDB.UpdateSupplyReverse(tx, "0xContractE", "TokenE")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), newSupply)
	require.NoError(t, tx.Commit())

	// check
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractE", "TokenE")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nft.Supply)
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateSupplyReverse_NonexistentNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = nftDB.UpdateSupplyReverse(tx, "0xContractF", "TokenF")
	assert.Error(t, err, "Expected error for nonexistent NFT")

	_ = tx.Rollback()
}

func TestNFTDb_UpdateSupplyReverse_SupplyWouldGoNegative(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// NFT starts supply=1
	_, err = nftDB.UpdateSupply(tx, "0xContractG", "TokenG")
	require.NoError(t, err)

	// revert once => supply=0
	_, err = nftDB.UpdateSupplyReverse(tx, "0xContractG", "TokenG")
	require.NoError(t, err)

	// revert again => negative => error
	_, err = nftDB.UpdateSupplyReverse(tx, "0xContractG", "TokenG")
	assert.Error(t, err)

	_ = tx.Rollback()
}

func TestNFTDb_UpdateBurntSupplyReverse_BasicRevert(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	// supply=1
	_, err = nftDB.UpdateSupply(tx, "0xContractH", "TokenH")
	require.NoError(t, err)
	// burn => burnt_supply=1
	err = nftDB.UpdateBurntSupply(tx, "0xContractH", "TokenH")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// revert burnt => burnt_supply=0
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	err = nftDB.UpdateBurntSupplyReverse(tx, "0xContractH", "TokenH")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// check
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractH", "TokenH")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), nft.BurntSupply)
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateBurntSupplyReverse_NonexistentNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	err = nftDB.UpdateBurntSupplyReverse(tx, "0xContractI", "TokenI")
	assert.Error(t, err)

	_ = tx.Rollback()
}

func TestNFTDb_UpdateBurntSupplyReverse_WouldGoNegative(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// supply=1 but burnt_supply=0 => revert burnt => negative => error
	_, err = nftDB.UpdateSupply(tx, "0xContractJ", "TokenJ")
	require.NoError(t, err)

	err = nftDB.UpdateBurntSupplyReverse(tx, "0xContractJ", "TokenJ")
	assert.Error(t, err, "Cannot revert burnt supply from 0")

	_ = tx.Rollback()
}

func TestNFTDb_GetNft_Nonexistent(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = nftDB.GetNft(tx, "0xContractK", "TokenK")
	assert.Error(t, err, "nonexistent NFT")

	_ = tx.Rollback()
}

func TestNFTDb_ClosedTxBehavior(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// commit immediately
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// now use closed tx
	_, err = nftDB.UpdateSupply(tx, "0xContractZ", "TokenZ")
	assert.Error(t, err, "tx is closed")
}

func TestNFTDb_ConcurrentAccess(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	var wg sync.WaitGroup
	contract := "0xConcurrent"
	tokenID := "TokenConc"

	// create NFT => supply=1
	txInit, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(txInit, contract, tokenID)
	require.NoError(t, err)
	require.NoError(t, txInit.Commit())

	worker := func(count int) {
		defer wg.Done()
		for i := 0; i < count; i++ {
			tx, err := db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			_, err = nftDB.UpdateSupply(tx, contract, tokenID)
			assert.NoError(t, err)
			err = tx.Commit()
			assert.NoError(t, err)
		}
	}

	wg.Add(2)
	go worker(50)
	go worker(50)
	wg.Wait()

	// final
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, contract, tokenID)
	require.NoError(t, err)
	// started with supply=1, plus 50+50
	assert.Equal(t, uint64(101), nft.Supply)
	_ = txCheck.Rollback()
}

func TestNFTDb_ErrorPropagation(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	cleanup() // close DB

	// any op => error
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		assert.Error(t, err)
	} else {
		_, err = nftDB.UpdateSupply(tx, "0xContractErr", "TokenErr")
		assert.Error(t, err, "closed DB transaction")
		_ = tx.Commit()
	}
}

func TestNFTDb_UpdateSupply_ErrorOnUpsert(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// 1) Insert the same (contract, tokenID) multiple times, it just increments supply
	tx1, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	supply, err := nftDB.UpdateSupply(tx1, "0xUpsert", "DupToken")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), supply)
	supply, err = nftDB.UpdateSupply(tx1, "0xUpsert", "DupToken")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), supply)
	require.NoError(t, tx1.Commit())

	// 2) If you *want* an error, let's rename the nfts table to break the upsert
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, renameErr := tx2.Exec(`ALTER TABLE nfts RENAME TO nfts_gone;`)
	require.NoError(t, renameErr)

	// Now calling UpdateSupply => error because table is missing
	_, err = nftDB.UpdateSupply(tx2, "0xUpsert", "DupToken")
	assert.Error(t, err, "Expected error due to renamed table")

	_ = tx2.Rollback()
}

func TestNFTDb_ForceSqlErrorByRenamingTable(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// create NFT => supply=3
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xRenameErr", "SomeToken")
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xRenameErr", "SomeToken")
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xRenameErr", "SomeToken")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// next Tx => rename table => break UpdateBurntSupply
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, renameErr := tx2.Exec(`ALTER TABLE nfts RENAME TO nfts_broken;`)
	require.NoError(t, renameErr)

	err = nftDB.UpdateBurntSupply(tx2, "0xRenameErr", "SomeToken")
	assert.Error(t, err, "table is missing => forced SQL error")

	_ = tx2.Rollback()
}

func TestNFTDb_ForceSqlError_UpdateSupply(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// 1) Insert once successfully
	supply, err := nftDB.UpdateSupply(tx, "0xErrUpsert", "TokenErrUpsert")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), supply)

	// 2) Now rename the table to break the next query
	_, err = tx.Exec("ALTER TABLE nfts RENAME TO nfts_broken;")
	require.NoError(t, err, "Renaming table should succeed in normal SQL usage")

	// 3) Attempt to insert again => forced SQL error
	_, err = nftDB.UpdateSupply(tx, "0xErrUpsert", "TokenErrUpsert")
	require.Error(t, err, "We expect an error because 'nfts' no longer exists")

	_ = tx.Rollback()
}

func TestNFTDb_ForceSqlError_UpdateSupplyReverse(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create NFT => supply=1
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xErrSupplyReverse", "TokenErrSupplyReverse")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Now rename the table in a new transaction, then attempt UpdateSupplyReverse
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = tx2.Exec("ALTER TABLE nfts RENAME TO nfts_gone;")
	require.NoError(t, err)

	_, err = nftDB.UpdateSupplyReverse(tx2, "0xErrSupplyReverse", "TokenErrSupplyReverse")
	require.Error(t, err, "Expected error because 'nfts' table is missing")

	_ = tx2.Rollback()
}

func TestNFTDb_ForceSqlError_UpdateBurntSupply(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create NFT => supply=1
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xErrBurn", "TokenErrBurn")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Now rename table => then attempt burn => forced error
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = tx2.Exec(`ALTER TABLE nfts RENAME TO nfts_broken;`)
	require.NoError(t, err)

	err = nftDB.UpdateBurntSupply(tx2, "0xErrBurn", "TokenErrBurn")
	require.Error(t, err, "Expected error: table is missing")

	_ = tx2.Rollback()
}

func TestNFTDb_ForceSqlError_UpdateBurntSupplyReverse(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// create NFT => burn => burnt_supply=1
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xErrBurnReverse", "TokenErrBurnReverse")
	require.NoError(t, err)
	err = nftDB.UpdateBurntSupply(tx, "0xErrBurnReverse", "TokenErrBurnReverse")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// rename => revert => error
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = tx2.Exec(`ALTER TABLE nfts RENAME TO nfts_missing;`)
	require.NoError(t, err)

	err = nftDB.UpdateBurntSupplyReverse(tx2, "0xErrBurnReverse", "TokenErrBurnReverse")
	require.Error(t, err, "Expected forced SQL error")

	_ = tx2.Rollback()
}

func TestNFTDb_ForceSqlError_GetNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// create NFT => supply=2
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xErrGetNft", "TokenErrGetNft")
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xErrGetNft", "TokenErrGetNft")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// rename => attempt GetNft => error
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = tx2.Exec(`ALTER TABLE nfts RENAME TO nfts_notfound;`)
	require.NoError(t, err)

	_, err = nftDB.GetNft(tx2, "0xErrGetNft", "TokenErrGetNft")
	require.Error(t, err, "Expected error on forced DB fail")

	_ = tx2.Rollback()
}

func TestNFTDb_NilTransaction(t *testing.T) {
	_, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// All these calls should fail/panic if the code doesn't guard against nil Tx.
	// Usually the database/sql calls will panic or produce an invalid memory address error.
	// You can decide how your code should handle nil Tx if at all.
	defer func() {
		// Recover from a potential panic to ensure test does not crash the suite.
		if r := recover(); r != nil {
			t.Logf("Recovered from panic as expected: %v", r)
		}
	}()

	_, err := nftDB.UpdateSupply(nil, "0xNilTx", "TokenNilTx")
	require.Error(t, err, "Expected error or panic with nil Tx")
}

func TestNFTDb_UpdateSupplyReverse_ForceErrorOnUpdate(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// 1) Create an NFT => supply=3
	{
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		_, err = nftDB.UpdateSupply(tx, "0xPartialErr", "TokenPartialErr")
		require.NoError(t, err)
		_, err = nftDB.UpdateSupply(tx, "0xPartialErr", "TokenPartialErr")
		require.NoError(t, err)
		_, err = nftDB.UpdateSupply(tx, "0xPartialErr", "TokenPartialErr")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	// 2) Now in a new Tx, rename 'nfts' => create a view named 'nfts'
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// rename table
	_, err = tx2.Exec("ALTER TABLE nfts RENAME TO real_nfts;")
	require.NoError(t, err)

	// create a NON-updatable view with the same columns
	// We use an aggregate function (e.g. burnt_supply+0) to ensure it's not updatable.
	_, err = tx2.Exec(`
		CREATE VIEW nfts AS
		SELECT contract, token_id, supply, burnt_supply+0 as burnt_supply
		FROM real_nfts;
	`)
	require.NoError(t, err)

	// 3) Attempt UpdateSupplyReverse => SELECT part will succeed,
	// but the UPDATE should fail since 'nfts' is now a non-updatable view.
	// Because the NFT *exists*, we won't trigger the sql.ErrNoRows case.
	// We'll only fail on the final UPDATE.
	newSupply, err := nftDB.UpdateSupplyReverse(tx2, "0xPartialErr", "TokenPartialErr")

	// We expect an error from the UPDATE statement
	require.Error(t, err, "UPDATE on a non-updatable view should fail")
	assert.Equal(t, uint64(0), newSupply, "We didn't get a new supply because it should fail before returning")

	_ = tx2.Rollback()
}

func TestNFTDb_UpdateBurntSupplyReverse_ForceErrorOnUpdate(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// 1) Create NFT => supply=2 => burnt=1
	{
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		// supply => 2
		_, err = nftDB.UpdateSupply(tx, "0xPartialBurnErr", "TokenPartialBurnErr")
		require.NoError(t, err)
		_, err = nftDB.UpdateSupply(tx, "0xPartialBurnErr", "TokenPartialBurnErr")
		require.NoError(t, err)
		// burn => burnt=1
		err = nftDB.UpdateBurntSupply(tx, "0xPartialBurnErr", "TokenPartialBurnErr")
		require.NoError(t, err)

		require.NoError(t, tx.Commit())
	}

	// 2) Replace the real 'nfts' with a non-updatable view
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = tx2.Exec("ALTER TABLE nfts RENAME TO real_nfts;")
	require.NoError(t, err)

	_, err = tx2.Exec(`
		CREATE VIEW nfts AS
		SELECT contract, token_id, supply, burnt_supply+0 as burnt_supply
		FROM real_nfts;
	`)
	require.NoError(t, err)

	// 3) Now call UpdateBurntSupplyReverse => the SELECT should succeed
	// (burnt_supply = 1) => no error from that part =>
	// but the final UPDATE fails because it's a view.
	err = nftDB.UpdateBurntSupplyReverse(tx2, "0xPartialBurnErr", "TokenPartialBurnErr")
	require.Error(t, err, "Expect error updating a non-updatable view")

	_ = tx2.Rollback()
}

func TestNewNFT(t *testing.T) {
	nft := newNFT()
	require.NotNil(t, nft)
}
