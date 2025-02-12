package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	_ "github.com/mattn/go-sqlite3"

	"github.com/dgraph-io/badger/v4"
)

// actionsReceivedCheckpointKey holds the key used for storing the checkpoint in Badger.
const actionsReceivedCheckpointKey = "tdh:actionsReceivedCheckpoint"

// TxKey represents the (transactionHash, from, to, contract, tokenId) 5-tuple.
type TxKey struct {
	TxHash   string
	From     string
	To       string
	Contract string
	TokenID  string
}

func main() {
	sqliteDBPath := flag.String("sqlite", "", "Path to the SQLite DB")
	flag.Parse()

	if *sqliteDBPath == "" {
		log.Fatalf("SQLite DB path is required (use --sqlite=/path/to/db.sqlite)")
	}

	// ------------------------------------------------------------------------
	// a) Open Badger in read-only mode
	// ------------------------------------------------------------------------
	const dbPath = "./db"
	db, err := badger.Open(badger.DefaultOptions(dbPath).WithReadOnly(true))
	if err != nil {
		log.Fatalf("Failed to open Badger DB: %v", err)
	}
	defer db.Close()

	// ------------------------------------------------------------------------
	// b) Get the checkpoint index from Badger
	//    We expect a string in the format "blockNumber:transactionIndex:logIndex".
	// ------------------------------------------------------------------------
	var checkpoint string
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(actionsReceivedCheckpointKey))
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		checkpoint = string(val)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to retrieve checkpoint: %v", err)
	}

	// Parse the checkpoint string to extract the block number.
	parts := strings.Split(checkpoint, ":")
	if len(parts) != 3 {
		log.Fatalf("Unexpected checkpoint format (%q). Expected 'block:txIndex:logIndex'.", checkpoint)
	}
	blockNumberStr := parts[0]
	cutoffBlockNumber, err := strconv.ParseUint(blockNumberStr, 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse block number from checkpoint: %v", err)
	}

	fmt.Printf("Checkpoint found: %s (blockNumber=%d)\n", checkpoint, cutoffBlockNumber)

	// ------------------------------------------------------------------------
	// c) Retrieve (TxHash, From, To, Contract, TokenID) for blockNumber < cutoff from Badger
	//
	// Keys look like:
	//   "tdh:transfer:{blockNumber}:{txIndex}:{logIndex}:{txHash}:{contract}:{tokenID}"
	//
	// We'll unmarshal the JSON (tokens.TokenTransfer) to get from/to/contract/tokenID.
	// ------------------------------------------------------------------------
	badgerTransfers := make(map[TxKey]struct{})

	err = db.View(func(txn *badger.Txn) error {
		prefix := []byte("tdh:transfer:")
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			keyStr := string(key)

			// Key format = tdh:transfer:0000000001:00001:00001:0xHASH:contract:tokenId
			keyParts := strings.Split(keyStr, ":")
			if len(keyParts) < 7 {
				// Not a well-formed transfer key, skip
				continue
			}

			// Extract blockNumber from key
			bnStr := keyParts[2]
			bn, parseErr := strconv.ParseUint(bnStr, 10, 64)
			if parseErr != nil {
				log.Printf("Failed to parse blockNumber from key %s: %v", keyStr, parseErr)
				continue
			}

			// We only care about transfers with blockNumber < cutoffBlockNumber
			if bn >= cutoffBlockNumber {
				// Because of zero-padding, once bn >= cutoffBlockNumber,
				// all subsequent keys are also >= that number. Break.
				break
			}

			errVal := item.Value(func(val []byte) error {
				var transfer tokens.TokenTransfer
				if errJSON := json.Unmarshal(val, &transfer); errJSON != nil {
					return fmt.Errorf("unmarshal error: %w", errJSON)
				}
				txKey := TxKey{
					TxHash:   strings.ToLower(transfer.TxHash),
					From:     strings.ToLower(transfer.From),
					To:       strings.ToLower(transfer.To),
					Contract: strings.ToLower(transfer.Contract),
					TokenID:  strings.ToLower(transfer.TokenID),
				}
				badgerTransfers[txKey] = struct{}{}
				return nil
			})
			if errVal != nil {
				log.Printf("Warning: can't read JSON for key=%s, err=%v", keyStr, errVal)
				// continue anyway
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to iterate Badger for transfers: %v", err)
	}

	fmt.Printf("Found %d (txHash, from, to, contract, tokenId) combos in Badger below block %d\n",
		len(badgerTransfers), cutoffBlockNumber)

	// ------------------------------------------------------------------------
	// d) Connect to the SQLite DB
	// ------------------------------------------------------------------------
	dbSQL, err := sql.Open("sqlite3", *sqliteDBPath)
	if err != nil {
		log.Fatalf("Failed to open SQLite DB at %s: %v", *sqliteDBPath, err)
	}
	defer dbSQL.Close()

	// ------------------------------------------------------------------------
	// e) Retrieve (transaction, from_address, to_address, contract, token_id) from SQLite
	//    for block < cutoff, only for the specified TDH contracts.
	// ------------------------------------------------------------------------
	tdhContracts := []string{
		constants.MEMES_CONTRACT,
		constants.NEXTGEN_CONTRACT,
		constants.GRADIENTS_CONTRACT,
	}

	placeholders := strings.Repeat("?,", len(tdhContracts))
	placeholders = placeholders[:len(placeholders)-1]

	query := fmt.Sprintf(`SELECT "transaction", "from_address", "to_address", "contract", "token_id"
                          FROM transactions
                          WHERE contract IN (%s)
                            AND block < ?`,
		placeholders,
	)

	args := make([]interface{}, len(tdhContracts)+1)
	for i, v := range tdhContracts {
		args[i] = v
	}
	args[len(tdhContracts)] = cutoffBlockNumber

	rows, err := dbSQL.Query(query, args...)
	if err != nil {
		log.Fatalf("Failed to query SQLite: %v", err)
	}
	defer rows.Close()

	sqliteTransfers := make(map[TxKey]struct{})
	for rows.Next() {
		var (
			txHash    string
			fromAddr  string
			toAddr    string
			contract  string
			tokenID   string
		)
		if err := rows.Scan(&txHash, &fromAddr, &toAddr, &contract, &tokenID); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		txKey := TxKey{
			TxHash:   strings.ToLower(txHash),
			From:     strings.ToLower(fromAddr),
			To:       strings.ToLower(toAddr),
			Contract: strings.ToLower(contract),
			TokenID:  strings.ToLower(tokenID),
		}
		sqliteTransfers[txKey] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v", err)
	}

	fmt.Printf("Found %d (txHash, from, to, contract, tokenId) combos in SQLite below block %d\n",
		len(sqliteTransfers), cutoffBlockNumber)

	// ------------------------------------------------------------------------
	// f) Compare the two sets by TxKey
	// ------------------------------------------------------------------------
	var missingInSQLite []TxKey
	var missingInBadger []TxKey

	// Items in BadgerTransfers but not in SqliteTransfers
	for txKey := range badgerTransfers {
		if _, exists := sqliteTransfers[txKey]; !exists {
			missingInSQLite = append(missingInSQLite, txKey)
		}
	}

	// Items in SqliteTransfers but not in BadgerTransfers
	for txKey := range sqliteTransfers {
		if _, exists := badgerTransfers[txKey]; !exists {
			missingInBadger = append(missingInBadger, txKey)
		}
	}

	// Print differences
	if len(missingInSQLite) > 0 {
		fmt.Println("\n❌ These (txHash, from, to, contract, tokenId) combos are in Badger but missing in SQLite:")
		for _, txKey := range missingInSQLite {
			fmt.Printf("   %s | from=%s => to=%s | contract=%s | tokenId=%s\n",
				txKey.TxHash, txKey.From, txKey.To, txKey.Contract, txKey.TokenID)
		}
	} else {
		fmt.Println("✅ All (txHash, from, to, contract, tokenId) in Badger are also present in SQLite")
	}

	if len(missingInBadger) > 0 {
		fmt.Println("\n❌ These (txHash, from, to, contract, tokenId) combos are in SQLite but missing in Badger:")
		for _, txKey := range missingInBadger {
			fmt.Printf("   %s | from=%s => to=%s | contract=%s | tokenId=%s\n",
				txKey.TxHash, txKey.From, txKey.To, txKey.Contract, txKey.TokenID)
		}
	} else {
		fmt.Println("\n✅ All (txHash, from, to, contract, tokenId) in SQLite are also present in Badger")
	}
}
