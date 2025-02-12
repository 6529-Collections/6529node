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

// deriveSQLiteType derives a TransferType from the given row data (fromAddr, toAddr, value).
func deriveSQLiteType(fromAddr, toAddr string, value float64) tokens.TransferType {
	switch {
	case fromAddr == constants.NULL_ADDRESS:
		// from = null/0x0
		if value > 0 {
			return tokens.MINT
		} else {
			return tokens.AIRDROP
		}
	case toAddr == constants.NULL_ADDRESS || toAddr == constants.DEAD_ADDRESS:
		// to = null/0x0 => burn
		return tokens.BURN
	case value > 0:
		// normal transfer with value => sale
		return tokens.SALE
	default:
		// normal transfer with no value => send
		return tokens.SEND
	}
}

// TxKey identifies a transfer by TxHash, From, To, Contract, and TokenID.
type TxKey struct {
	TxHash   string
	From     string
	To       string
	Contract string
	TokenID  string
}

// mismatch keeps track of where Badger's TransferType differs from SQLite's derived type.
type mismatch struct {
	Key        TxKey
	BadgerType tokens.TransferType
	SQLiteType tokens.TransferType
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
	db, err := badger.Open(
		badger.DefaultOptions(dbPath).
			WithReadOnly(true).
			WithLogger(nil),
	)
	if err != nil {
		log.Fatalf("Failed to open Badger DB: %v", err)
	}
	defer db.Close()

	// ------------------------------------------------------------------------
	// b) Get the checkpoint index from Badger (blockNumber:transactionIndex:logIndex)
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

	parts := strings.Split(checkpoint, ":")
	if len(parts) != 3 {
		log.Fatalf("Unexpected checkpoint format %q (expected block:txIndex:logIndex)", checkpoint)
	}
	blockNumberStr := parts[0]
	cutoffBlockNumber, err := strconv.ParseUint(blockNumberStr, 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse block number from checkpoint: %v", err)
	}
	fmt.Printf("Checkpoint found: %s (blockNumber=%d)\n", checkpoint, cutoffBlockNumber)

	// ------------------------------------------------------------------------
	// c) Retrieve Badger transfers (< cutoffBlockNumber)
	//    storing them in badgerTransfers[TxKey] = TransferType
	// ------------------------------------------------------------------------
	badgerTransfers := make(map[TxKey]tokens.TransferType)

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

			// Key format: tdh:transfer:0000000001:00001:00001:0xHASH:contract:tokenId
			keyParts := strings.Split(keyStr, ":")
			if len(keyParts) < 7 {
				continue
			}
			bnStr := keyParts[2]
			bn, parseErr := strconv.ParseUint(bnStr, 10, 64)
			if parseErr != nil {
				log.Printf("Failed to parse blockNumber from key=%s: %v", keyStr, parseErr)
				continue
			}
			if bn >= cutoffBlockNumber {
				// Because of zero-padding, all subsequent keys also >= cutoff
				break
			}

			errVal := item.Value(func(val []byte) error {
				var tt tokens.TokenTransfer
				if errJSON := json.Unmarshal(val, &tt); errJSON != nil {
					return fmt.Errorf("unmarshal error: %w", errJSON)
				}
				// Build the TxKey
				txKey := TxKey{
					TxHash:   strings.ToLower(tt.TxHash),
					From:     strings.ToLower(tt.From),
					To:       strings.ToLower(tt.To),
					Contract: strings.ToLower(tt.Contract),
					TokenID:  strings.ToLower(tt.TokenID),
				}
				badgerTransfers[txKey] = tt.Type
				return nil
			})
			if errVal != nil {
				log.Printf("Warning: can't read JSON for key=%s, err=%v", keyStr, errVal)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to iterate Badger for transfers: %v", err)
	}

	fmt.Printf("Found %d transfers in Badger below block %d\n", len(badgerTransfers), cutoffBlockNumber)

	// ------------------------------------------------------------------------
	// d) Connect to the SQLite DB
	// ------------------------------------------------------------------------
	dbSQL, err := sql.Open("sqlite3", *sqliteDBPath)
	if err != nil {
		log.Fatalf("Failed to open SQLite DB at %s: %v", *sqliteDBPath, err)
	}
	defer dbSQL.Close()

	// ------------------------------------------------------------------------
	// e) Retrieve from SQLite: transaction, from_address, to_address, contract, token_id, value
	//    Only for certain contracts, block < cutoff.
	//    Then we derive the TransferType from (from, to, value).
	// ------------------------------------------------------------------------
	tdhContracts := []string{
		constants.MEMES_CONTRACT,
		constants.NEXTGEN_CONTRACT,
		constants.GRADIENTS_CONTRACT,
	}
	ph := strings.Repeat("?,", len(tdhContracts))
	ph = ph[:len(ph)-1] // remove trailing comma

	query := fmt.Sprintf(`
		SELECT
			"transaction",
			"from_address",
			"to_address",
			"contract",
			"token_id",
			"value"
		FROM transactions
		WHERE contract IN (%s)
		  AND block < ?
	`, ph)

	args := make([]interface{}, len(tdhContracts)+1)
	for i, c := range tdhContracts {
		args[i] = c
	}
	args[len(tdhContracts)] = cutoffBlockNumber

	rows, err := dbSQL.Query(query, args...)
	if err != nil {
		log.Fatalf("Failed to query SQLite: %v", err)
	}
	defer rows.Close()

	sqliteTransfers := make(map[TxKey]tokens.TransferType)

	for rows.Next() {
		var (
			txHash   string
			fromAddr string
			toAddr   string
			contract string
			tokenID  string
			value    float64
		)

		if err := rows.Scan(&txHash, &fromAddr, &toAddr, &contract, &tokenID, &value); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		txKey := TxKey{
			TxHash:   strings.ToLower(txHash),
			From:     strings.ToLower(fromAddr),
			To:       strings.ToLower(toAddr),
			Contract: strings.ToLower(contract),
			TokenID:  strings.ToLower(tokenID),
		}
		derivedType := deriveSQLiteType(txKey.From, txKey.To, value)
		sqliteTransfers[txKey] = derivedType
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v", err)
	}

	fmt.Printf("Found %d transfers in SQLite below block %d\n", len(sqliteTransfers), cutoffBlockNumber)

	// ------------------------------------------------------------------------
	// f) Compare the sets
	//    1) Missing in SQLite vs. missing in Badger
	//    2) Type mismatches for transfers that exist in both.
	// ------------------------------------------------------------------------

	var missingInSQLite []TxKey
	var missingInBadger []TxKey
	var mismatches []mismatch

	// We’ll collect keys that appear in both sets so we can check type mismatches
	for key, bType := range badgerTransfers {
		sType, inSQLite := sqliteTransfers[key]
		if !inSQLite {
			// Key is not in sqlite
			missingInSQLite = append(missingInSQLite, key)
		} else {
			// Key is in both => compare types
			if bType != sType {
				mismatches = append(mismatches, mismatch{Key: key, BadgerType: bType, SQLiteType: sType})
			}
		}
	}
	// Now look for those in SQLite but not in Badger
	for key := range sqliteTransfers {
		if _, inBadger := badgerTransfers[key]; !inBadger {
			missingInBadger = append(missingInBadger, key)
		}
	}

	fmt.Println("\n======= RESULTS =======")

	// (1) Missing in SQLite
	if len(missingInSQLite) > 0 {
		fmt.Printf("\n❌ Present in Badger but missing in SQLite (x%d):\n", len(missingInSQLite))
		for _, key := range missingInSQLite {
			fmt.Printf("   %s | from=%s => to=%s | contract=%s | tokenId=%s\n",
				key.TxHash, key.From, key.To, key.Contract, key.TokenID)
		}
	} else {
		fmt.Println("\n✅ All Badger transfers are present in SQLite")
	}

	// (2) Missing in Badger
	if len(missingInBadger) > 0 {
		fmt.Printf("\n❌ Present in SQLite but missing in Badger (x%d):\n", len(missingInBadger))
		for _, key := range missingInBadger {
			fmt.Printf("   %s | from=%s => to=%s | contract=%s | tokenId=%s\n",
				key.TxHash, key.From, key.To, key.Contract, key.TokenID)
		}
	} else {
		fmt.Println("\n✅ All SQLite transfers are present in Badger")
	}

	// (3) Type Mismatches
	if len(mismatches) > 0 {
		fmt.Printf("\n❌ Transfer Type Mismatches (x%d)\n", len(mismatches))
		// for _, mm := range mismatches {
		// 	fmt.Printf("\n\nTRX Hash: %s\n", mm.Key.TxHash)
		// 	fmt.Printf("Badger: %s	|	SQLite: %s\n",
		// 		mm.BadgerType, mm.SQLiteType)
		// }
	} else {
		fmt.Println("\n✅ No type mismatches between Badger and SQLite for the common TxKeys.")
	}
}
