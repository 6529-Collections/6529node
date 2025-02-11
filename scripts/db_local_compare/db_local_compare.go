package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/6529-Collections/6529node/pkg/constants"
	_ "github.com/mattn/go-sqlite3"

	"github.com/dgraph-io/badger/v4"
)

// actionsReceivedCheckpointKey holds the key used for storing the checkpoint in Badger.
const actionsReceivedCheckpointKey = "tdh:actionsReceivedCheckpoint"

func main() {
    sqliteDBPath := flag.String("sqlite", "", "Path to the SQLite DB")
    flag.Parse()

    if *sqliteDBPath == "" {
        log.Fatalf("SQLite DB path is required")
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
    // c) Retrieve all transactions with blockNumber < cutoff from Badger
    //
    // Keys look like:
    // "tdh:transfer:{blockNumber}:{txIndex}:{logIndex}:{txHash}:{contract}:{tokenID}"
    //
    // blockNumber is zero-padded to 10 digits in the key for lexical ordering.
    // We'll store these transaction hashes in a set for easy comparison.
    // ------------------------------------------------------------------------
    var badgerTxHashes = make(map[string]struct{})

    err = db.View(func(txn *badger.Txn) error {
        prefix := []byte("tdh:transfer:")
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        it := txn.NewIterator(opts)
        defer it.Close()

        for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
            item := it.Item()
            k := item.Key()
            keyStr := string(k)

            // Key format = tdh:transfer:0000000001:00001:00001:0xHASH:contract:tokenId
            parts := strings.Split(keyStr, ":")
            if len(parts) < 7 {
                // Not a well-formed transfer key, skip
                continue
            }

            // Extract zero-padded blockNumber from key
            bnStr := parts[2]
            bn, err := strconv.ParseUint(bnStr, 10, 64)
            if err != nil {
                log.Printf("Failed to parse blockNumber from key %s: %v", keyStr, err)
                continue
            }

            // We only care about transfers with blockNumber < cutoffBlockNumber
            if bn < cutoffBlockNumber {
                // The transaction hash is at parts[5]
                txHash := parts[5]
                badgerTxHashes[txHash] = struct{}{}
            } else {
                // Because of zero-padding, once bn >= cutoffBlockNumber,
                // all subsequent are >= as well, so we can break early.
                break
            }
        }
        return nil
    })
    if err != nil {
        log.Fatalf("Failed to iterate Badger for transfers: %v", err)
    }

    fmt.Printf("Found %d transaction(s) in Badger below block %d\n", len(badgerTxHashes), cutoffBlockNumber)

    // ------------------------------------------------------------------------
    // d) Connect to a SQLite DB
    // ------------------------------------------------------------------------
    dbSQL, err := sql.Open("sqlite3", *sqliteDBPath)
    if err != nil {
        log.Fatalf("Failed to open SQLite DB at %s: %v", *sqliteDBPath, err)
    }
    defer dbSQL.Close()

    // ------------------------------------------------------------------------
    // e) Retrieve transaction hashes from SQLite for blocks < cutoffBlockNumber
    //    We assume the `transactions` table has columns:
    //      - `block` (the block number)
    //      - `transaction` (the tx hash)
    // ------------------------------------------------------------------------
    tdhContracts := []string{
        constants.MEMES_CONTRACT,
        constants.NEXTGEN_CONTRACT,
        constants.GRADIENTS_CONTRACT,
    }
    placeholders := strings.Repeat("?,", len(tdhContracts))
	placeholders = placeholders[:len(placeholders)-1]
    query := fmt.Sprintf(`SELECT "transaction" FROM transactions WHERE contract IN (%s) AND block < ?`, placeholders)
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

    var sqliteTxHashes = make(map[string]struct{})
    for rows.Next() {
        var txHash string
        if err := rows.Scan(&txHash); err != nil {
            log.Fatalf("Failed to scan row: %v", err)
        }
        sqliteTxHashes[txHash] = struct{}{}
    }
    if err := rows.Err(); err != nil {
        log.Fatalf("Row iteration error: %v", err)
    }

    fmt.Printf("Found %d transaction(s) in SQLite below block %d\n", len(sqliteTxHashes), cutoffBlockNumber)

    // ------------------------------------------------------------------------
    // f) Compare the two sets of transaction hashes and find differences.
    //    - Which are in Badger but not in SQLite?
    //    - Which are in SQLite but not in Badger?
    // ------------------------------------------------------------------------
    var missingInSQLite []string
    var missingInBadger []string

    // Find hashes present in Badger but not in SQLite
    for txHash := range badgerTxHashes {
        if _, exists := sqliteTxHashes[txHash]; !exists {
            missingInSQLite = append(missingInSQLite, txHash)
        }
    }

    // Find hashes present in SQLite but not in Badger
    for txHash := range sqliteTxHashes {
        if _, exists := badgerTxHashes[txHash]; !exists {
            missingInBadger = append(missingInBadger, txHash)
        }
    }

    if len(missingInSQLite) > 0 {
        fmt.Println("❌ Transactions present in Badger but missing in SQLite:")
        fmt.Println(strings.Join(missingInSQLite, "\n"))
    } else {
        fmt.Println("✅ All transactions of SQLite are present in Badger")
    }

    if len(missingInBadger) > 0 {
        fmt.Println("❌ Transactions present in SQLite but missing in Badger:")
        fmt.Println(strings.Join(missingInBadger, "\n"))
    } else {
        fmt.Println("✅ All transactions of Badger are present in SQLite")
    }
}
