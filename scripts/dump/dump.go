package main

import (
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"unicode"

	"github.com/dgraph-io/badger/v4"
)

const dbPath = "./db"

func main() {
	// Command-line flags
	outputMode := flag.String("o", "console", "Output mode: 'console' or 'file'")
	outputFile := flag.String("f", "dump.txt", "Output file (if mode is 'file')")
	flag.Parse()

	var out *os.File
	var err error

	// Handle output destination
	if *outputMode == "file" {
		out, err = os.Create(*outputFile)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer out.Close()
	} else {
		out = os.Stdout // Default to console
	}

	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		log.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	if *outputMode == "file" {
		fmt.Println("Dumping BadgerDB contents to file", *outputFile)
	} else {
		fmt.Println("Dumping BadgerDB contents to console")
	}

	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			keyStr := string(key)

			// Detect possible numeric suffix (uint64)
			var keyPrefix string
			var numericValue uint64
			hasNumericSuffix := len(key) > 8 && isBinaryUint64(key[len(key)-8:])

			if hasNumericSuffix {
				numericValue = binary.BigEndian.Uint64(key[len(key)-8:])
				keyPrefix = string(key[:len(key)-8])
			} else {
				keyPrefix = keyStr
			}

			err := item.Value(func(val []byte) error {
				fmt.Fprintf(out, "Key: %s\n", keyStr)
				if hasNumericSuffix {
					fmt.Fprintf(out, "  Prefix: %s\n", keyPrefix)
					fmt.Fprintf(out, "  Numeric Suffix (uint64): %d\n", numericValue)
				}

				// Value processing
				if len(val) == 8 {
					// Big-endian uint64
					num := binary.BigEndian.Uint64(val)
					fmt.Fprintf(out, "  Value (uint64): %d\n", num)
				} else if len(val) == 32 {
					// Probably a hash (Ethereum block/tx hash)
					fmt.Fprintf(out, "  Value (Hex, 32-byte data): %s\n", hex.EncodeToString(val))
				} else if isPrintable(val) {
					// Printable string
					fmt.Fprintf(out, "  Value (String): %s\n", string(val))
				} else {
					// Unknown binary data
					fmt.Fprintf(out, "  Value (Hex): %s\n", hex.EncodeToString(val))
				}
				fmt.Fprintln(out, "-------------------------")
				return nil
			})

			if err != nil {
				fmt.Fprintf(out, "  [ERROR] Could not read value: %v\n", err)
			}
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error while iterating: %v", err)
	}

	fmt.Println("Dump complete.")
}

// isBinaryUint64 checks if the last 8 bytes are likely a big-endian uint64.
func isBinaryUint64(data []byte) bool {
	if len(data) != 8 {
		return false
	}
	// Heuristic: A uint64 is unlikely to contain printable ASCII characters.
	for _, b := range data {
		if unicode.IsPrint(rune(b)) {
			return false
		}
	}
	return true
}

// isPrintable checks if a byte slice consists of printable characters.
func isPrintable(data []byte) bool {
	for _, b := range data {
		if b < 32 || b > 126 {
			return false
		}
	}
	return true
}
