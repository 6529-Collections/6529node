package tdh_calculation

import (
	"crypto/sha256"
	"fmt"
)

type DataItem struct {
	Key   string
	Value int
}

func hashPair(a, b string) string {
	h := sha256.New()
	h.Write([]byte(a + b))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func GetMerkleRoot(data []DataItem) string {
	// Step 1: Generate leaf nodes by hashing each address-value pair
	leaves := make([]string, len(data))
	for i, item := range data {
		h := sha256.New()
		h.Write([]byte(fmt.Sprintf("%s:%d", item.Key, item.Value)))
		leaves[i] = fmt.Sprintf("%x", h.Sum(nil))
	}

	// Step 2: Build the Merkle Tree by hashing pairs until we reach the root
	for len(leaves) > 1 {
		tempLeaves := make([]string, 0, (len(leaves)+1)/2)

		for i := 0; i < len(leaves); i += 2 {
			if i+1 < len(leaves) {
				// Hash pairs of leaves
				tempLeaves = append(tempLeaves, hashPair(leaves[i], leaves[i+1]))
			} else {
				// If odd number, duplicate the last leaf
				tempLeaves = append(tempLeaves, leaves[i])
			}
		}

		leaves = tempLeaves
	}

	// The last remaining element is the Merkle root
	return fmt.Sprintf("0x%s", leaves[0])
}
