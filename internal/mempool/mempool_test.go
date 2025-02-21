package mempool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMempoolInterface(t *testing.T) {
	mp := NewMempool()

	assert.Equal(t, 0, mp.Size())

	err := mp.AddTransaction(&Transaction{ID: "tx123"})
	assert.NoError(t, err)
	assert.Equal(t, 1, mp.Size())

	blockTxs := mp.GetTransactionsForBlock(10)
	assert.Len(t, blockTxs, 1)
	assert.Equal(t, "tx123", blockTxs[0].ID)

	mp.RemoveTransactions(blockTxs)
	assert.Equal(t, 1, mp.Size())

	err = mp.ReinjectOrphanedTxs(blockTxs)
	assert.NoError(t, err)
}
