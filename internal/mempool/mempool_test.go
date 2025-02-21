package mempool

import (
	"sync"
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

func TestMempoolConcurrency(t *testing.T) {
	mp := NewMempool()

	var wg sync.WaitGroup
	concurrency := 20
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			_ = mp.AddTransaction(&Transaction{ID: "txConcurrency"})
		}(i)
	}
	wg.Wait()

	assert.Equal(t, concurrency, mp.Size())
}
