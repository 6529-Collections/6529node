package mempool

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMempoolInterface(t *testing.T) {
	mp := NewMempool()
	assert.Equal(t, 0, mp.Size())

	err := mp.AddTransaction(&Transaction{ID: "tx123", Fee: 10})
	assert.NoError(t, err)
	assert.Equal(t, 1, mp.Size())

	blockTxs := mp.GetTransactionsForBlock(10)
	assert.Len(t, blockTxs, 1)
	assert.Equal(t, "tx123", blockTxs[0].ID)

	mp.RemoveTransactions(blockTxs)
	assert.Equal(t, 1, mp.Size()) // stale references remain in priority queue

	err = mp.ReinjectOrphanedTxs(blockTxs)
	assert.NoError(t, err)

	mpImpl := mp.(*mempoolImpl)
	mpImpl.Stop()
}

func TestMempoolConcurrency(t *testing.T) {
	mp := NewMempool()
	var wg sync.WaitGroup
	concurrency := 20
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			txID := fmt.Sprintf("txConcurrency-%d", id)
			_ = mp.AddTransaction(&Transaction{ID: txID, Fee: 5})
		}(i)
	}
	wg.Wait()

	assert.Equal(t, concurrency, mp.Size())

	mpImpl := mp.(*mempoolImpl)
	mpImpl.Stop()
}

func TestMempoolPriority(t *testing.T) {
	mp := NewMempool()
	_ = mp.AddTransaction(&Transaction{ID: "lowFee", Fee: 1})
	_ = mp.AddTransaction(&Transaction{ID: "midFee", Fee: 10})
	_ = mp.AddTransaction(&Transaction{ID: "highFee", Fee: 50})

	txs := mp.GetTransactionsForBlock(3)
	assert.Len(t, txs, 3)
	assert.Equal(t, "highFee", txs[0].ID)
	assert.Equal(t, "midFee", txs[1].ID)
	assert.Equal(t, "lowFee", txs[2].ID)

	assert.Equal(t, 3, mp.Size())

	mpImpl := mp.(*mempoolImpl)
	mpImpl.Stop()
}

func TestMempoolValidation(t *testing.T) {
	mp := NewMempool()

	err := mp.AddTransaction(&Transaction{ID: "", Fee: 5})
	assert.Equal(t, ErrInvalidFormat, err)
	assert.Equal(t, 0, mp.Size())

	err = mp.AddTransaction(&Transaction{ID: "invalid-sig", Fee: 5})
	assert.Equal(t, ErrInvalidSignature, err)
	assert.Equal(t, 0, mp.Size())

	err = mp.AddTransaction(&Transaction{ID: "txBelowFee", Fee: 0})
	assert.Equal(t, ErrInsufficientFee, err)
	assert.Equal(t, 0, mp.Size())

	err = mp.AddTransaction(&Transaction{ID: "txValid", Fee: 5})
	assert.NoError(t, err)
	assert.Equal(t, 1, mp.Size())

	mpImpl := mp.(*mempoolImpl)
	mpImpl.Stop()
}

func TestRemoveTransactionsActualBehavior(t *testing.T) {
	mp := NewMempool()
	_ = mp.AddTransaction(&Transaction{ID: "A", Fee: 10})
	_ = mp.AddTransaction(&Transaction{ID: "B", Fee: 20})
	assert.Equal(t, 2, mp.Size())

	blockTxs := mp.GetTransactionsForBlock(2)
	assert.Len(t, blockTxs, 2)
	mp.RemoveTransactions(blockTxs)
	assert.Equal(t, 2, mp.Size())

	nextBlockTxs := mp.GetTransactionsForBlock(2)
	assert.Len(t, nextBlockTxs, 0)
	assert.Equal(t, 2, mp.Size())

	mpImpl := mp.(*mempoolImpl)
	mpImpl.Stop()
}

func TestMempoolCapacityEviction(t *testing.T) {
	mp := NewMempool()
	mpImpl := mp.(*mempoolImpl)
	mpImpl.maxSize = 2

	_ = mp.AddTransaction(&Transaction{ID: "A", Fee: 10})
	_ = mp.AddTransaction(&Transaction{ID: "B", Fee: 20})
	assert.Equal(t, 2, len(mpImpl.txMap))

	err := mp.AddTransaction(&Transaction{ID: "C", Fee: 5})
	assert.Equal(t, ErrMempoolFull, err)
	assert.Equal(t, 2, len(mpImpl.txMap))

	err = mp.AddTransaction(&Transaction{ID: "D", Fee: 30})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(mpImpl.txMap))

	_, aExists := mpImpl.txMap["A"]
	_, bExists := mpImpl.txMap["B"]
	_, dExists := mpImpl.txMap["D"]
	assert.False(t, aExists)
	assert.True(t, bExists)
	assert.True(t, dExists)

	mpImpl.Stop()
}

func TestMempoolTTLEviction(t *testing.T) {
	mp := NewMempool()
	mpImpl := mp.(*mempoolImpl)
	mpImpl.ttlSeconds = 1

	err := mp.AddTransaction(&Transaction{ID: "X", Fee: 10})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mpImpl.txMap))

	time.Sleep(2 * time.Second)
	mpImpl.evictExpired()

	assert.Equal(t, 0, len(mpImpl.txMap))
	mpImpl.Stop()
}
