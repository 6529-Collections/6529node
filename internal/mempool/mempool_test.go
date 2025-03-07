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
	// Lazy removal means the PQ still has one stale reference,
	// so Size() remains 1.
	assert.Equal(t, 1, mp.Size())

	err = mp.ReinjectOrphanedTxs(blockTxs)
	assert.NoError(t, err)

	// Clean up goroutine
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

	// If all are valid, we should see them in the queue (plus stale items).
	// Because we haven't removed anything, size should match concurrency in the PQ.
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
	// Should see them in descending fee order
	assert.Len(t, txs, 3)
	assert.Equal(t, "highFee", txs[0].ID)
	assert.Equal(t, "midFee", txs[1].ID)
	assert.Equal(t, "lowFee", txs[2].ID)
	// The PQ is re-pushed so Size() is still 3
	assert.Equal(t, 3, mp.Size())

	mpImpl := mp.(*mempoolImpl)
	mpImpl.Stop()
}

func TestMempoolValidation(t *testing.T) {
	mp := NewMempool()

	// Empty ID -> invalid format
	err := mp.AddTransaction(&Transaction{ID: "", Fee: 5})
	assert.Equal(t, ErrInvalidFormat, err)
	assert.Equal(t, 0, mp.Size())

	// "invalid-sig" -> stub signature check fails
	err = mp.AddTransaction(&Transaction{ID: "invalid-sig", Fee: 5})
	assert.Equal(t, ErrInvalidSignature, err)
	assert.Equal(t, 0, mp.Size())

	// Fee below baseFee=1 -> insufficient
	err = mp.AddTransaction(&Transaction{ID: "txBelowFee", Fee: 0})
	assert.Equal(t, ErrInsufficientFee, err)
	assert.Equal(t, 0, mp.Size())

	// Valid transaction
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

	mp.RemoveTransactions([]*Transaction{{ID: "A"}, {ID: "B"}})

	// Stale references remain in the queue, so Size() stays 2
	assert.Equal(t, 2, mp.Size())

	nextBlockTxs := mp.GetTransactionsForBlock(2)
	// Because A and B are removed from txMap, the mempool won't return them
	assert.Len(t, nextBlockTxs, 0)

	// The queue is still full of stale references
	assert.Equal(t, 2, mp.Size())

	mpImpl := mp.(*mempoolImpl)
	mpImpl.Stop()
}

func TestMempoolCapacityEviction(t *testing.T) {
	mp := NewMempool()
	mpImpl := mp.(*mempoolImpl)
	mpImpl.maxSize = 2 // small capacity for testing

	// Add two transactions
	_ = mp.AddTransaction(&Transaction{ID: "A", Fee: 10})
	_ = mp.AddTransaction(&Transaction{ID: "B", Fee: 20})
	assert.Equal(t, 2, len(mpImpl.txMap))

	// Attempt to add a lower-fee transaction -> should be rejected
	err := mp.AddTransaction(&Transaction{ID: "C", Fee: 5})
	assert.Equal(t, ErrMempoolFull, err)
	assert.Equal(t, 2, len(mpImpl.txMap))

	// Add a higher-fee transaction -> should evict the lowest (A with fee=10)
	err = mp.AddTransaction(&Transaction{ID: "D", Fee: 30})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(mpImpl.txMap))

	// Confirm "A" was evicted, "B" and "D" remain
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

	// Lower the TTL so it expires quickly
	mpImpl.ttlSeconds = 1

	// Add a transaction
	err := mp.AddTransaction(&Transaction{ID: "X", Fee: 10})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mpImpl.txMap))

	// Sleep so TTL can expire
	time.Sleep(2 * time.Second)

	// Force a manual eviction call (the background goroutine also does this,
	// but we want a deterministic test).
	mpImpl.evictExpired()

	assert.Equal(t, 0, len(mpImpl.txMap))

	mpImpl.Stop()
}
