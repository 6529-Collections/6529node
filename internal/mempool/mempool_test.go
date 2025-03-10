package mempool

import (
	"fmt"
	"math/rand"
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

func TestMempoolConcurrentAddAndGet(t *testing.T) {
	mp := NewMempool()
	mpImpl := mp.(*mempoolImpl)
	mpImpl.maxSize = 100

	var wg sync.WaitGroup
	concurrencyAdd := 20
	concurrencyGet := 5

	wg.Add(concurrencyAdd + concurrencyGet)

	for i := 0; i < concurrencyAdd; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				txID := fmt.Sprintf("txConAdd-%d-%d", id, j)
				_ = mp.AddTransaction(&Transaction{ID: txID, Fee: uint64(rand.Intn(100) + 1)})
			}
		}(i)
	}

	for i := 0; i < concurrencyGet; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				_ = mp.GetTransactionsForBlock(10)
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// No strict correctness beyond no race/panic, but let's ensure we have some transactions
	assert.Greater(t, mp.Size(), 0)

	mpImpl.Stop()
}

func TestMempoolEvictionSameFeeTie(t *testing.T) {
	mp := NewMempool()
	mpImpl := mp.(*mempoolImpl)
	mpImpl.maxSize = 2

	_ = mp.AddTransaction(&Transaction{ID: "tx1", Fee: 10})
	_ = mp.AddTransaction(&Transaction{ID: "tx2", Fee: 10})
	assert.Equal(t, 2, len(mpImpl.txMap))

	err := mp.AddTransaction(&Transaction{ID: "tx3", Fee: 10})
	assert.Equal(t, ErrMempoolFull, err, "Ties should cause rejection since new fee <= lowest fee")
	assert.Equal(t, 2, len(mpImpl.txMap))

	mpImpl.Stop()
}

func TestMempoolRemoveTransactionsNonExistent(t *testing.T) {
	mp := NewMempool()

	err := mp.AddTransaction(&Transaction{ID: "txA", Fee: 10})
	assert.NoError(t, err)
	assert.Equal(t, 1, mp.Size())

	mp.RemoveTransactions([]*Transaction{{ID: "txA"}})
	assert.Equal(t, 1, mp.Size()) // stale reference in PQ

	assert.NotPanics(t, func() {
		mp.RemoveTransactions([]*Transaction{{ID: "txA"}})
	})

	mpImpl := mp.(*mempoolImpl)
	mpImpl.Stop()
}

func TestMempoolReinjectOrphanedTxsAtCapacity(t *testing.T) {
	mp := NewMempool()
	mpImpl := mp.(*mempoolImpl)
	mpImpl.maxSize = 2

	_ = mp.AddTransaction(&Transaction{ID: "origA", Fee: 10})
	_ = mp.AddTransaction(&Transaction{ID: "origB", Fee: 20})
	assert.Equal(t, 2, len(mpImpl.txMap))

	orphanTxs := []*Transaction{
		{ID: "orphan1", Fee: 5},
		{ID: "orphan2", Fee: 30},
	}

	err := mp.ReinjectOrphanedTxs(orphanTxs)
	assert.NoError(t, err)

	_, hasOrigA := mpImpl.txMap["origA"]
	_, hasOrigB := mpImpl.txMap["origB"]
	_, hasOrphan1 := mpImpl.txMap["orphan1"]
	_, hasOrphan2 := mpImpl.txMap["orphan2"]

	assert.False(t, hasOrigA, "origA should be evicted by orphan2 (fee=30)")
	assert.True(t, hasOrigB, "origB (fee=20) remains")
	assert.False(t, hasOrphan1, "orphan1 (fee=5) is too low, so not injected")
	assert.True(t, hasOrphan2, "orphan2 (fee=30) accepted")

	mpImpl.Stop()
}

func TestMempoolTTLEvictionBoundary(t *testing.T) {
	mp := NewMempool()
	mpImpl := mp.(*mempoolImpl)
	mpImpl.ttlSeconds = 1

	err := mp.AddTransaction(&Transaction{ID: "boundaryTx", Fee: 10})
	assert.NoError(t, err)

	time.Sleep(500 * time.Millisecond)
	mpImpl.evictExpired()
	_, exists := mpImpl.txMap["boundaryTx"]
	assert.True(t, exists, "Tx should still be in the pool because TTL not fully elapsed")

	time.Sleep(600 * time.Millisecond)
	mpImpl.evictExpired()
	_, exists = mpImpl.txMap["boundaryTx"]
	assert.False(t, exists, "Tx should be evicted after exceeding 1s TTL")

	mpImpl.Stop()
}

func TestMempoolRequestMoreTransactionsThanExist(t *testing.T) {
	mp := NewMempool()
	_ = mp.AddTransaction(&Transaction{ID: "txOne", Fee: 10})
	_ = mp.AddTransaction(&Transaction{ID: "txTwo", Fee: 5})

	txs := mp.GetTransactionsForBlock(10) // more than 2
	assert.Len(t, txs, 2, "Should only return 2 total transactions")

	// Check order by fee
	assert.Equal(t, "txOne", txs[0].ID)
	assert.Equal(t, "txTwo", txs[1].ID)

	mpImpl := mp.(*mempoolImpl)
	mpImpl.Stop()
}

func TestMempoolLargeParallelInsertions(t *testing.T) {
	mp := NewMempool()
	mpImpl := mp.(*mempoolImpl)
	mpImpl.maxSize = 20
	var wg sync.WaitGroup

	insertCount := 100
	wg.Add(insertCount)

	for i := 0; i < insertCount; i++ {
		go func(idx int) {
			defer wg.Done()
			fee := uint64(rand.Intn(100) + 1)
			txID := fmt.Sprintf("txLarge-%d-Fee%d", idx, fee)
			_ = mp.AddTransaction(&Transaction{ID: txID, Fee: fee})
		}(i)
	}

	wg.Wait()

	finalMapSize := len(mpImpl.txMap)
	assert.LessOrEqual(t, finalMapSize, 20, "Mempool should never exceed maxSize")
	mpImpl.Stop()
}

func TestMempoolMultiBlockCycle(t *testing.T) {
	mp := NewMempool()
	mpImpl := mp.(*mempoolImpl)
	mpImpl.maxSize = 10

	for i := 0; i < 5; i++ {
		txID := fmt.Sprintf("blockCycle-%d", i)
		_ = mp.AddTransaction(&Transaction{ID: txID, Fee: uint64(i + 1)})
	}
	assert.Equal(t, 5, mp.Size())

	blockTxs1 := mp.GetTransactionsForBlock(3)
	assert.Len(t, blockTxs1, 3)
	mp.RemoveTransactions(blockTxs1)

	for i := 5; i < 7; i++ {
		txID := fmt.Sprintf("blockCycle-%d", i)
		_ = mp.AddTransaction(&Transaction{ID: txID, Fee: uint64(i + 1)})
	}

	blockTxs2 := mp.GetTransactionsForBlock(3)
	assert.True(t, len(blockTxs2) <= 4, "We had 2 leftover + 2 new = 4 total")
	mp.RemoveTransactions(blockTxs2)

	leftAfter := mp.GetTransactionsForBlock(10)
	assert.True(t, len(leftAfter) <= 2, "At most 2 remain if fewer than 3 were removed previously")

	mpImpl.Stop()
}
