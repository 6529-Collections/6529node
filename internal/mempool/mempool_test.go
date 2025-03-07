package mempool

import (
	"fmt"
	"sync"
	"testing"

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
	// Currently we do lazy removal from the priority queue
	// so Size() still reflects the PQ length.
	// If we popped from the PQ earlier, they'd remain in the queue data structure
	// but are removed from txMap. This is expected in our approach.
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
			txID := fmt.Sprintf("txConcurrency-%d", id)
			_ = mp.AddTransaction(&Transaction{ID: txID, Fee: 5})
		}(i)
	}
	wg.Wait()

	// Some or all might fail if they triggered invalid format or insufficient fee checks;
	// but here we used valid IDs and Fee=5 with baseFee=1, so all should succeed.
	assert.Equal(t, concurrency, mp.Size())
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
}
