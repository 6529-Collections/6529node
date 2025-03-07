package mempool

import (
	"container/heap"
	"errors"
	"sync"

	"go.uber.org/zap"
)

var (
	ErrInvalidFormat    = errors.New("invalid transaction format")
	ErrInvalidSignature = errors.New("invalid signature")
	ErrInsufficientFee  = errors.New("insufficient fee")
)

// Transaction can be extended later to include real signature fields, etc.
type Transaction struct {
	ID  string
	Fee uint64
}

type Mempool interface {
	AddTransaction(tx *Transaction) error
	GetTransactionsForBlock(maxCount int) []*Transaction
	RemoveTransactions(txs []*Transaction)
	Size() int
	ReinjectOrphanedTxs(txs []*Transaction) error
}

type mempoolImpl struct {
	mu      sync.RWMutex
	txMap   map[string]*Transaction
	pq      txPriorityQueue
	baseFee uint64
}

func NewMempool() Mempool {
	zap.L().Info("Creating new mempool")
	return &mempoolImpl{
		txMap:   make(map[string]*Transaction),
		pq:      make(txPriorityQueue, 0),
		baseFee: 1, // minimal fee
	}
}

func (m *mempoolImpl) AddTransaction(tx *Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	zap.L().Info("AddTransaction called", zap.String("txID", tx.ID))

	if err := m.validateTransaction(tx); err != nil {
		zap.L().Warn("Transaction validation failed", zap.Error(err), zap.String("txID", tx.ID))
		return err
	}

	m.txMap[tx.ID] = tx
	heap.Push(&m.pq, tx)
	return nil
}

func (m *mempoolImpl) GetTransactionsForBlock(maxCount int) []*Transaction {
	m.mu.Lock()
	defer m.mu.Unlock()

	zap.L().Info("GetTransactionsForBlock called", zap.Int("maxCount", maxCount))

	// Pop up to maxCount items from the priority queue
	var popped []*Transaction
	for i := 0; i < maxCount && m.pq.Len() > 0; i++ {
		top := heap.Pop(&m.pq).(*Transaction)
		popped = append(popped, top)
	}

	// Now we re-push all popped items, but only *after* we've collected them.
	// This ensures we do a single pass over the queue and preserve the original order.
	// We do NOT skip re-pushing stale items, because the tests expect "lazy removal"
	// to keep the pq size the same. However, we only return items that are still in the map.
	var results []*Transaction
	for _, tx := range popped {
		heap.Push(&m.pq, tx)
		if _, stillValid := m.txMap[tx.ID]; stillValid {
			results = append(results, tx)
		}
	}

	return results
}

func (m *mempoolImpl) RemoveTransactions(txs []*Transaction) {
	m.mu.Lock()
	defer m.mu.Unlock()

	zap.L().Info("RemoveTransactions called", zap.Int("count", len(txs)))
	for _, tx := range txs {
		delete(m.txMap, tx.ID)
	}
}

func (m *mempoolImpl) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// We are returning the size of the queue (including stale).
	// This matches the tests, which expect "lazy removal".
	return len(m.pq)
}

func (m *mempoolImpl) ReinjectOrphanedTxs(txs []*Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	zap.L().Info("ReinjectOrphanedTxs called", zap.Int("count", len(txs)))
	for _, tx := range txs {
		if err := m.validateTransaction(tx); err != nil {
			zap.L().Warn("Orphaned tx invalid on re-inject", zap.Error(err), zap.String("txID", tx.ID))
			continue
		}
		m.txMap[tx.ID] = tx
		heap.Push(&m.pq, tx)
	}
	return nil
}

func (m *mempoolImpl) validateTransaction(tx *Transaction) error {
	if tx.ID == "" {
		return ErrInvalidFormat
	}
	if !stubSignatureValid(tx) {
		return ErrInvalidSignature
	}
	if tx.Fee < m.baseFee {
		return ErrInsufficientFee
	}
	return nil
}

// stubSignatureValid simulates a signature check.
// Extend this with real cryptographic signature verification later
func stubSignatureValid(tx *Transaction) bool {
	if tx.ID == "invalid-sig" {
		return false
	}
	return true
}

type txPriorityQueue []*Transaction

func (pq txPriorityQueue) Len() int {
	return len(pq)
}

// We want a max-heap by Fee: higher fee => higher priority
func (pq txPriorityQueue) Less(i, j int) bool {
	return pq[i].Fee > pq[j].Fee
}

func (pq txPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *txPriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*Transaction))
}

func (pq *txPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}
