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
		baseFee: 1}
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
	if len(m.pq) == 0 {
		return nil
	}

	var popped []*Transaction
	for i := 0; i < maxCount && m.pq.Len() > 0; i++ {
		top := heap.Pop(&m.pq).(*Transaction)
		popped = append(popped, top)
	}

	for _, tx := range popped {
		heap.Push(&m.pq, tx)
	}

	return popped
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
	// For demonstration: treat "invalid-sig" as a marker for a bad signature
	if tx.ID == "invalid-sig" {
		return false
	}
	return true
}

type txPriorityQueue []*Transaction

func (pq txPriorityQueue) Len() int {
	return len(pq)
}

func (pq txPriorityQueue) Less(i, j int) bool {
	// higher fee = higher priority
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
