package mempool

import (
	"container/heap"
	"sync"

	"go.uber.org/zap"
)

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
	mu    sync.RWMutex
	txMap map[string]*Transaction
	pq    txPriorityQueue
}

func NewMempool() Mempool {
	zap.L().Info("Creating new mempool")
	return &mempoolImpl{
		txMap: make(map[string]*Transaction),
		pq:    make(txPriorityQueue, 0),
	}
}

func (m *mempoolImpl) AddTransaction(tx *Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	zap.L().Info("AddTransaction called", zap.String("txID", tx.ID))
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
	return nil
}

type txPriorityQueue []*Transaction

func (pq txPriorityQueue) Len() int {
	return len(pq)
}

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
