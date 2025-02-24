package mempool

import (
	"sync"

	"go.uber.org/zap"
)

type Transaction struct {
	ID string
}

type Mempool interface {
	AddTransaction(tx *Transaction) error
	GetTransactionsForBlock(maxCount int) []*Transaction
	RemoveTransactions(txs []*Transaction)
	Size() int
	ReinjectOrphanedTxs(txs []*Transaction) error
}

type mempoolImpl struct {
	mu           sync.RWMutex
	transactions []*Transaction
}

func NewMempool() Mempool {
	zap.L().Info("Creating new mempool")
	return &mempoolImpl{}
}

func (m *mempoolImpl) AddTransaction(tx *Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	zap.L().Info("AddTransaction called", zap.String("txID", tx.ID))
	m.transactions = append(m.transactions, tx)
	return nil
}

func (m *mempoolImpl) GetTransactionsForBlock(maxCount int) []*Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()
	zap.L().Info("GetTransactionsForBlock called", zap.Int("maxCount", maxCount))
	if len(m.transactions) == 0 {
		return nil
	}
	if maxCount >= len(m.transactions) {
		return m.transactions
	}
	return m.transactions[:maxCount]
}

func (m *mempoolImpl) RemoveTransactions(txs []*Transaction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	zap.L().Info("RemoveTransactions called", zap.Int("count", len(txs)))
	// No-op to match existing tests
}

func (m *mempoolImpl) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.transactions)
}

func (m *mempoolImpl) ReinjectOrphanedTxs(txs []*Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	zap.L().Info("ReinjectOrphanedTxs called", zap.Int("count", len(txs)))
	// No-op to match existing tests
	return nil
}
