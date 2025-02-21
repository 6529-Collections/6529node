package mempool

import (
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
	// Basic placeholder fields for now
	transactions []*Transaction
}

func NewMempool() Mempool {
	zap.L().Info("Creating new mempool")
	return &mempoolImpl{}
}

func (m *mempoolImpl) AddTransaction(tx *Transaction) error {
	zap.L().Info("AddTransaction called", zap.String("txID", tx.ID))
	// No-op for now
	m.transactions = append(m.transactions, tx)
	return nil
}

func (m *mempoolImpl) GetTransactionsForBlock(maxCount int) []*Transaction {
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
	zap.L().Info("RemoveTransactions called", zap.Int("count", len(txs)))
	// No-op for now
}

func (m *mempoolImpl) Size() int {
	return len(m.transactions)
}

func (m *mempoolImpl) ReinjectOrphanedTxs(txs []*Transaction) error {
	zap.L().Info("ReinjectOrphanedTxs called", zap.Int("count", len(txs)))
	// No-op for now
	return nil
}
