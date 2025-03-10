package mempool

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	ErrInvalidFormat    = errors.New("invalid transaction format")
	ErrInvalidSignature = errors.New("invalid signature")
	ErrInsufficientFee  = errors.New("insufficient fee")
	ErrMempoolFull      = errors.New("mempool is full")
)

type Transaction struct {
	ID          string
	Fee         uint64
	arrivalTime time.Time
}

type Mempool interface {
	AddTransaction(tx *Transaction) error
	GetTransactionsForBlock(maxCount int) []*Transaction
	RemoveTransactions(txs []*Transaction)
	Size() int
	ReinjectOrphanedTxs(txs []*Transaction) error
}

type mempoolImpl struct {
	mu         sync.RWMutex
	txMap      map[string]*Transaction
	pq         txPriorityQueue
	baseFee    uint64
	maxSize    int
	ttlSeconds int
	quitCh     chan struct{}
}

func NewMempool() Mempool {
	m := &mempoolImpl{
		txMap:      make(map[string]*Transaction),
		pq:         make(txPriorityQueue, 0),
		baseFee:    1,
		maxSize:    20000,
		ttlSeconds: 86400,
		quitCh:     make(chan struct{}),
	}
	zap.L().Info("Creating new mempool",
		zap.Int("maxSize", m.maxSize),
		zap.Int("ttlSeconds", m.ttlSeconds),
	)
	go m.runTTLEvictionLoop()
	return m
}

func (m *mempoolImpl) AddTransaction(tx *Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tx.arrivalTime.IsZero() {
		tx.arrivalTime = time.Now()
	}

	zap.L().Info("AddTransaction called", zap.String("txID", tx.ID), zap.Uint64("fee", tx.Fee))

	if err := m.validateTransaction(tx); err != nil {
		zap.L().Warn("Transaction validation failed", zap.Error(err), zap.String("txID", tx.ID), zap.Uint64("fee", tx.Fee))
		return err
	}
	if len(m.txMap) >= m.maxSize {
		lowestTx, lowestID := m.findLowestFeeTx()
		if lowestTx == nil || tx.Fee <= lowestTx.Fee {
			zap.L().Warn("Mempool is full, rejecting transaction", zap.String("txID", tx.ID), zap.Uint64("fee", tx.Fee))
			return ErrMempoolFull
		}
		delete(m.txMap, lowestID)
		zap.L().Info("Evicting lowest-fee transaction", zap.String("evictedID", lowestID), zap.Uint64("evictedFee", lowestTx.Fee))
	}
	m.txMap[tx.ID] = tx
	heap.Push(&m.pq, tx)
	return nil
}

func (m *mempoolImpl) GetTransactionsForBlock(maxCount int) []*Transaction {
	m.mu.Lock()
	defer m.mu.Unlock()

	zap.L().Info("GetTransactionsForBlock called", zap.Int("maxCount", maxCount))

	var popped []*Transaction
	for i := 0; i < maxCount && m.pq.Len() > 0; i++ {
		top := heap.Pop(&m.pq).(*Transaction)
		popped = append(popped, top)
	}
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
	return len(m.pq)
}

func (m *mempoolImpl) ReinjectOrphanedTxs(txs []*Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	zap.L().Info("ReinjectOrphanedTxs called", zap.Int("count", len(txs)))
	for _, tx := range txs {
		if tx.arrivalTime.IsZero() {
			tx.arrivalTime = time.Now()
		}
		if err := m.validateTransaction(tx); err != nil {
			zap.L().Warn("Orphaned tx invalid on re-inject", zap.Error(err), zap.String("txID", tx.ID), zap.Uint64("fee", tx.Fee))
			continue
		}
		if len(m.txMap) >= m.maxSize {
			lowestTx, lowestID := m.findLowestFeeTx()
			if lowestTx == nil || tx.Fee <= lowestTx.Fee {
				zap.L().Warn("Mempool full, orphan re-inject rejected", zap.String("txID", tx.ID), zap.Uint64("fee", tx.Fee))
				continue
			}
			delete(m.txMap, lowestID)
			zap.L().Info("Evicting lowest-fee tx for re-inject", zap.String("evictedID", lowestID), zap.Uint64("evictedFee", lowestTx.Fee))
		}
		m.txMap[tx.ID] = tx
		heap.Push(&m.pq, tx)
	}
	return nil
}

func (m *mempoolImpl) Stop() {
	close(m.quitCh)
}

func (m *mempoolImpl) runTTLEvictionLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.evictExpired()
		case <-m.quitCh:
			return
		}
	}
}

func (m *mempoolImpl) evictExpired() {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, tx := range m.txMap {
		age := now.Sub(tx.arrivalTime)
		if age > time.Duration(m.ttlSeconds)*time.Second {
			delete(m.txMap, id)
			zap.L().Info("Evicting expired transaction", zap.String("txID", id), zap.Uint64("fee", tx.Fee), zap.Duration("age", age))
		}
	}
}

func (m *mempoolImpl) validateTransaction(tx *Transaction) error {
	if tx.ID == "" {
		return ErrInvalidFormat
	}
	if !isSignatureValid(tx) {
		return ErrInvalidSignature
	}
	if tx.Fee < m.baseFee {
		return ErrInsufficientFee
	}
	return nil
}

func isSignatureValid(tx *Transaction) bool {
	// TODO Replace with actual signature validation
	return tx.ID != "invalid-sig"
}

func (m *mempoolImpl) findLowestFeeTx() (*Transaction, string) {
	var lowest *Transaction
	var lowestID string
	for id, tx := range m.txMap {
		if lowest == nil || tx.Fee < lowest.Fee {
			lowest = tx
			lowestID = id
		}
	}
	return lowest, lowestID
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
	*pq = old[:n-1]
	return x
}
