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

// Transaction can be extended later to include real signature fields, etc.
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

// We add fields for capacity (maxSize) and TTL (ttlSeconds).
type mempoolImpl struct {
	mu         sync.RWMutex
	txMap      map[string]*Transaction
	pq         txPriorityQueue
	baseFee    uint64
	maxSize    int
	ttlSeconds int
	quitCh     chan struct{}
}

// Default constructor now includes capacity & TTL with some example defaults.
// We also launch a goroutine that periodically evicts expired transactions.
func NewMempool() Mempool {
	m := &mempoolImpl{
		txMap:      make(map[string]*Transaction),
		pq:         make(txPriorityQueue, 0),
		baseFee:    1,     // minimal fee
		maxSize:    20000, // example default capacity
		ttlSeconds: 86400, // default 24 hours
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

	// Mark arrival time if not already set
	if tx.arrivalTime.IsZero() {
		tx.arrivalTime = time.Now()
	}

	zap.L().Info("AddTransaction called", zap.String("txID", tx.ID), zap.Uint64("fee", tx.Fee))

	if err := m.validateTransaction(tx); err != nil {
		zap.L().Warn("Transaction validation failed",
			zap.Error(err),
			zap.String("txID", tx.ID),
			zap.Uint64("fee", tx.Fee),
		)
		return err
	}

	// Check capacity. If we're at max size, see if we should evict or reject.
	if len(m.txMap) >= m.maxSize {
		lowestTx, lowestID := m.findLowestFeeTx()
		if lowestTx == nil || tx.Fee <= lowestTx.Fee {
			zap.L().Warn("Mempool is full, rejecting transaction",
				zap.String("txID", tx.ID),
				zap.Uint64("fee", tx.Fee),
			)
			return ErrMempoolFull
		}
		// Evict the lowest-fee tx
		delete(m.txMap, lowestID)
		zap.L().Info("Evicting lowest-fee transaction",
			zap.String("evictedID", lowestID),
			zap.Uint64("evictedFee", lowestTx.Fee),
		)
		// Stale references remain in the pq; they'll be skipped on pop
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

	// Re-push all popped items so the queue is unchanged overall.
	// We'll only return items that remain in txMap (non-stale).
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
	// Lazy removal means the priority queue can have stale items,
	// so we just return the length of the priority queue.
	return len(m.pq)
}

func (m *mempoolImpl) ReinjectOrphanedTxs(txs []*Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	zap.L().Info("ReinjectOrphanedTxs called", zap.Int("count", len(txs)))
	for _, tx := range txs {
		// Re-validate under mempool constraints
		if tx.arrivalTime.IsZero() {
			tx.arrivalTime = time.Now()
		}
		if err := m.validateTransaction(tx); err != nil {
			zap.L().Warn("Orphaned tx invalid on re-inject",
				zap.Error(err),
				zap.String("txID", tx.ID),
				zap.Uint64("fee", tx.Fee),
			)
			continue
		}
		// Check capacity
		if len(m.txMap) >= m.maxSize {
			lowestTx, lowestID := m.findLowestFeeTx()
			if lowestTx == nil || tx.Fee <= lowestTx.Fee {
				zap.L().Warn("Mempool full, orphan re-inject rejected",
					zap.String("txID", tx.ID),
					zap.Uint64("fee", tx.Fee),
				)
				continue
			}
			delete(m.txMap, lowestID)
			zap.L().Info("Evicting lowest-fee tx for re-inject",
				zap.String("evictedID", lowestID),
				zap.Uint64("evictedFee", lowestTx.Fee),
			)
		}
		m.txMap[tx.ID] = tx
		heap.Push(&m.pq, tx)
	}
	return nil
}

// Stop the TTL loop if needed.
func (m *mempoolImpl) Stop() {
	close(m.quitCh)
}

// Periodically evict expired transactions (based on arrivalTime + ttlSeconds).
func (m *mempoolImpl) runTTLEvictionLoop() {
	ticker := time.NewTicker(10 * time.Second) // runs every 10s, adjust as needed
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

// Remove from txMap any transaction whose arrivalTime + ttlSeconds is in the past.
func (m *mempoolImpl) evictExpired() {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	for id, tx := range m.txMap {
		age := now.Sub(tx.arrivalTime)
		if age > time.Duration(m.ttlSeconds)*time.Second {
			delete(m.txMap, id)
			zap.L().Info("Evicting expired transaction",
				zap.String("txID", id),
				zap.Uint64("fee", tx.Fee),
				zap.Duration("age", age),
			)
		}
	}
}

// validateTransaction enforces basic checks: format, signature, fee >= baseFee.
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
// Extend with real cryptographic verification if needed.
func stubSignatureValid(tx *Transaction) bool {
	return tx.ID != "invalid-sig"
}

// findLowestFeeTx is an O(n) scan over txMap to get the transaction with the lowest fee.
// This is a simple solution but might be improved if your throughput is large.
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

// txPriorityQueue is a max-heap by Fee (higher Fee => higher priority).
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
