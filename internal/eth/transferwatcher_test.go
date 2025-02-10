package eth

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/6529-Collections/6529node/internal/eth/mocks"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

type mockSubscription struct {
	mock.Mock
}

func (m *mockSubscription) Unsubscribe() {
	m.Called()
}
func (m *mockSubscription) Err() <-chan error {
	return nil
}

type InMemoryBlockHashDb struct {
	mu    sync.Mutex
	store map[uint64]common.Hash
}

func NewInMemoryBlockHashDb() *InMemoryBlockHashDb {
	return &InMemoryBlockHashDb{store: make(map[uint64]common.Hash)}
}

func (db *InMemoryBlockHashDb) GetHash(blockNumber uint64) (common.Hash, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	h, ok := db.store[blockNumber]
	return h, ok
}

func (db *InMemoryBlockHashDb) SetHash(blockNumber uint64, hash common.Hash) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.store[blockNumber] = hash
	return nil
}

func (db *InMemoryBlockHashDb) RevertFromBlock(blockNumber uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for k := range db.store {
		if k >= blockNumber {
			delete(db.store, k)
		}
	}
	return nil
}

func makeHeader(num uint64, _ common.Hash) *types.Header {
	return &types.Header{
		Number: big.NewInt(int64(num)),
		Extra:  []byte{},
	}
}
func makeRealisticHeader(num uint64, parent common.Hash) *types.Header {
	return &types.Header{
		ParentHash: parent,
		Number:     big.NewInt(int64(num)),
		GasLimit:   8_000_000,
		Time:       10_000_000 + num,
		Extra:      []byte("test block"),
	}
}
func makeHash(prefix byte) common.Hash {
	h := common.Hash{}
	h[0] = prefix
	return h
}

var zeroHash = common.Hash{}

func init() {
	zap.ReplaceGlobals(zap.NewExample())
}

func TestDefaultTokensTransfersWatcher(t *testing.T) {
	t.Run("TestGroupLogsByBlock", testGroupLogsByBlock)
	t.Run("TestWatchTransfersSimplePolling", testWatchTransfersSimplePolling)
	t.Run("TestWatchTransfersSubscription", testWatchTransfersSubscription)
	t.Run("TestReorgDetected", testReorgDetected)
	t.Run("TestReorgDuringCheckAndHandle", testReorgDuringCheckAndHandle)
	t.Run("TestPollingErrorAndRecovery", testPollingErrorAndRecovery)
	t.Run("TestCancelContextMidway", testCancelContextMidway)
	t.Run("TestLargeBlockRange", testLargeBlockRange)
	t.Run("TestWatchTransfers_SaleDetectionSuccess", testWatchTransfersSaleDetectionSuccess)
	t.Run("TestWatchTransfers_SaleDetectionError", testWatchTransfersSaleDetectionError)
}

func testGroupLogsByBlock(t *testing.T) {
	logs1 := tokens.TokenTransfer{BlockNumber: 100, TransactionIndex: 1, LogIndex: 1}
	logs2 := tokens.TokenTransfer{BlockNumber: 100, TransactionIndex: 1, LogIndex: 2}
	logs3 := tokens.TokenTransfer{BlockNumber: 101, TransactionIndex: 0, LogIndex: 0}

	decoded := []tokens.TokenTransfer{
		logs1, logs2, logs3,
	}

	groups := groupLogsByBlock(decoded)
	assert.Len(t, groups, 2)
	assert.Equal(t, 2, len(groups[100]))
	assert.Equal(t, 1, len(groups[101]))
}

func testWatchTransfersSimplePolling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := mocks.NewEthClient(t)
	mockBlockDb := mocks.NewBlockHashDb(t)
	mockDecoder := mocks.NewEthTransactionLogsDecoder(t)
	mockSalesDetector := mocks.NewSalesDetector(t)

	watcher := &DefaultTokensTransfersWatcher{
		Decoder:      mockDecoder,
		BlockTracker: mockBlockDb,
		SaleDetector: mockSalesDetector,
	}

	mockClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Return(nil, errors.New("no wss support")).
		Maybe()

	headerAt12 := makeHeader(12, makeHash(0x12))
	mockClient.On("HeaderByNumber", mock.Anything, (*big.Int)(nil)).
		Return(headerAt12, nil).
		Maybe()

	sampleLogs := []types.Log{
		{BlockNumber: 10, Index: 0, TxIndex: 0, Address: common.HexToAddress("0xABCDEF")},
		{BlockNumber: 12, Index: 1, TxIndex: 0, Address: common.HexToAddress("0xABCDEF")},
	}
	filterQuery := ethereum.FilterQuery{
		FromBlock: big.NewInt(10),
		ToBlock:   big.NewInt(12),
		Addresses: []common.Address{common.HexToAddress("0xABCDEF")},
	}
	mockClient.On("FilterLogs", mock.Anything, filterQuery).
		Return(sampleLogs, nil).
		Maybe()

	t10 := tokens.TokenTransfer{BlockNumber: 10, LogIndex: 0, TransactionIndex: 0}
	t12 := tokens.TokenTransfer{BlockNumber: 12, LogIndex: 1, TransactionIndex: 0}
	mockDecoder.On("Decode", sampleLogs).
		Return([][]tokens.TokenTransfer{{t10, t12}}).
		Maybe()

	// For simplicity, we can have the SaleDetector return an empty classification for each TX
	mockSalesDetector.On("DetectIfSale", mock.Anything, mock.AnythingOfType("common.Hash"), mock.Anything).
		Return(map[int]tokens.TransferType{
			0: tokens.SEND,
			1: tokens.SEND,
		}, nil).
		Maybe()

	safeHash := makeHash(0xAB)
	safeHeader := makeHeader(9999, safeHash)

	mockClient.On("HeaderByNumber", mock.Anything, mock.AnythingOfType("*big.Int")).
		Return(safeHeader, nil).
		Maybe()
	mockBlockDb.On("GetHash", mock.AnythingOfType("uint64")).
		Return(common.Hash{}, false).
		Maybe()
	mockBlockDb.On("SetHash", mock.AnythingOfType("uint64"), mock.AnythingOfType("common.Hash")).
		Return(nil).
		Maybe()

	transfersChan := make(chan []tokens.TokenTransfer, 10)
	latestBlockChan := make(chan uint64, 10)

	doneCh := make(chan struct{})
	go func() {
		err := watcher.WatchTransfers(ctx, mockClient,
			[]string{"0xABCDEF"},
			10,
			transfersChan, latestBlockChan,
		)
		assert.NoError(t, err)
		close(doneCh)
	}()

	var allTransfers []tokens.TokenTransfer
	for i := 0; i < 2; i++ {
		select {
		case batch := <-transfersChan:
			allTransfers = append(allTransfers, batch...)
		case <-time.After(1 * time.Second):
			t.Fatal("Did not receive expected transfers in time")
		}
	}
	assert.Len(t, allTransfers, 2)
	assert.Equal(t, uint64(10), allTransfers[0].BlockNumber)
	assert.Equal(t, uint64(12), allTransfers[1].BlockNumber)

	select {
	case latest := <-latestBlockChan:
		assert.Equal(t, uint64(12), latest)
	case <-time.After(1 * time.Second):
		t.Fatal("Did not receive latest block signal in time")
	}

	cancel()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Watcher did not stop after context cancellation")
	}
}

func testWatchTransfersSubscription(t *testing.T) {
	zap.ReplaceGlobals(zap.NewExample())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := mocks.NewEthClient(t)
	mockDecoder := mocks.NewEthTransactionLogsDecoder(t)
	blockDb := NewInMemoryBlockHashDb()
	mockSalesDetector := mocks.NewSalesDetector(t)

	watcher := &DefaultTokensTransfersWatcher{
		Decoder:      mockDecoder,
		BlockTracker: blockDb,
		SaleDetector: mockSalesDetector,
	}

	startBlock := uint64(50)
	block50 := makeRealisticHeader(50, zeroHash)
	block50Hash := block50.Hash()

	block51 := makeRealisticHeader(51, block50Hash)
	block51Hash := block51.Hash()

	block52 := makeRealisticHeader(52, block51Hash)

	mockClient.On("HeaderByNumber", mock.Anything, (*big.Int)(nil)).
		Return(block50, nil).
		Maybe()

	mockSub := &mockSubscription{}
	mockSub.On("Unsubscribe").Return().Once()

	mockClient.On("SubscribeNewHead", mock.Anything, mock.AnythingOfType("chan<- *types.Header")).
		Return(func(ctx context.Context, c chan<- *types.Header) ethereum.Subscription {
			go func() {
				time.Sleep(200 * time.Millisecond)
				c <- block51
				time.Sleep(200 * time.Millisecond)
				c <- block52
			}()
			return mockSub
		}, nil).
		Maybe()

	logs50 := []types.Log{
		{BlockNumber: 50, Index: 0, TxIndex: 1, Address: common.HexToAddress("0xABCDEF")},
	}
	logs51 := []types.Log{
		{BlockNumber: 51, Index: 5, TxIndex: 0, Address: common.HexToAddress("0xABCDEF")},
	}
	logs52 := []types.Log{
		{BlockNumber: 52, Index: 7, TxIndex: 1, Address: common.HexToAddress("0xABCDEF")},
	}

	getLogsInRange := func(from, to uint64) []types.Log {
		var out []types.Log
		if from <= 50 && 50 <= to {
			out = append(out, logs50...)
		}
		if from <= 51 && 51 <= to {
			out = append(out, logs51...)
		}
		if from <= 52 && 52 <= to {
			out = append(out, logs52...)
		}
		return out
	}

	mockClient.On("FilterLogs", mock.Anything, mock.AnythingOfType("ethereum.FilterQuery")).
		Return(func(_ context.Context, q ethereum.FilterQuery) []types.Log {
			f, t := q.FromBlock.Uint64(), q.ToBlock.Uint64()
			return getLogsInRange(f, t)
		}, nil).
		Maybe()

	mockDecoder.On("Decode", mock.Anything).
		Return(func(all []types.Log) [][]tokens.TokenTransfer {
			if len(all) == 0 {
				return nil
			}
			byBlock := make(map[uint64][]tokens.TokenTransfer)
			for _, lg := range all {
				tr := tokens.TokenTransfer{
					BlockNumber:      lg.BlockNumber,
					TransactionIndex: uint64(lg.TxIndex),
					LogIndex:         uint64(lg.Index),
				}
				byBlock[lg.BlockNumber] = append(byBlock[lg.BlockNumber], tr)
			}
			var out [][]tokens.TokenTransfer
			for _, btrs := range byBlock {
				out = append(out, btrs)
			}
			return out
		}).
		Maybe()

	mockSalesDetector.On("DetectIfSale", mock.Anything, mock.AnythingOfType("common.Hash"), mock.Anything).
		Return(map[int]tokens.TransferType{
			0: tokens.SEND,
		}, nil).
		Maybe()

	mockClient.On("HeaderByNumber", mock.Anything, mock.MatchedBy(func(b *big.Int) bool {
		return b != nil && b.Uint64() == 50
	})).Return(block50, nil).Maybe()

	mockClient.On("HeaderByNumber", mock.Anything, mock.MatchedBy(func(b *big.Int) bool {
		return b != nil && b.Uint64() == 51
	})).Return(block51, nil).Maybe()

	mockClient.On("HeaderByNumber", mock.Anything, mock.MatchedBy(func(b *big.Int) bool {
		return b != nil && b.Uint64() == 52
	})).Return(block52, nil).Maybe()

	transfersChan := make(chan []tokens.TokenTransfer, 10)
	latestBlockChan := make(chan uint64, 10)

	doneCh := make(chan error)
	go func() {
		err := watcher.WatchTransfers(
			ctx,
			mockClient,
			[]string{"0xABCDEF"},
			startBlock,
			transfersChan,
			latestBlockChan,
		)
		doneCh <- err
	}()

	var allTransfers []tokens.TokenTransfer
	for i := 0; i < 3; i++ {
		select {
		case batch := <-transfersChan:
			allTransfers = append(allTransfers, batch...)
		case <-time.After(5 * time.Second):
			t.Fatal("did not receive block's transfers in time")
		}
	}
	assert.Len(t, allTransfers, 3)

	var blockNums []uint64
	for _, tr := range allTransfers {
		blockNums = append(blockNums, tr.BlockNumber)
	}
	assert.ElementsMatch(t, []uint64{50, 51, 52}, blockNums)

	var last uint64
	for i := 0; i < 3; i++ {
		select {
		case lb := <-latestBlockChan:
			last = lb
		case <-time.After(2 * time.Second):
			t.Fatal("did not get latest block update")
		}
	}
	assert.Equal(t, uint64(52), last)

	cancel()
	select {
	case err := <-doneCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Expected nil or context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watcher did not exit after context cancel")
	}
}

func testReorgDetected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := mocks.NewEthClient(t)
	mockBlockDb := mocks.NewBlockHashDb(t)

	mockSales := mocks.NewSalesDetector(t)

	watcher := &DefaultTokensTransfersWatcher{
		BlockTracker: mockBlockDb,
		SaleDetector: mockSales,
	}

	safeHash := makeHash(0xFA)
	safeHeader := makeHeader(9999, safeHash)

	mockBlockDb.On("GetHash", mock.MatchedBy(func(b uint64) bool { return b != 99 })).
		Return(safeHash, true).Maybe()
	mockClient.On("HeaderByNumber", mock.Anything, mock.MatchedBy(func(num *big.Int) bool {
		return num.Uint64() != 99
	})).Return(safeHeader, nil).Maybe()

	oldHash99 := makeHash(0xAA)
	newHash99 := makeHash(0xBB)
	mockBlockDb.On("GetHash", uint64(99)).
		Return(oldHash99, true).Maybe()

	hdr99 := makeHeader(99, newHash99)
	mockClient.On("HeaderByNumber", mock.Anything, big.NewInt(99)).
		Return(hdr99, nil).Maybe()

	mockBlockDb.On("RevertFromBlock", uint64(99)).
		Return(nil).Once()

	err := watcher.processRangeWithPartialReorg(
		ctx,
		mockClient,
		nil,
		100,
		100,
		nil,
		nil,
	)
	if err == nil {
		t.Fatal("Expected a reorg error, but got nil")
	}
	if err != ErrReorgDetected {
		t.Fatalf("Expected ErrReorgDetected, got %v", err)
	}
}

func testReorgDuringCheckAndHandle(t *testing.T) {
	ctx := context.Background()

	mockClient := mocks.NewEthClient(t)
	mockBlockDb := mocks.NewBlockHashDb(t)
	mockSales := mocks.NewSalesDetector(t)

	watcher := &DefaultTokensTransfersWatcher{
		BlockTracker: mockBlockDb,
		SaleDetector: mockSales,
	}

	safeHash := makeHash(0x77)
	safeHeader := makeHeader(9999, safeHash)

	mockBlockDb.On("GetHash", mock.MatchedBy(func(b uint64) bool { return b != 14 })).
		Return(safeHash, true).
		Maybe()
	mockClient.On("HeaderByNumber", mock.Anything, mock.MatchedBy(func(n *big.Int) bool { return n.Uint64() != 14 })).
		Return(safeHeader, nil).
		Maybe()

	dbHash14 := makeHash(0x14)
	chainHash14 := makeHash(0xFF)

	mockBlockDb.On("GetHash", uint64(14)).
		Return(dbHash14, true).
		Maybe()

	header14 := makeHeader(14, chainHash14)
	mockClient.On("HeaderByNumber", mock.Anything, big.NewInt(14)).
		Return(header14, nil).
		Maybe()

	mockBlockDb.On("RevertFromBlock", uint64(14)).
		Return(nil).
		Once()

	err := watcher.checkAndHandleReorg(ctx, mockClient, 15)
	if err == nil {
		t.Fatal("Expected a reorg error, got nil")
	}
	if err != ErrReorgDetected {
		t.Fatalf("Expected ErrReorgDetected, got %v", err)
	}
}

func testPollingErrorAndRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := mocks.NewEthClient(t)
	mockBlockDb := mocks.NewBlockHashDb(t)
	mockDecoder := mocks.NewEthTransactionLogsDecoder(t)
	mockSales := mocks.NewSalesDetector(t)

	watcher := &DefaultTokensTransfersWatcher{
		Decoder:      mockDecoder,
		BlockTracker: mockBlockDb,
		SaleDetector: mockSales,
	}

	mockClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Return(nil, errors.New("no subscription")).
		Once()

	headerAt2 := makeHeader(2, makeHash(0x02))
	mockClient.On("HeaderByNumber", mock.Anything, (*big.Int)(nil)).
		Return(headerAt2, nil).
		Maybe()

	query12 := ethereum.FilterQuery{
		FromBlock: big.NewInt(1),
		ToBlock:   big.NewInt(2),
		Addresses: []common.Address{},
	}
	mockClient.On("FilterLogs", mock.Anything, query12).
		Return(nil, errors.New("temporary node error")).
		Once()

	successLogs := []types.Log{
		{BlockNumber: 1, Index: 0},
		{BlockNumber: 2, Index: 0},
	}
	mockClient.On("FilterLogs", mock.Anything, query12).
		Return(successLogs, nil).
		Once()

	mockClient.On("FilterLogs", mock.Anything, mock.AnythingOfType("ethereum.FilterQuery")).
		Return([]types.Log{}, nil).
		Maybe()

	mockDecoder.On("Decode", successLogs).
		Return([][]tokens.TokenTransfer{
			{
				{BlockNumber: 1, LogIndex: 0},
				{BlockNumber: 2, LogIndex: 0},
			},
		}).
		Once()

	mockSales.On("DetectIfSale", mock.Anything, mock.AnythingOfType("common.Hash"), mock.Anything).
		Return(map[int]tokens.TransferType{
			0: tokens.SEND,
			1: tokens.SEND,
		}, nil).Maybe()

	safeHash := makeHash(0xAB)
	safeHeader := makeHeader(9999, safeHash)

	mockClient.On("HeaderByNumber", mock.Anything, mock.AnythingOfType("*big.Int")).
		Return(safeHeader, nil).
		Maybe()
	mockBlockDb.On("GetHash", mock.AnythingOfType("uint64")).
		Return(common.Hash{}, false).
		Maybe()
	mockBlockDb.On("SetHash", mock.AnythingOfType("uint64"), mock.AnythingOfType("common.Hash")).
		Return(nil).
		Maybe()

	transfersChan := make(chan []tokens.TokenTransfer, 10)
	latestBlockChan := make(chan uint64, 10)

	doneCh := make(chan error)
	go func() {
		err := watcher.WatchTransfers(ctx, mockClient, []string{}, 1, transfersChan, latestBlockChan)
		doneCh <- err
	}()

	var allTransfers []tokens.TokenTransfer
readLoop:
	for {
		select {
		case batch := <-transfersChan:
			allTransfers = append(allTransfers, batch...)
			if len(allTransfers) >= 2 {
				break readLoop
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Did not receive expected transfers for blocks 1 & 2 in time")
		}
	}

	assert.Len(t, allTransfers, 2)
	assert.Equal(t, uint64(1), allTransfers[0].BlockNumber)
	assert.Equal(t, uint64(2), allTransfers[1].BlockNumber)

	select {
	case lb := <-latestBlockChan:
		assert.Equal(t, uint64(2), lb)
	case <-time.After(1 * time.Second):
		t.Fatal("Did not get latestBlockChan update for block 2")
	}

	cancel()
	select {
	case err := <-doneCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Expected nil or context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Watcher did not exit after context cancellation")
	}
}

func testCancelContextMidway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := mocks.NewEthClient(t)
	mockBlockDb := mocks.NewBlockHashDb(t)
	mockDecoder := mocks.NewEthTransactionLogsDecoder(t)
	mockSales := mocks.NewSalesDetector(t)

	watcher := &DefaultTokensTransfersWatcher{
		Decoder:      mockDecoder,
		BlockTracker: mockBlockDb,
		SaleDetector: mockSales,
	}

	mockClient.On("HeaderByNumber", mock.Anything, mock.AnythingOfType("*big.Int")).
		Return(makeHeader(100, makeHash(0x64)), nil).
		Maybe()

	mockClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Return(nil, errors.New("no wss")).
		Maybe()

	mockClient.On("FilterLogs", mock.Anything, mock.AnythingOfType("ethereum.FilterQuery")).
		Return([]types.Log{}, nil).
		Maybe()

	mockBlockDb.On("GetHash", mock.AnythingOfType("uint64")).
		Return(common.Hash{}, false).
		Maybe()

	mockBlockDb.On("SetHash", mock.AnythingOfType("uint64"), mock.AnythingOfType("common.Hash")).
		Return(nil).
		Maybe()

	mockDecoder.On("Decode", mock.Anything).
		Return(nil).
		Maybe()

	mockSales.On("DetectIfSale", mock.Anything, mock.AnythingOfType("common.Hash"), mock.Anything).
		Return(map[int]tokens.TransferType{}, nil).
		Maybe()

	transfersChan := make(chan []tokens.TokenTransfer, 10)
	latestBlockChan := make(chan uint64, 1000)

	doneCh := make(chan error)
	go func() {
		err := watcher.WatchTransfers(ctx, mockClient, []string{}, 1, transfersChan, latestBlockChan)
		doneCh <- err
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-doneCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Expected nil or context.Canceled, got: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("WatchTransfers did not exit after context cancellation")
	}
}

func testLargeBlockRange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	originalSize := maxChunkSize
	maxChunkSize = 2000
	defer func() { maxChunkSize = originalSize }()

	mockClient := mocks.NewEthClient(t)
	mockBlockDb := mocks.NewBlockHashDb(t)
	mockDecoder := mocks.NewEthTransactionLogsDecoder(t)
	mockSales := mocks.NewSalesDetector(t)

	watcher := &DefaultTokensTransfersWatcher{
		Decoder:      mockDecoder,
		BlockTracker: mockBlockDb,
		SaleDetector: mockSales,
	}

	headerAt5000 := makeHeader(5000, makeHash(0x88))
	mockClient.On("HeaderByNumber", mock.Anything, (*big.Int)(nil)).
		Return(headerAt5000, nil).
		Times(3)

	mockClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Return(nil, errors.New("no wss")).
		Once()

	q1 := ethereum.FilterQuery{
		FromBlock: big.NewInt(1),
		ToBlock:   big.NewInt(2000),
		Addresses: []common.Address{},
	}
	q2 := ethereum.FilterQuery{
		FromBlock: big.NewInt(2001),
		ToBlock:   big.NewInt(4000),
		Addresses: []common.Address{},
	}
	q3 := ethereum.FilterQuery{
		FromBlock: big.NewInt(4001),
		ToBlock:   big.NewInt(5000),
		Addresses: []common.Address{},
	}

	mockClient.On("FilterLogs", mock.Anything, q1).
		Return([]types.Log{}, nil).
		Once()
	mockClient.On("FilterLogs", mock.Anything, q2).
		Return([]types.Log{}, nil).
		Once()
	mockClient.On("FilterLogs", mock.Anything, q3).
		Return([]types.Log{}, nil).
		Once()

	mockClient.On("HeaderByNumber", mock.Anything, mock.AnythingOfType("*big.Int")).
		Return(makeHeader(100, makeHash(0x64)), nil).Maybe()

	mockBlockDb.On("GetHash", mock.AnythingOfType("uint64")).
		Return(common.Hash{}, false).
		Maybe()
	mockBlockDb.On("SetHash", mock.AnythingOfType("uint64"), mock.AnythingOfType("common.Hash")).
		Return(nil).
		Maybe()

	mockDecoder.On("Decode", mock.Anything).
		Return(nil).
		Maybe()

	mockSales.On("DetectIfSale", mock.Anything, mock.AnythingOfType("common.Hash"), mock.Anything).
		Return(map[int]tokens.TransferType{}, nil).
		Maybe()

	transfersChan := make(chan []tokens.TokenTransfer, 1000000)
	latestBlockChan := make(chan uint64, 1000000)

	doneCh := make(chan error, 1)
	go func() {
		err := watcher.WatchTransfers(ctx, mockClient, []string{}, 1, transfersChan, latestBlockChan)
		doneCh <- err
	}()

	var seen []uint64
	var saw5000 bool
readLoop:
	for {
		select {
		case b := <-latestBlockChan:
			seen = append(seen, b)
			if b == 5000 && !saw5000 {
				saw5000 = true
				cancel()
			}

		case err := <-doneCh:
			if err != nil {
				t.Fatalf("WatchTransfers error: %v", err)
			}
			break readLoop

		case <-time.After(2 * time.Second):
			t.Fatal("Timed out waiting for the watcher to return.")
		}
	}

	assert.NotEmpty(t, seen)
	assert.Equal(t, uint64(5000), seen[len(seen)-1])
}

func testWatchTransfersSaleDetectionSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := mocks.NewEthClient(t)
	mockBlockDb := mocks.NewBlockHashDb(t)
	mockDecoder := mocks.NewEthTransactionLogsDecoder(t)
	mockSales := mocks.NewSalesDetector(t)

	watcher := &DefaultTokensTransfersWatcher{
		Decoder:      mockDecoder,
		BlockTracker: mockBlockDb,
		SaleDetector: mockSales,
	}

	headerAt200 := makeHeader(200, makeHash(0xAA))
	mockClient.
		On("HeaderByNumber", mock.Anything, (*big.Int)(nil)).
		Return(headerAt200, nil).
		Maybe()

	mockClient.
		On("SubscribeNewHead", mock.Anything, mock.Anything).
		Return(nil, errors.New("no subscription")).
		Maybe()

	txHashA := common.HexToHash("0xABC")
	txHashB := common.HexToHash("0xDEF")
	logs := []types.Log{
		{BlockNumber: 100, TxHash: txHashA, Index: 1, TxIndex: 0},
		{BlockNumber: 100, TxHash: txHashA, Index: 2, TxIndex: 0},
		{BlockNumber: 100, TxHash: txHashB, Index: 3, TxIndex: 1},
	}
	filterQuery := ethereum.FilterQuery{
		FromBlock: big.NewInt(100),
		ToBlock:   big.NewInt(200),
		Addresses: []common.Address{common.HexToAddress("0xABCDEF")},
	}
	mockClient.
		On("FilterLogs", mock.Anything, filterQuery).
		Return(logs, nil).
		Once()

	txa1 := tokens.TokenTransfer{
		TxHash:           "0xABC",
		BlockNumber:      100,
		LogIndex:         1,
		TransactionIndex: 0,
		Type:             tokens.SEND,
	}
	txa2 := tokens.TokenTransfer{
		TxHash:           "0xABC",
		BlockNumber:      100,
		LogIndex:         2,
		TransactionIndex: 0,
		Type:             tokens.SEND,
	}
	txb1 := tokens.TokenTransfer{
		TxHash:           "0xDEF",
		BlockNumber:      100,
		LogIndex:         3,
		TransactionIndex: 1,
		Type:             tokens.SEND,
	}

	mockDecoder.
		On("Decode", logs).
		Return([][]tokens.TokenTransfer{
			{txa1, txa2, txb1},
		}).
		Once()

	mockBlockDb.
		On("GetHash", mock.MatchedBy(func(b uint64) bool { return b < 100 })).
		Return(common.Hash{}, false).
		Maybe()

	mockBlockDb.
		On("GetHash", uint64(100)).
		Return(common.Hash{}, false).
		Once()
	mockBlockDb.
		On("SetHash", uint64(100), mock.Anything).
		Return(nil).
		Once()

	mockClient.
		On("HeaderByNumber", mock.Anything, big.NewInt(100)).
		Return(makeHeader(100, makeHash(0xBE)), nil).
		Once()

	mockSales.
		On("DetectIfSale", mock.Anything, txHashA, mock.AnythingOfType("[]tokens.TokenTransfer")).
		Return(map[int]tokens.TransferType{
			0: tokens.SALE,
			1: tokens.SEND,
		}, nil).
		Once()

	mockSales.
		On("DetectIfSale", mock.Anything, txHashB, mock.AnythingOfType("[]tokens.TokenTransfer")).
		Return(map[int]tokens.TransferType{0: tokens.AIRDROP}, nil).
		Once()

	transfersChan := make(chan []tokens.TokenTransfer, 10)
	latestBlockChan := make(chan uint64, 10)

	doneCh := make(chan error)
	go func() {
		err := watcher.WatchTransfers(
			ctx,
			mockClient,
			[]string{"0xABCDEF"},
			100,
			transfersChan,
			latestBlockChan,
		)
		doneCh <- err
	}()

	var gotTransfers []tokens.TokenTransfer
	select {
	case batch := <-transfersChan:
		gotTransfers = batch
	case <-time.After(time.Second):
		t.Fatal("Did not receive expected NFT transfers from block=100 in time")
	}
	assert.Len(t, gotTransfers, 3, "Should emit 3 xfers from block=100")
	assert.Equal(t, tokens.SALE, gotTransfers[0].Type, "0xABC index0 => SALE")
	assert.Equal(t, tokens.SEND, gotTransfers[1].Type, "0xABC index1 => SEND")
	assert.Equal(t, tokens.AIRDROP, gotTransfers[2].Type, "0xDEF => AIRDROP")

	select {
	case latest := <-latestBlockChan:
		assert.Equal(t, uint64(200), latest, "Should signal we finished up to block=200")
	case <-time.After(time.Second):
		t.Fatal("Did not get latest block signal for block=200")
	}

	cancel()
	select {
	case err := <-doneCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Expected nil or context.Canceled, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WatchTransfers did not exit after context cancel")
	}
}

func testWatchTransfersSaleDetectionError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := mocks.NewEthClient(t)
	mockBlockDb := mocks.NewBlockHashDb(t)
	mockDecoder := mocks.NewEthTransactionLogsDecoder(t)
	mockSales := mocks.NewSalesDetector(t)

	watcher := &DefaultTokensTransfersWatcher{
		Decoder:      mockDecoder,
		BlockTracker: mockBlockDb,
		SaleDetector: mockSales,
	}

	headerAt300 := makeHeader(300, makeHash(0x33))
	mockClient.
		On("HeaderByNumber", mock.Anything, (*big.Int)(nil)).
		Return(headerAt300, nil).
		Maybe()

	mockClient.
		On("SubscribeNewHead", mock.Anything, mock.Anything).
		Return(nil, errors.New("no subscription")).
		Maybe()

	txHashErr := common.HexToHash("0xBAD")
	logs := []types.Log{
		{BlockNumber: 200, TxHash: txHashErr, Index: 3, TxIndex: 0},
	}
	fq := ethereum.FilterQuery{
		FromBlock: big.NewInt(200),
		ToBlock:   big.NewInt(300),
		Addresses: []common.Address{common.HexToAddress("0xFEEED")},
	}
	mockClient.
		On("FilterLogs", mock.Anything, fq).
		Return(logs, nil).
		Once()

	transferErr := tokens.TokenTransfer{
		TxHash:           "0xBAD",
		BlockNumber:      200,
		LogIndex:         3,
		TransactionIndex: 0,
		Type:             tokens.SEND,
	}
	mockDecoder.
		On("Decode", logs).
		Return([][]tokens.TokenTransfer{{transferErr}}).
		Once()

	mockBlockDb.
		On("GetHash", mock.MatchedBy(func(b uint64) bool { return b < 200 })).
		Return(common.Hash{}, false).
		Maybe()

	mockBlockDb.
		On("GetHash", uint64(200)).
		Return(common.Hash{}, false).
		Once()
	mockBlockDb.
		On("SetHash", uint64(200), mock.Anything).
		Return(nil).
		Once()

	mockClient.
		On("HeaderByNumber", mock.Anything, big.NewInt(200)).
		Return(makeHeader(200, makeHash(0xBE)), nil).
		Once()

	mockSales.
		On("DetectIfSale", mock.Anything, txHashErr, mock.Anything).
		Return(nil, errors.New("some sale detection error")).
		Once()

	transfersChan := make(chan []tokens.TokenTransfer, 10)
	latestBlockChan := make(chan uint64, 10)

	doneCh := make(chan error)
	go func() {
		err := watcher.WatchTransfers(ctx,
			mockClient,
			[]string{"0xFEEED"},
			200,
			transfersChan,
			latestBlockChan,
		)
		doneCh <- err
	}()

	var gotTransfers []tokens.TokenTransfer
	select {
	case batch := <-transfersChan:
		gotTransfers = batch
	case <-time.After(time.Second):
		t.Fatal("No NFT transfers were emitted!")
	}

	assert.Len(t, gotTransfers, 1)
	assert.Equal(t, tokens.SEND, gotTransfers[0].Type, "No classification if sale detection fails")

	select {
	case latest := <-latestBlockChan:
		assert.Equal(t, uint64(300), latest)
	case <-time.After(time.Second):
		t.Fatal("Did not get latest block signal for block=300")
	}

	cancel()
	select {
	case err := <-doneCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Expected nil or context.Canceled, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WatchTransfers did not stop after context cancel")
	}
}
