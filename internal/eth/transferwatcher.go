package eth

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

var ErrReorgDetected = errors.New("reorg detected")

type TokensTransfersWatcher interface {
	WatchTransfers(
		contracts []string,
		startBlock uint64,
		transfersChan chan<- []tokens.TokenTransfer,
		latestBlockChan chan<- uint64,
		tipReachedChan chan<- bool,
	) error
}

type DefaultTokensTransfersWatcher struct {
	ctx              context.Context
	client           EthClient
	decoder          EthTransactionLogsDecoder
	blockTracker     BlockHashDb
	salesDetector    SalesDetector
	maxBlocksInBatch uint64
	maxLogsInBatch   uint64
}

func NewTokensTransfersWatcher(db *badger.DB, ctx context.Context) (*DefaultTokensTransfersWatcher, error) {
	ethClient, err := CreateEthClient()
	if err != nil {
		return nil, err
	}
	maxBlocksInBatch := config.Get().TdhMaxBlocksInBatch
	if maxBlocksInBatch == 0 {
		maxBlocksInBatch = 20000
	}
	maxLogsInBatch := config.Get().TdhMaxLogsInBatch
	if maxLogsInBatch == 0 {
		maxLogsInBatch = 2000
	}
	return &DefaultTokensTransfersWatcher{
		ctx:              ctx,
		client:           ethClient,
		decoder:          NewDefaultEthTransactionLogsDecoder(),
		blockTracker:     NewBlockHashDb(db),
		salesDetector:    NewDefaultSalesDetector(ethClient),
		maxBlocksInBatch: maxBlocksInBatch,
		maxLogsInBatch:   maxLogsInBatch,
	}, nil
}

func (w *DefaultTokensTransfersWatcher) WatchTransfers(
	contracts []string,
	startBlock uint64,
	transfersChan chan<- []tokens.TokenTransfer,
	latestBlockChan chan<- uint64,
	tipReachedChan chan<- bool,
) error {
	defer w.client.Close()
	contractAddrs := make([]common.Address, len(contracts))
	for i, addr := range contracts {
		contractAddrs[i] = common.HexToAddress(addr)
	}

	zap.L().Info("Starting watch on contract transfers",
		zap.Strings("contracts", contracts),
		zap.Uint64("startBlock", startBlock),
	)

	currentBlock := startBlock
	for {
		tipBlock, err := latestBlockNumber(w.ctx, w.client)
		if err != nil {
			if sleepInterrupted(w.ctx, 1*time.Second) {
				return nil
			}
			continue
		}

		if currentBlock <= tipBlock {
			endBlock := currentBlock + w.maxBlocksInBatch - 1
			if endBlock > tipBlock {
				endBlock = tipBlock
			}
			err = w.processRangeAdaptive(
				contractAddrs,
				currentBlock,
				endBlock,
				int(w.maxLogsInBatch),
				transfersChan,
				latestBlockChan,
			)
			if err != nil {
				if errors.Is(err, ErrReorgDetected) {
					continue
				}
				zap.L().Warn("Failed processing blocks range", zap.Error(err))
				if sleepInterrupted(w.ctx, 1*time.Second) {
					return err
				}
				continue
			}
			currentBlock = endBlock + 1
			continue
		}

		newHeads := make(chan *types.Header, 16)

		fmt.Println("tip reached")
		tipReachedChan <- true
		sub, err := w.client.SubscribeNewHead(w.ctx, newHeads)
		if err != nil {
			zap.L().Warn("Falling back to polling", zap.Error(err))
			return w.pollForNewBlocks(contractAddrs, &currentBlock, transfersChan, latestBlockChan)
		}
		return w.subscribeAndProcessHeads(sub, newHeads, contractAddrs, &currentBlock, transfersChan, latestBlockChan)
	}
}

func (w *DefaultTokensTransfersWatcher) pollForNewBlocks(
	contractAddrs []common.Address,
	currentBlock *uint64,
	transfersChan chan<- []tokens.TokenTransfer,
	latestBlockChan chan<- uint64,
) error {
	for {
		if w.ctx.Err() != nil {
			return nil
		}
		tipBlock, err := latestBlockNumber(w.ctx, w.client)
		if err != nil {
			zap.L().Error("Could not get latest block (polling)", zap.Error(err))
			if sleepInterrupted(w.ctx, 3*time.Second) {
				return nil
			}
			continue
		}

		if *currentBlock <= tipBlock {
			endBlock := *currentBlock + w.maxBlocksInBatch - 1
			if endBlock > tipBlock {
				endBlock = tipBlock
			}
			err := w.processRangeAdaptive(contractAddrs, *currentBlock, endBlock, int(w.maxLogsInBatch), transfersChan, latestBlockChan)
			if err != nil {
				if errors.Is(err, ErrReorgDetected) {
					continue
				}
				zap.L().Error("Failed processing blocks range (polling)", zap.Error(err))
				if sleepInterrupted(w.ctx, 3*time.Second) {
					return nil
				}
				continue
			}
			*currentBlock = endBlock + 1
			continue
		}

		zap.L().Debug("No new block yet (polling)",
			zap.Uint64("current", *currentBlock),
			zap.Uint64("tip", tipBlock),
		)
		if sleepInterrupted(w.ctx, 100*time.Millisecond) {
			return nil
		}
	}
}

func (w *DefaultTokensTransfersWatcher) subscribeAndProcessHeads(
	sub ethereum.Subscription,
	newHeads <-chan *types.Header,
	contractAddrs []common.Address,
	currentBlock *uint64,
	transfersChan chan<- []tokens.TokenTransfer,
	latestBlockChan chan<- uint64,
) error {
	defer sub.Unsubscribe()

	for {
		select {
		case err := <-sub.Err():
			return err

		case header := <-newHeads:
			if header == nil {
				return nil
			}
			blockNum := header.Number.Uint64()
			for *currentBlock < blockNum {
				endBlock := *currentBlock + w.maxBlocksInBatch - 1
				if endBlock >= blockNum-1 {
					endBlock = blockNum - 1
				}
				err := w.processRangeAdaptive(contractAddrs, *currentBlock, endBlock, int(w.maxLogsInBatch), transfersChan, latestBlockChan)
				if err != nil {
					if errors.Is(err, ErrReorgDetected) {
						continue
					}
					zap.L().Error("Failed processing blocks range (subscription)", zap.Error(err))
					return err
				}
				*currentBlock = endBlock + 1
			}

			if blockNum >= *currentBlock {
				err := w.processRangeAdaptive(contractAddrs, blockNum, blockNum, int(w.maxLogsInBatch), transfersChan, latestBlockChan)
				if err != nil {
					if errors.Is(err, ErrReorgDetected) {
						continue
					}
					return err
				}
				*currentBlock = blockNum + 1
			}

		case <-w.ctx.Done():
			return nil
		}
	}
}

func (w *DefaultTokensTransfersWatcher) processRangeAdaptive(
	contractAddrs []common.Address,
	startBlock, endBlock uint64,
	maxLogsThreshold int,
	transfersChan chan<- []tokens.TokenTransfer,
	latestBlockChan chan<- uint64,
) error {
	if startBlock > endBlock {
		return nil
	}

	if err := w.checkAndHandleReorg(startBlock); err != nil {
		return err
	}

	logs, err := w.fetchLogsInRange(contractAddrs, startBlock, endBlock)
	if err != nil {
		zap.L().Error("Failed fetching logs (adaptive)",
			zap.Uint64("start", startBlock),
			zap.Uint64("end", endBlock),
			zap.Error(err),
		)
		return err
	}

	if len(logs) > maxLogsThreshold && endBlock > startBlock {
		mid := (startBlock + endBlock) / 2
		err := w.processRangeAdaptive(contractAddrs, startBlock, mid, maxLogsThreshold, transfersChan, latestBlockChan)
		if err != nil {
			if errors.Is(err, ErrReorgDetected) {
				return err
			}
			return err
		}
		return w.processRangeAdaptive(contractAddrs, mid+1, endBlock, maxLogsThreshold, transfersChan, latestBlockChan)
	}

	decodedByBlock := w.decoder.Decode(logs)

	var allTransfers []tokens.TokenTransfer
	for _, blockTransfers := range decodedByBlock {
		allTransfers = append(allTransfers, blockTransfers...)
	}

	txMap := make(map[common.Hash][]*tokens.TokenTransfer)
	for i := range allTransfers {
		txHash := common.HexToHash(allTransfers[i].TxHash)
		txMap[txHash] = append(txMap[txHash], &allTransfers[i])
	}
	for txHash, xfers := range txMap {
		resultMap, err := w.salesDetector.DetectIfSale(w.ctx, txHash, deref(xfers))
		if err != nil {
			zap.L().Error("Sale detection failed", zap.Error(err), zap.String("txHash", txHash.Hex()))
			continue
		}
		for i, tr := range xfers {
			tr.Type = resultMap[i]
		}
	}

	blockGroups := groupLogsByBlock(allTransfers)
	if len(blockGroups) == 0 {
		latestBlockChan <- endBlock
		return nil
	}

	var blocksWithLogs []uint64
	for b := range blockGroups {
		blocksWithLogs = append(blocksWithLogs, b)
	}
	sort.Slice(blocksWithLogs, func(i, j int) bool { return blocksWithLogs[i] < blocksWithLogs[j] })

	var lastLogTime time.Time

	for _, b := range blocksWithLogs {
		header, err := w.client.HeaderByNumber(w.ctx, big.NewInt(int64(b)))
		if err != nil {
			zap.L().Error("Could not fetch block header", zap.Uint64("block", b), zap.Error(err))
			return err
		}
		chainHash := header.Hash()
		recordedHash, found := w.blockTracker.GetHash(b)
		if found && recordedHash != chainHash {
			zap.L().Warn("Reorg detected (adaptive)",
				zap.Uint64("block", b),
				zap.String("oldHash", recordedHash.Hex()),
				zap.String("newHash", chainHash.Hex()),
			)
			_ = w.blockTracker.RevertFromBlock(b)
			return ErrReorgDetected
		}
		if !found {
			if err := w.blockTracker.SetHash(b, chainHash); err != nil {
				zap.L().Error("Could not set block hash", zap.Uint64("block", b), zap.Error(err))
				return err
			}
		}

		if time.Since(lastLogTime) >= 10*time.Second {
			zap.L().Info("TDH Contracts Listener progress", zap.Uint64("currentlyOnBlock", b))
			lastLogTime = time.Now()
		}

		logsInBlock := blockGroups[b]
		sort.Slice(logsInBlock, func(i, j int) bool {
			if logsInBlock[i].TransactionIndex != logsInBlock[j].TransactionIndex {
				return logsInBlock[i].TransactionIndex < logsInBlock[j].TransactionIndex
			}
			return logsInBlock[i].LogIndex < logsInBlock[j].LogIndex
		})

		select {
		case transfersChan <- logsInBlock:
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}

	latestBlockChan <- endBlock
	return nil
}

func deref(xfers []*tokens.TokenTransfer) []tokens.TokenTransfer {
	derefed := make([]tokens.TokenTransfer, len(xfers))
	for i, xfer := range xfers {
		derefed[i] = *xfer
	}
	return derefed
}

func (w *DefaultTokensTransfersWatcher) checkAndHandleReorg(
	startBlock uint64,
) error {
	if startBlock == 0 {
		return nil
	}
	maxDepth := 12
	var reorgStart uint64

	for i := 0; i < maxDepth; i++ {
		if startBlock == 0 {
			break
		}
		blockNum := startBlock - 1 - uint64(i)
		recordedHash, found := w.blockTracker.GetHash(blockNum)
		if !found {
			if blockNum == 0 {
				return nil
			}
			startBlock--
			continue
		}
		header, err := w.client.HeaderByNumber(w.ctx, big.NewInt(int64(blockNum)))
		if err != nil {
			zap.L().Error("Could not fetch block header (reorg check)",
				zap.Uint64("block", blockNum),
				zap.Error(err),
			)
			return err
		}
		if header.Hash() != recordedHash {
			reorgStart = blockNum
			break
		}
		break
	}
	if reorgStart > 0 {
		zap.L().Warn("Deep reorg detected", zap.Uint64("reorgStartBlock", reorgStart))
		err := w.blockTracker.RevertFromBlock(reorgStart)
		if err != nil {
			zap.L().Error("Could not revert block hash", zap.Uint64("block", reorgStart), zap.Error(err))
			return err
		}
		return ErrReorgDetected
	}
	return nil
}

func latestBlockNumber(ctx context.Context, client EthClient) (uint64, error) {
	header, err := client.HeaderByNumber(ctx, nil)
	if err != nil {
		zap.L().Error("Could not get latest block header", zap.Error(err))
		return 0, err
	}
	return header.Number.Uint64(), nil
}

func (w *DefaultTokensTransfersWatcher) fetchLogsInRange(
	addresses []common.Address,
	startBlock, endBlock uint64,
) ([]types.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(startBlock)),
		ToBlock:   big.NewInt(int64(endBlock)),
		Addresses: addresses,
	}
	return w.client.FilterLogs(w.ctx, query)
}

func groupLogsByBlock(decoded []tokens.TokenTransfer) map[uint64][]tokens.TokenTransfer {
	groups := make(map[uint64][]tokens.TokenTransfer)
	for _, t := range decoded {
		groups[t.BlockNumber] = append(groups[t.BlockNumber], t)
	}
	return groups
}

func sleepInterrupted(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}
