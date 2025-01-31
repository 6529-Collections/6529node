package eth

import (
	"context"
	"errors"
	"math/big"
	"sort"
	"time"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

const maxChunkSize = uint64(2000)

var ErrReorgDetected = errors.New("reorg detected")

type TokensTransfersWatcher interface {
	WatchTransfers(
		ctx context.Context,
		client EthClient,
		contracts []string,
		startBlock uint64,
		transfersChan chan<- []tokens.TokenTransfer,
		latestBlockChan chan<- uint64,
	) error
}

type DefaultTokensTransfersWatcher struct {
	Decoder      EthTransactionLogsDecoder
	BlockTracker BlockHashDb
}

func (w *DefaultTokensTransfersWatcher) WatchTransfers(
	ctx context.Context,
	client EthClient,
	contracts []string,
	startBlock uint64,
	transfersChan chan<- []tokens.TokenTransfer,
	latestBlockChan chan<- uint64,
) error {
	contractAddrs := make([]common.Address, len(contracts))
	for i, addr := range contracts {
		contractAddrs[i] = common.HexToAddress(addr)
	}

	zap.L().Info("Starting watch on contract transfers", zap.Strings("contracts", contracts), zap.Uint64("startBlock", startBlock))

	currentBlock := startBlock
	for {
		tipBlock, err := latestBlockNumber(ctx, client)
		if err != nil {
			if sleepInterrupted(ctx, 1*time.Second) {
				return nil
			}
			continue
		}

		if currentBlock <= tipBlock {
			endBlock := currentBlock + maxChunkSize - 1
			if endBlock > tipBlock {
				endBlock = tipBlock
			}
			err = w.processRangeWithReorg(ctx, client, contractAddrs, currentBlock, endBlock, transfersChan, latestBlockChan)
			if err != nil {
				if errors.Is(err, ErrReorgDetected) {
					continue
				}
				zap.L().Warn("Failed processing blocks range", zap.Error(err))
				if sleepInterrupted(ctx, 1*time.Second) {
					return err
				}
				continue
			}
			currentBlock = endBlock + 1
			continue
		}

		newHeads := make(chan *types.Header, 16)
		sub, err := client.SubscribeNewHead(ctx, newHeads)
		if err != nil {
			zap.L().Warn("Falling back to polling", zap.Error(err))
			return w.pollForNewBlocks(ctx, client, contractAddrs, &currentBlock, transfersChan, latestBlockChan)
		}
		return w.subscribeAndProcessHeads(ctx, sub, newHeads, client, contractAddrs, &currentBlock, transfersChan, latestBlockChan)
	}
}

func (w *DefaultTokensTransfersWatcher) pollForNewBlocks(
	ctx context.Context,
	client EthClient,
	contractAddrs []common.Address,
	currentBlock *uint64,
	transfersChan chan<- []tokens.TokenTransfer,
	latestBlockChan chan<- uint64,
) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		tipBlock, err := latestBlockNumber(ctx, client)
		if err != nil {
			zap.L().Error("Could not get latest block (polling)", zap.Error(err))
			if sleepInterrupted(ctx, 3*time.Second) {
				return nil
			}
			continue
		}

		if *currentBlock <= tipBlock {
			endBlock := *currentBlock + maxChunkSize - 1
			if endBlock > tipBlock {
				endBlock = tipBlock
			}
			err := w.processRangeWithReorg(ctx, client, contractAddrs, *currentBlock, endBlock, transfersChan, latestBlockChan)
			if err != nil {
				if errors.Is(err, ErrReorgDetected) {
					continue
				}
				zap.L().Error("Failed processing blocks range (polling)", zap.Error(err))
				if sleepInterrupted(ctx, 3*time.Second) {
					return nil
				}
				continue
			}
			*currentBlock = endBlock + 1
			continue
		}

		zap.L().Debug("No new block yet (polling)", zap.Uint64("current", *currentBlock), zap.Uint64("tip", tipBlock))
		if sleepInterrupted(ctx, 100*time.Millisecond) {
			return nil
		}
	}
}

func (w *DefaultTokensTransfersWatcher) subscribeAndProcessHeads(
	ctx context.Context,
	sub ethereum.Subscription,
	newHeads <-chan *types.Header,
	client EthClient,
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
				endBlock := *currentBlock + maxChunkSize - 1
				if endBlock >= blockNum-1 {
					endBlock = blockNum - 1
				}
				err := w.processRangeWithReorg(ctx, client, contractAddrs, *currentBlock, endBlock, transfersChan, latestBlockChan)
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
				err := w.processRangeWithReorg(ctx, client, contractAddrs, blockNum, blockNum, transfersChan, latestBlockChan)
				if err != nil {
					if errors.Is(err, ErrReorgDetected) {
						continue
					}
					return err
				}
				*currentBlock = blockNum + 1
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (w *DefaultTokensTransfersWatcher) processRangeWithReorg(
	ctx context.Context,
	client EthClient,
	contractAddrs []common.Address,
	startBlock, endBlock uint64,
	transfersChan chan<- []tokens.TokenTransfer,
	latestBlockChan chan<- uint64,
) error {
	if err := w.checkAndHandleReorg(ctx, client, startBlock); err != nil {
		return err
	}

	logs, err := fetchLogsInRange(ctx, client, contractAddrs, startBlock, endBlock)
	if err != nil {
		zap.L().Error("Failed fetching logs", zap.Uint64("start", startBlock), zap.Uint64("end", endBlock), zap.Error(err))
		return err
	}

	decoded := w.Decoder.Decode(logs)
	blockGroups := groupLogsByBlock(decoded)
	var lastLogTime time.Time
	for b := startBlock; b <= endBlock; b++ {
		latestBlockChan <- endBlock
		if time.Since(lastLogTime) >= 10*time.Second {
			zap.L().Info("TDH Contracts Listener progress", zap.Uint64("currentlyOnBlock", b))
			lastLogTime = time.Now()
		}
		header, err := client.HeaderByNumber(ctx, big.NewInt(int64(b)))
		if err != nil {
			zap.L().Error("Could not fetch block header", zap.Uint64("block", b), zap.Error(err))
			return err
		}
		chainHash := header.Hash()
		recordedHash, found := w.BlockTracker.GetHash(b)
		if found && recordedHash != chainHash {
			zap.L().Warn("Reorg detected", zap.Uint64("block", b),
				zap.String("oldHash", recordedHash.Hex()),
				zap.String("newHash", chainHash.Hex()),
			)
			w.BlockTracker.RevertFromBlock(b)
			return ErrReorgDetected
		}
		if !found {
			w.BlockTracker.SetHash(b, chainHash)
		}

		if logsInBlock, ok := blockGroups[b]; ok && len(logsInBlock) > 0 {
			sort.Slice(logsInBlock, func(i, j int) bool {
				if logsInBlock[i].BlockNumber != logsInBlock[j].BlockNumber {
					return logsInBlock[i].BlockNumber < logsInBlock[j].BlockNumber
				}
				if logsInBlock[i].TransactionIndex != logsInBlock[j].TransactionIndex {
					return logsInBlock[i].TransactionIndex < logsInBlock[j].TransactionIndex
				}
				return logsInBlock[i].LogIndex < logsInBlock[j].LogIndex
			})

			select {
			case transfersChan <- logsInBlock:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

func (w *DefaultTokensTransfersWatcher) checkAndHandleReorg(
	ctx context.Context,
	client EthClient,
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
		blockNum := startBlock - 1
		recordedHash, found := w.BlockTracker.GetHash(blockNum)
		if !found {
			if blockNum == 0 {
				return nil
			}
			startBlock--
			continue
		}
		header, err := client.HeaderByNumber(ctx, big.NewInt(int64(blockNum)))
		if err != nil {
			zap.L().Error("Could not fetch block header (reorg check)", zap.Uint64("block", blockNum), zap.Error(err))
			return err
		}
		if header.Hash() != recordedHash {
			reorgStart = blockNum
			break
		}
	}
	if reorgStart > 0 {
		zap.L().Warn("Deep reorg detected", zap.Uint64("reorgStartBlock", reorgStart))
		w.BlockTracker.RevertFromBlock(reorgStart)
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

func fetchLogsInRange(
	ctx context.Context,
	client EthClient,
	addresses []common.Address,
	startBlock, endBlock uint64,
) ([]types.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(startBlock)),
		ToBlock:   big.NewInt(int64(endBlock)),
		Addresses: addresses,
	}
	return client.FilterLogs(ctx, query)
}

func groupLogsByBlock(decoded [][]tokens.TokenTransfer) map[uint64][]tokens.TokenTransfer {
	groups := make(map[uint64][]tokens.TokenTransfer)
	for _, batch := range decoded {
		for _, t := range batch {
			groups[t.BlockNumber] = append(groups[t.BlockNumber], t)
		}
	}
	return groups
}

func sleepInterrupted(ctx context.Context, d time.Duration) bool {
	time.Sleep(d)
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
