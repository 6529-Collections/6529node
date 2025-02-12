package tdh

import (
	"context"

	"github.com/6529-Collections/6529node/internal/eth"
	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

type TdhContractsListener struct {
	transfersWatcher        eth.TokensTransfersWatcher
	transfersReceivedAction eth.TdhTransfersReceivedAction
	progressTracker         eth.TdhIdxTrackerDb
}

func (client TdhContractsListener) listen(tipReachedChan chan<- bool) error {
	nftActionsChan := make(chan []tokens.TokenTransfer)
	go func() {
		for batch := range nftActionsChan {
			err := client.transfersReceivedAction.Handle(batch)
			if err != nil {
				zap.L().Error("Error handling transfers", zap.Error(err))
				panic(err)
			}
		}
	}()
	startBlock, err := client.progressTracker.GetProgress()
	if err != nil {
		return err
	}
	if startBlock > constants.TDH_CONTRACTS_EPOCH_BLOCK {
		startBlock -= 50 // just to be safe
	}
	if startBlock < constants.TDH_CONTRACTS_EPOCH_BLOCK {
		startBlock = constants.TDH_CONTRACTS_EPOCH_BLOCK
	}
	latestBlockChannel := make(chan uint64)
	go func() {
		for latestBlock := range latestBlockChannel {
			err := client.progressTracker.SetProgress(latestBlock)
			if err != nil {
				zap.L().Error("Error setting progress", zap.Error(err))
			}
		}
	}()
	return client.transfersWatcher.WatchTransfers(
		[]string{
			constants.MEMES_CONTRACT,
			constants.GRADIENTS_CONTRACT,
			constants.NEXTGEN_CONTRACT,
		},
		startBlock,
		nftActionsChan,
		latestBlockChannel,
		tipReachedChan,
	)
}

func BlockUntilOnTipAndKeepListeningAsync(badger *badger.DB, ctx context.Context) error {
	tdhSynchroniserFatalErrors := make(chan error, 10)
	go func() {
		for err := range tdhSynchroniserFatalErrors {
			zap.L().Fatal("Fatal error listening on TDH contracts", zap.Error(err))
		}
	}()
	transfersWatcher, err := eth.NewTokensTransfersWatcher(badger, ctx)
	if err != nil {
		return err
	}
	listener := &TdhContractsListener{
		transfersWatcher:        transfersWatcher,
		transfersReceivedAction: eth.NewTdhTransfersReceivedActionImpl(badger, ctx),
		progressTracker:         eth.NewTdhIdxTrackerDb(badger),
	}
	tipReachedChan := make(chan bool, 10)
	go func() {
		err = listener.listen(tipReachedChan)
		if err != nil {
			tdhSynchroniserFatalErrors <- err
		}
	}()
	select {
	case <-tipReachedChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
