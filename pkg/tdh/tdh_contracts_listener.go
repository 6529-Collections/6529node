package tdh

import (
	"context"

	"github.com/6529-Collections/6529node/internal/eth"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

var (
	MEMES_CONTRACT            = "0x33fd426905f149f8376e227d0c9d3340aad17af1"
	NEXTGEN_CONTRACT          = "0x45882f9bc325E14FBb298a1Df930C43a874B83ae"
	GRADIENTS_CONTRACT        = "0x0c58ef43ff3032005e472cb5709f8908acb00205"
	TDH_CONTRACTS_EPOCH_BLOCK = uint64(13360860)
)

type TdhContractsListener struct {
	transfersWatcher        eth.TokensTransfersWatcher
	transfersReceivedAction eth.TdhTransfersReceivedAction
	progressTracker         eth.TdhIdxTrackerDb
}

func (client TdhContractsListener) Listen() error {
	nftActionsChan := make(chan []tokens.TokenTransfer)
	go func() {
		for batch := range nftActionsChan {
			err := client.transfersReceivedAction.Handle(batch)
			if err != nil {
				zap.L().Error("Error handling transfers", zap.Error(err))
			}
		}
	}()
	startBlock, err := client.progressTracker.GetProgress()
	if err != nil {
		return err
	}
	if startBlock > TDH_CONTRACTS_EPOCH_BLOCK {
		startBlock -= 50 // just to be safe
	}
	if startBlock < TDH_CONTRACTS_EPOCH_BLOCK {
		startBlock = TDH_CONTRACTS_EPOCH_BLOCK
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
			MEMES_CONTRACT,
			GRADIENTS_CONTRACT,
			NEXTGEN_CONTRACT,
		},
		startBlock,
		nftActionsChan,
		latestBlockChannel,
	)
}

func CreateTdhContractsListener(badger *badger.DB, ctx context.Context) (*TdhContractsListener, error) {
	transfersWatcher, err := eth.NewTokensTransfersWatcher(badger, ctx)
	if err != nil {
		return nil, err
	}
	return &TdhContractsListener{
		transfersWatcher:        transfersWatcher,
		transfersReceivedAction: eth.NewTdhTransfersReceivedActionImpl(badger, ctx),
		progressTracker:         eth.NewTdhIdxTrackerDb(badger),
	}, nil
}
