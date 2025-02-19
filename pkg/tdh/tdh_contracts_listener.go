package tdh

import (
	"context"
	"database/sql"

	"github.com/6529-Collections/6529node/internal/eth"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
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

func (client TdhContractsListener) listen(tipReachedChan chan<- bool) error {
	nftActionsChan := make(chan tokens.TokenTransferBatch)
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
	return client.transfersWatcher.WatchTransfers(
		[]string{
			MEMES_CONTRACT,
			GRADIENTS_CONTRACT,
			NEXTGEN_CONTRACT,
		},
		startBlock,
		nftActionsChan,
		tipReachedChan,
	)
}

func BlockUntilOnTipAndKeepListeningAsync(db *sql.DB, ctx context.Context) error {
	tdhSynchroniserFatalErrors := make(chan error, 10)
	go func() {
		for err := range tdhSynchroniserFatalErrors {
			zap.L().Fatal("Fatal error listening on TDH contracts", zap.Error(err))
		}
	}()
	transfersWatcher, err := eth.NewTokensTransfersWatcher(db, ctx)
	if err != nil {
		return err
	}
	progressTracker := eth.NewTdhIdxTrackerDb(db)
	listener := &TdhContractsListener{
		transfersWatcher:        transfersWatcher,
		transfersReceivedAction: eth.NewDefaultTdhTransfersReceivedAction(ctx, progressTracker),
		progressTracker:         progressTracker,
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
