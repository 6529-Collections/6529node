package tdh

import (
	"context"
	"database/sql"

	"github.com/6529-Collections/6529node/internal/eth"
	"github.com/6529-Collections/6529node/internal/eth/ethdb"
	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"go.uber.org/zap"
)

type TdhContractsListener struct {
	transfersWatcher        eth.TokensTransfersWatcher
	transfersReceivedAction eth.TdhTransfersReceivedAction
	progressTracker         ethdb.TdhIdxTrackerDb
}

func (client TdhContractsListener) listen(tipReachedChan chan<- bool) error {
	nftActionsChan := make(chan models.TokenTransferBatch)
	errChan := make(chan error, 1)

	go func() {
		for batch := range nftActionsChan {
			err := client.transfersReceivedAction.Handle(batch)
			if err != nil {
				zap.L().Error("Error handling transfers", zap.Error(err))
				errChan <- err // Send error to main function
				return         // Stop further processing
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

	// Start transfers watching in a separate goroutine
	done := make(chan error, 1)
	go func() {
		done <- client.transfersWatcher.WatchTransfers(
			[]string{
				constants.MEMES_CONTRACT,
				constants.GRADIENTS_CONTRACT,
				constants.NEXTGEN_CONTRACT,
			},
			startBlock,
			nftActionsChan,
			tipReachedChan,
		)
	}()

	// Wait for any error from errChan or done
	select {
	case err := <-errChan:
		return err
	case err := <-done:
		return err
	}
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
	transferDb := ethdb.NewTransferDb()
	ownerDb := ethdb.NewOwnerDb()
	nftDb := ethdb.NewNFTDb()
	progressTracker := ethdb.NewTdhIdxTrackerDb(db)
	listener := &TdhContractsListener{
		transfersWatcher:        transfersWatcher,
		transfersReceivedAction: eth.NewTdhTransfersReceivedActionImpl(ctx, db, transferDb, ownerDb, nftDb, progressTracker),
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
