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
	ethClient               eth.EthClient
	transfersWatcher        eth.TokensTransfersWatcher
	transfersReceivedAction eth.TdhTransfersReceivedAction
	progressTracker         eth.TdhIdxTrackerDb
}

func (client TdhContractsListener) Listen(ctx context.Context) error {
    // 1) Prepare channels
    nftActionsChan := make(chan []tokens.TokenTransfer)
    latestBlockChan := make(chan uint64)
    // We'll collect errors from either the "Handle" goroutine or WatchTransfers
    errorChan := make(chan error, 1)

    // 2) Fetch progress
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

    // 3) Goroutine: read from nftActionsChan => call Handle => if error => send to errorChan
    go func() {
        defer close(errorChan) // once this goroutine exits, we close errorChan
        for batch := range nftActionsChan {
            // If Handle fails => we send error and return => no more batches processed
            if herr := client.transfersReceivedAction.Handle(ctx, batch); herr != nil {
                zap.L().Error("Error handling transfers, stopping loop", zap.Error(herr))
                errorChan <- herr
                return
            }
        }
    }()

    // 4) Goroutine: read latestBlockChan => update progress
    go func() {
        for lb := range latestBlockChan {
            client.progressTracker.SetProgress(lb)
        }
    }()

    // 5) Goroutine: run WatchTransfers. On error, send to errorChan
    go func() {
        err := client.transfersWatcher.WatchTransfers(
            ctx,
            client.ethClient,
            []string{
                MEMES_CONTRACT,
                GRADIENTS_CONTRACT,
                NEXTGEN_CONTRACT,
            },
            startBlock,
            nftActionsChan,
            latestBlockChan,
        )
        if err != nil {
            // Send the watch error, if it occurs
            select {
            case errorChan <- err:
            default:
                // If errorChan is closed or blocked, we just log
                zap.L().Warn("WatchTransfers error but errorChan not available", zap.Error(err))
            }
        }
        // Once WatchTransfers returns, close these channels so the other goroutines can exit
        close(nftActionsChan)
        close(latestBlockChan)
    }()

    // 6) Wait for either:
    //    - an error from errorChan
    //    - context cancellation
    //    - or everything to finish gracefully.
    select {
    case <-ctx.Done():
        // If context is canceled externally, we stop.
        zap.L().Info("Context canceled, Listen stopping")
        return ctx.Err()

    case err := <-errorChan:
        if err != nil {
            // We got a real error => return it
            zap.L().Error("Listener stopping due to errorChan", zap.Error(err))
            return err
        }
        // If errorChan is closed without an error => no error. Possibly all done.
        zap.L().Info("No errors from errorChan, likely done.")
        return nil
    }
}

func (client TdhContractsListener) Close() {
	client.ethClient.Close()
}

func CreateTdhContractsListener(badger *badger.DB) (*TdhContractsListener, error) {
	client, err := eth.CreateEthClient()
	if err != nil {
		return nil, err
	}
	return &TdhContractsListener{
		ethClient: client,
		transfersWatcher: &eth.DefaultTokensTransfersWatcher{
			Decoder:      &eth.DefaultEthTransactionLogsDecoder{},
			BlockTracker: eth.NewBlockHashDb(badger),
		},
		transfersReceivedAction: eth.NewTdhTransfersReceivedActionImpl(badger),
		progressTracker:         eth.NewTdhIdxTrackerDb(badger),
	}, nil
}
