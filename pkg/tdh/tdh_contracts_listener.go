package tdh

import (
	"context"

	"github.com/6529-Collections/6529node/internal/eth"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
)

var (
	MEMES_CONTRACT            = "0x33fd426905f149f8376e227d0c9d3340aad17af1"
	NEXTGEN_CONTRACT          = "0x45882f9bc325E14FBb298a1Df930C43a874B83ae"
	GRADIENTS_CONTRACT        = "0x0c58ef43ff3032005e472cb5709f8908acb00205"
	TDH_CONTRACTS_EPOCH_BLOCK = uint64(14933632)
)

type TdhContractsListener struct {
	ethClient               eth.EthClient
	transfersWatcher        eth.TokensTransfersWatcher
	transfersReceivedAction eth.TdhTransfersReceivedAction
	progressTracker         eth.TdhIdxTrackerDb
}

func (client TdhContractsListener) Listen(
	ctx context.Context,
) error {
	nftActionsChan := make(chan []tokens.TokenTransfer)
	go func() {
		for batch := range nftActionsChan {
			err := client.transfersReceivedAction.Handle(ctx, batch)
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
			client.progressTracker.SetProgress(latestBlock)
		}
	}()
	return client.transfersWatcher.WatchTransfers(
		ctx,
		client.ethClient, []string{
			MEMES_CONTRACT,
			GRADIENTS_CONTRACT,
			NEXTGEN_CONTRACT,
		},
		startBlock,
		nftActionsChan,
		latestBlockChannel,
	)
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
		transfersReceivedAction: &eth.DefaultTdhTransfersReceivedAction{},
		progressTracker:         eth.NewTdhIdxTrackerDb(badger),
	}, nil
}
