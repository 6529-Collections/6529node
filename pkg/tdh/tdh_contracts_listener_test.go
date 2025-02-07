package tdh

import (
	"context"
	"errors"
	"testing"
	"time"

	mocks "github.com/6529-Collections/6529node/internal/eth/mocks"
	transferwatcher "github.com/6529-Collections/6529node/internal/eth/mocks/transferwatcher"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTdhContractsListener_Listen_ProgressGreaterThanEpochBlock(t *testing.T) {
    zap.ReplaceGlobals(zap.NewNop())

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Mocks
    mEthClient := new(mocks.EthClient)
    mIdxTracker := new(mocks.TdhIdxTrackerDb)
    mTransfersAction := new(mocks.TdhTransfersReceivedAction)
    mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

    mIdxTracker.On("GetProgress").Return(uint64(13360878), nil).Once()
    expectedStartBlock := TDH_CONTRACTS_EPOCH_BLOCK

    mIdxTracker.On("SetProgress", mock.AnythingOfType("uint64")).Return(nil).Maybe()

    mTransfersWatcher.
        On("WatchTransfers",
            mock.Anything,
            mEthClient,
            []string{MEMES_CONTRACT, GRADIENTS_CONTRACT, NEXTGEN_CONTRACT},
            expectedStartBlock,
            mock.Anything,
            mock.Anything,
        ).
        Return(nil).
        Run(func(args mock.Arguments) {
            nftChan := args.Get(4).(chan<- []tokens.TokenTransfer)
            lbChan := args.Get(5).(chan<- uint64)

            // Emit one new block
            lbChan <- 13360878

            // Emit one batch of transfers
            nftChan <- []tokens.TokenTransfer{
                {From: "0x111", To: "0x222"},
            }
        })

    mTransfersAction.
        On("Handle", mock.Anything, []tokens.TokenTransfer{{From: "0x111", To: "0x222"}}).
        Return(nil).
        Once()

    listener := TdhContractsListener{
        ethClient:               mEthClient,
        transfersWatcher:        mTransfersWatcher,
        transfersReceivedAction: mTransfersAction,
        progressTracker:         mIdxTracker,
    }

    err := listener.Listen(ctx)
    assert.NoError(t, err)

    time.Sleep(50 * time.Millisecond)

    mIdxTracker.AssertExpectations(t)
    mTransfersWatcher.AssertExpectations(t)
    mTransfersAction.AssertExpectations(t)
    mEthClient.AssertExpectations(t)
}

func TestTdhContractsListener_Listen_ProgressLessThanEpochBlock(t *testing.T) {
    zap.ReplaceGlobals(zap.NewNop())

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    mEthClient := new(mocks.EthClient)
    mIdxTracker := new(mocks.TdhIdxTrackerDb)
    mTransfersAction := new(mocks.TdhTransfersReceivedAction)
    mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

    // We assume progress=13360860 => that is EXACTLY EPOCH
    mIdxTracker.On("GetProgress").Return(uint64(13360860), nil).Once()

    // The 'WatchTransfers' call
    mTransfersWatcher.
        On("WatchTransfers",
            mock.Anything,
            mEthClient,
            []string{MEMES_CONTRACT, GRADIENTS_CONTRACT, NEXTGEN_CONTRACT},
            TDH_CONTRACTS_EPOCH_BLOCK,
            mock.Anything,
            mock.Anything,
        ).
        Return(nil).
        Run(func(args mock.Arguments) {
            nftChan := args.Get(4).(chan<- []tokens.TokenTransfer)
            
            // We push one batch
            nftChan <- []tokens.TokenTransfer{{From: "0x111", To: "0x222"}}

        })

    // Because we push a batch => the code calls 'Handle' with that batch
    mTransfersAction.
        On("Handle",
            mock.Anything,
            []tokens.TokenTransfer{{From: "0x111", To: "0x222"}},
        ).
        Return(nil).
        Once()

    listener := TdhContractsListener{
        ethClient:               mEthClient,
        transfersWatcher:        mTransfersWatcher,
        transfersReceivedAction: mTransfersAction,
        progressTracker:         mIdxTracker,
    }

    err := listener.Listen(ctx)
    assert.NoError(t, err)

    // Let concurrency settle
    time.Sleep(50 * time.Millisecond)

    // Now checks pass
    mIdxTracker.AssertExpectations(t)
    mTransfersWatcher.AssertExpectations(t)
    mTransfersAction.AssertExpectations(t)
    mEthClient.AssertExpectations(t)
}

func TestTdhContractsListener_Listen_ErrorOnGetProgress(t *testing.T) {
    zap.ReplaceGlobals(zap.NewNop())

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    mEthClient := new(mocks.EthClient)
    mIdxTracker := new(mocks.TdhIdxTrackerDb)
    mTransfersAction := new(mocks.TdhTransfersReceivedAction)
    mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

    mIdxTracker.On("GetProgress").Return(uint64(0), errors.New("failed to get progress")).Once()

    listener := TdhContractsListener{
        ethClient:               mEthClient,
        transfersWatcher:        mTransfersWatcher,
        transfersReceivedAction: mTransfersAction,
        progressTracker:         mIdxTracker,
    }

    err := listener.Listen(ctx)
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "failed to get progress")

    // If we fail on getProgress, WatchTransfers won't be called at all
    mTransfersWatcher.AssertNotCalled(t, "WatchTransfers", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)

    mIdxTracker.AssertExpectations(t)
    mTransfersAction.AssertExpectations(t)
    mTransfersWatcher.AssertExpectations(t)
    mEthClient.AssertExpectations(t)
}

func TestTdhContractsListener_Listen_ErrorOnWatchTransfers(t *testing.T) {
    zap.ReplaceGlobals(zap.NewNop())

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    mEthClient := new(mocks.EthClient)
    mIdxTracker := new(mocks.TdhIdxTrackerDb)
    mTransfersAction := new(mocks.TdhTransfersReceivedAction)
    mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

    mIdxTracker.On("GetProgress").Return(uint64(13360878), nil).Once()

    // WatchTransfers fails immediately
    mTransfersWatcher.
        On("WatchTransfers",
            mock.Anything,
            mEthClient,
            mock.Anything,
            uint64(13360860),
            mock.Anything,
            mock.Anything,
        ).
        Return(errors.New("watch transfers failed")).
        Once()

    listener := TdhContractsListener{
        ethClient:               mEthClient,
        transfersWatcher:        mTransfersWatcher,
        transfersReceivedAction: mTransfersAction,
        progressTracker:         mIdxTracker,
    }

    err := listener.Listen(ctx)
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "watch transfers failed")

    // No handle calls if WatchTransfers fails early
    mTransfersAction.AssertNotCalled(t, "Handle", mock.Anything, mock.Anything)

    mIdxTracker.AssertExpectations(t)
    mTransfersAction.AssertExpectations(t)
    mTransfersWatcher.AssertExpectations(t)
    mEthClient.AssertExpectations(t)
}

func TestTdhContractsListener_Listen_HandleErrorStopsLoop(t *testing.T) {
    zap.ReplaceGlobals(zap.NewNop())

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Mocks
    mEthClient := new(mocks.EthClient)
    mIdxTracker := new(mocks.TdhIdxTrackerDb)
    mTransfersAction := new(mocks.TdhTransfersReceivedAction)
    mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

    // The progress tracker returns a known block
    mIdxTracker.On("GetProgress").Return(uint64(13360860), nil).Once()

    // The watcher will emit two batches of transfers: BAD then GOOD
    mTransfersWatcher.
        On("WatchTransfers",
            mock.Anything,
            mEthClient,
            mock.Anything,
            uint64(13360860),
            mock.Anything, // nftChan
            mock.Anything, // lbChan
        ).
        Return(nil).
        Run(func(args mock.Arguments) {
            nftChan := args.Get(4).(chan<- []tokens.TokenTransfer)
            lbChan := args.Get(5).(chan<- uint64)

            // First batch => "0xBAD"
            nftChan <- []tokens.TokenTransfer{{From: "0xBAD"}}

            // Second batch => "0xGOOD" (we'll see if it gets processed or not)
            nftChan <- []tokens.TokenTransfer{{From: "0xGOOD"}}

            close(nftChan)
            close(lbChan)
        }).
        Once()

    // We expect the first Handle call to fail
    mTransfersAction.
        On("Handle", mock.Anything, []tokens.TokenTransfer{{From: "0xBAD"}}).
        Return(errors.New("some handle error")).
        Once()

    // We EXPECT that the second batch is **not** handled, so we won't mock it at all.
    // We'll check with 'AssertNotCalled' below.

    listener := TdhContractsListener{
        ethClient:               mEthClient,
        transfersWatcher:        mTransfersWatcher,
        transfersReceivedAction: mTransfersAction,
        progressTracker:         mIdxTracker,
    }

    // Because we want the loop to stop on error, we expect 'Listen' to return that error.
    err := listener.Listen(ctx)
    require.Error(t, err, "Listen should return the handle error immediately")
    assert.Contains(t, err.Error(), "some handle error")

    // The second batch should NOT be handled
    mTransfersAction.AssertNotCalled(t, "Handle", mock.Anything, []tokens.TokenTransfer{{From: "0xGOOD"}})

    // Check mocks
    mTransfersAction.AssertExpectations(t)
    mIdxTracker.AssertExpectations(t)
    mTransfersWatcher.AssertExpectations(t)
    mEthClient.AssertExpectations(t)
}

func TestTdhContractsListener_Close(t *testing.T) {
    mEthClient := new(mocks.EthClient)
    mEthClient.On("Close").Return().Once()

    listener := TdhContractsListener{
        ethClient: mEthClient,
    }

    listener.Close()
    mEthClient.AssertExpectations(t)
}
