package tdh

import (
	"errors"
	"testing"
	"time"

	mocks "github.com/6529-Collections/6529node/internal/eth/mocks"
	transferwatcher "github.com/6529-Collections/6529node/internal/eth/mocks/transferwatcher"
	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func TestTdhContractsListener_Listen_ProgressGreaterThanEpochBlock(t *testing.T) {
	zap.ReplaceGlobals(zap.NewNop())

	mIdxTracker := new(mocks.TdhIdxTrackerDb)
	mTransfersAction := new(mocks.TdhTransfersReceivedAction)
	mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

	mIdxTracker.On("GetProgress").Return(uint64(13360878), nil).Once()
	expectedStartBlock := constants.TDH_CONTRACTS_EPOCH_BLOCK

	mIdxTracker.On("SetProgress", mock.AnythingOfType("uint64")).Return(nil).Maybe()

	mTransfersWatcher.
		On("WatchTransfers",
			[]string{constants.MEMES_CONTRACT, constants.GRADIENTS_CONTRACT, constants.NEXTGEN_CONTRACT},
			expectedStartBlock,
			mock.Anything,
			mock.Anything,
		).
		Return(nil).
		Run(func(args mock.Arguments) {
			batchChan := args.Get(2).(chan<- models.TokenTransferBatch)

			batchChan <- models.TokenTransferBatch{
				BlockNumber: 13360878,
				Transfers: []models.TokenTransfer{
					{From: "0x111", To: "0x222"},
				},
			}

			close(batchChan)
		})

	mTransfersAction.
		On("Handle", models.TokenTransferBatch{
			BlockNumber: 13360878,
			Transfers:   []models.TokenTransfer{{From: "0x111", To: "0x222"}},
		}).
		Return(nil).
		Once()

	listener := TdhContractsListener{
		transfersWatcher:        mTransfersWatcher,
		transfersReceivedAction: mTransfersAction,
		progressTracker:         mIdxTracker,
	}

	err := listener.listen(make(chan bool))
	assert.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	mIdxTracker.AssertExpectations(t)
	mTransfersWatcher.AssertExpectations(t)
	mTransfersAction.AssertExpectations(t)
}

func TestTdhContractsListener_Listen_ProgressLessThanEpochBlock(t *testing.T) {
	zap.ReplaceGlobals(zap.NewNop())

	mIdxTracker := new(mocks.TdhIdxTrackerDb)
	mTransfersAction := new(mocks.TdhTransfersReceivedAction)
	mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

	mIdxTracker.On("GetProgress").Return(uint64(13360860), nil).Once()

	mTransfersWatcher.
		On("WatchTransfers",
			[]string{constants.MEMES_CONTRACT, constants.GRADIENTS_CONTRACT, constants.NEXTGEN_CONTRACT},
			constants.TDH_CONTRACTS_EPOCH_BLOCK,
			mock.Anything,
			mock.Anything,
		).
		Return(nil).
		Run(func(args mock.Arguments) {
			batchChan := args.Get(2).(chan<- models.TokenTransferBatch)
			close(batchChan)
		})

	listener := TdhContractsListener{
		transfersWatcher:        mTransfersWatcher,
		transfersReceivedAction: mTransfersAction,
		progressTracker:         mIdxTracker,
	}

	err := listener.listen(make(chan bool))
	assert.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	mIdxTracker.AssertExpectations(t)
	mTransfersWatcher.AssertExpectations(t)
	mTransfersAction.AssertExpectations(t)
}

func TestTdhContractsListener_Listen_ErrorOnGetProgress(t *testing.T) {
	zap.ReplaceGlobals(zap.NewNop())

	mIdxTracker := new(mocks.TdhIdxTrackerDb)
	mTransfersAction := new(mocks.TdhTransfersReceivedAction)
	mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

	mIdxTracker.On("GetProgress").Return(uint64(0), errors.New("failed to get progress")).Once()

	listener := TdhContractsListener{
		transfersWatcher:        mTransfersWatcher,
		transfersReceivedAction: mTransfersAction,
		progressTracker:         mIdxTracker,
	}

	err := listener.listen(make(chan bool))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get progress")

	mTransfersWatcher.AssertNotCalled(t, "WatchTransfers", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mIdxTracker.AssertExpectations(t)
	mTransfersAction.AssertExpectations(t)
	mTransfersWatcher.AssertExpectations(t)
}

func TestTdhContractsListener_Listen_ErrorOnWatchTransfers(t *testing.T) {
	zap.ReplaceGlobals(zap.NewNop())

	mIdxTracker := new(mocks.TdhIdxTrackerDb)
	mTransfersAction := new(mocks.TdhTransfersReceivedAction)
	mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

	mIdxTracker.On("GetProgress").Return(uint64(13360878), nil).Once()

	mTransfersWatcher.
		On("WatchTransfers",
			mock.Anything,
			uint64(13360860),
			mock.Anything,
			mock.Anything,
		).
		Return(errors.New("watch transfers failed")).
		Once()

	listener := TdhContractsListener{
		transfersWatcher:        mTransfersWatcher,
		transfersReceivedAction: mTransfersAction,
		progressTracker:         mIdxTracker,
	}

	err := listener.listen(make(chan bool))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "watch transfers failed")

	mTransfersAction.AssertNotCalled(t, "Handle", mock.Anything)

	mIdxTracker.AssertExpectations(t)
	mTransfersAction.AssertExpectations(t)
	mTransfersWatcher.AssertExpectations(t)
}

func TestTdhContractsListener_Listen_HandleErrorStopsLoop(t *testing.T) {
	zap.ReplaceGlobals(zap.NewNop())

	mIdxTracker := new(mocks.TdhIdxTrackerDb)
	mTransfersAction := new(mocks.TdhTransfersReceivedAction)
	mTransfersWatcher := new(transferwatcher.TokensTransfersWatcher)

	mIdxTracker.On("GetProgress").Return(uint64(13360860), nil).Once()

	mTransfersWatcher.
		On("WatchTransfers",
			mock.Anything,
			uint64(13360860),
			mock.Anything,
			mock.Anything,
		).
		Return(nil).
		Run(func(args mock.Arguments) {
			batchChan := args.Get(2).(chan<- models.TokenTransferBatch)

			batchChan <- models.TokenTransferBatch{
				BlockNumber: 13360860,
				Transfers: []models.TokenTransfer{
					{From: "0xBAD"},
				},
			}

			batchChan <- models.TokenTransferBatch{
				BlockNumber: 13360860,
				Transfers: []models.TokenTransfer{
					{From: "0xGOOD"},
				},
			}

			close(batchChan)
		}).
		Once()

	mTransfersAction.On("Handle", models.TokenTransferBatch{
		BlockNumber: 13360860,
		Transfers:   []models.TokenTransfer{{From: "0xBAD"}},
	}).
		Return(errors.New("some handle error")).
		Once()

	mTransfersAction.On("Handle", models.TokenTransferBatch{
		BlockNumber: 13360860,
		Transfers:   []models.TokenTransfer{{From: "0xGOOD"}},
	}).
		Return(nil).
		Once()

	listener := TdhContractsListener{
		transfersWatcher:        mTransfersWatcher,
		transfersReceivedAction: mTransfersAction,
		progressTracker:         mIdxTracker,
	}

	err := listener.listen(make(chan bool))

	// Expect an error because Handle() for 0xBAD failed
	assert.Error(t, err, "Listen should fail if Handle fails")

	// Ensure Handle() for 0xGOOD was never called
	mTransfersAction.AssertNotCalled(t, "Handle", []models.TokenTransfer{{From: "0xGOOD"}})

	mIdxTracker.AssertExpectations(t)
	mTransfersWatcher.AssertExpectations(t)
}
