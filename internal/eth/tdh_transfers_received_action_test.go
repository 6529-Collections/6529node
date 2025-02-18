package eth

import (
	"context"
	"testing"

	"github.com/6529-Collections/6529node/internal/eth/mocks"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/stretchr/testify/mock"
)

func TestTdhTransfersReceivedAction_Handle(t *testing.T) {
	progressTracker := mocks.NewTdhIdxTrackerDb(t)
	action := &DefaultTdhTransfersReceivedAction{
		ctx:             context.Background(),
		progressTracker: progressTracker,
	}
	progressTracker.On("SetProgress", uint64(1), mock.Anything).Return(nil)
	trBatch := tokens.TokenTransferBatch{
		Transfers: []tokens.TokenTransfer{
			{
				BlockNumber: 1,
				TxHash:      "0x123",
				From:        "0x123",
				To:          "0x123",
				TokenID:     "0x123",
				Amount:      1,
				Contract:    "0x123",
			},
		},
		BlockNumber: 1,
	}
	err := action.Handle(trBatch)
	if err != nil {
		t.Fatalf("error handling transfers: %v", err)
	}
}
