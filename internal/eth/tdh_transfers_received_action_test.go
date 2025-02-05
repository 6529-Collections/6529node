package eth

import (
	"context"
	"testing"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
)

func TestTdhTransfersReceivedAction_Handle(t *testing.T) {
	action := &DefaultTdhTransfersReceivedAction{}
	err := action.Handle(context.Background(), []tokens.TokenTransfer{
		{
			BlockNumber: 1,
			TxHash:      "0x123",
			From:        "0x123",
			To:          "0x123",
			TokenID:     "0x123",
			Amount:      1,
			Contract:    "0x123",
		},
	})
	if err != nil {
		t.Fatalf("error handling transfers: %v", err)
	}
}
