package eth

import (
	"context"
	"os"
	"testing"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/stretchr/testify/require"
)

func TestTdhTransfersReceivedAction_Handle(t *testing.T) {
	// Setup in-memory BadgerDB
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	badgerDB, err := db.OpenBadger(tmpDir)
	require.NoError(t, err)

	// Initialize the action
	action := NewTdhTransfersReceivedActionImpl(badgerDB)

	// Define test cases
	tests := []struct {
		name      string
		transfers []tokens.TokenTransfer
		expectErr bool
	}{
		{
			name: "Valid transfer",
			transfers: []tokens.TokenTransfer{
				{
					BlockNumber: 1,
					TxHash:      "0xabc",
					From:        "0x0000000000000000000000000000000000000000",
					To:          "0x456",
					TokenID:     "0x1",
					Amount:      1,
					Contract:    "0xcontract",
				},
			},
			expectErr: false,
		},
		{
			name: "Insufficient balance",
			transfers: []tokens.TokenTransfer{
				{
					BlockNumber: 1,
					TxHash:      "0xdef",
					From:        "0x789",
					To:          "0x456",
					TokenID:     "0x2",
					Amount:      10, // Sender does not have enough balance
					Contract:    "0xcontract",
				},
			},
			expectErr: true,
		},
	}

	// Run test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := action.Handle(context.Background(), tt.transfers)
			if tt.expectErr {
				require.Error(t, err, "Expected an error but got none")
			} else {
				require.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}
