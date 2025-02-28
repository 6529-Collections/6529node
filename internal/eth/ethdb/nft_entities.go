package ethdb

import (
	"github.com/6529-Collections/6529node/pkg/tdh/models"
)

type NFT struct {
	Contract    string `json:"contract"`
	TokenID     string `json:"token_id"`
	Supply      uint64 `json:"supply"`
	BurntSupply uint64 `json:"burnt_supply"`
}

type NFTOwner struct {
	Owner         string `json:"owner"`
	Contract      string `json:"contract"`
	TokenID       string `json:"token_id"`
	TokenUniqueID uint64 `json:"token_unique_id"`
	Timestamp     uint64 `json:"timestamp"`
}

type NFTTransfer struct {
	BlockNumber      uint64              `json:"block_number"`
	TransactionIndex uint64              `json:"transaction_index"`
	LogIndex         uint64              `json:"log_index"`
	BlockTime        uint64              `json:"block_time"`
	TxHash           string              `json:"tx_hash"`
	EventName        string              `json:"event_name"`
	From             string              `json:"from"`
	To               string              `json:"to"`
	Contract         string              `json:"contract"`
	TokenID          string              `json:"token_id"`
	TokenUniqueID    uint64              `json:"token_unique_id"`
	Type             models.TransferType `json:"type"`
}

type TokenTransferCheckpoint struct {
	ID               uint64
	BlockNumber      uint64
	TransactionIndex uint64
	LogIndex         uint64
}
