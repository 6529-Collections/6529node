package ethdb

import (
	"github.com/6529-Collections/6529node/pkg/tdh/models"
)

type NFT struct {
	Contract    string
	TokenID     string
	Supply      uint64
	BurntSupply uint64
}

type NFTOwner struct {
	Owner         string
	Contract      string
	TokenID       string
	TokenUniqueID uint64
	Timestamp     uint64
}

type NFTTransfer struct {
	BlockNumber      uint64
	TransactionIndex uint64
	LogIndex         uint64
	BlockTime        uint64
	TxHash           string
	EventName        string
	From             string
	To               string
	Contract         string
	TokenID          string
	TokenUniqueID    uint64
	Type             models.TransferType
}

type TokenTransferCheckpoint struct {
	ID               uint64
	BlockNumber      uint64
	TransactionIndex uint64
	LogIndex         uint64
}
