package models

type TransferType string

func (t TransferType) String() string {
	return string(t)
}

const (
	SALE    TransferType = "SALE"
	SEND    TransferType = "SEND"
	AIRDROP TransferType = "AIRDROP"
	MINT    TransferType = "MINT"
	BURN    TransferType = "BURN"
)

type TokenTransfer struct {
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
	Amount           int64
	Type             TransferType
}

type TokenTransferBatch struct {
	Transfers   []TokenTransfer
	BlockNumber uint64
}
