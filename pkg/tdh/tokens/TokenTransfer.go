package tokens

import "strings"

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
	TxHash           string
	EventName        string
	From             string
	To               string
	Contract         string
	TokenID          string
	Amount           int64
	Type             TransferType
}

func Normalize(t *TokenTransfer) *TokenTransfer {
	t.TxHash = strings.ToLower(t.TxHash)
	t.Contract = strings.ToLower(t.Contract)
	t.From = strings.ToLower(t.From)
	t.To = strings.ToLower(t.To)
	return t
}
