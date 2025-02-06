package tokens

type TransferType string

func (t TransferType) String() string {
	return string(t)
}

const (
	SALE    TransferType = "SALE"
	OTHER   TransferType = "OTHER"
	AIRDROP TransferType = "AIRDROPPED"
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
