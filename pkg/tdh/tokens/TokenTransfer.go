package tokens

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
}
