package eth

import (
	"errors"
	"math/big"
	"strings"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

var erc1155ABI abi.ABI
var erc721TransferSig = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

func init() {
	erc1155Abi, err := abi.JSON(strings.NewReader(`[
    {
        "anonymous": false,
        "inputs": [
            {"indexed": true, "name": "operator", "type": "address"},
            {"indexed": true, "name": "from",     "type": "address"},
            {"indexed": true, "name": "to",       "type": "address"},
            {"indexed": false,"name": "id",       "type": "uint256"},
            {"indexed": false,"name": "value",    "type": "uint256"}
        ],
        "name": "TransferSingle",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": true, "name": "operator", "type": "address"},
            {"indexed": true, "name": "from",     "type": "address"},
            {"indexed": true, "name": "to",       "type": "address"},
            {"indexed": false,"name": "ids",      "type": "uint256[]"},
            {"indexed": false,"name": "values",   "type": "uint256[]"}
        ],
        "name": "TransferBatch",
        "type": "event"
    }
	]`))
	if err != nil {
		panic("failed to parse ERC1155 ABI")
	}
	erc1155ABI = erc1155Abi
}

type EthTransactionLogsDecoder interface {
	Decode(allLogs []types.Log) [][]tokens.TokenTransfer
}

type DefaultEthTransactionLogsDecoder struct {
}

func NewDefaultEthTransactionLogsDecoder() *DefaultEthTransactionLogsDecoder {
	return &DefaultEthTransactionLogsDecoder{}
}

func (d *DefaultEthTransactionLogsDecoder) Decode(allLogs []types.Log) [][]tokens.TokenTransfer {
	blocks := map[uint64][]tokens.TokenTransfer{}
	for _, lg := range allLogs {
		if len(lg.Topics) == 0 {
			continue
		}
		blockNum := lg.BlockNumber
		erc1155transferSingleSig := erc1155ABI.Events["TransferSingle"].ID
		erc1155transferBatchSig := erc1155ABI.Events["TransferBatch"].ID
		sig := lg.Topics[0]
		if sig == erc721TransferSig {
			if len(lg.Topics) == 4 {
				from := common.HexToAddress(lg.Topics[1].Hex())
				to := common.HexToAddress(lg.Topics[2].Hex())
				tokenId := new(big.Int).SetBytes(lg.Topics[3].Bytes())
				blocks[blockNum] = append(blocks[lg.BlockNumber], tokens.TokenTransfer{
					BlockNumber:      lg.BlockNumber,
					TxHash:           lg.TxHash.Hex(),
					Contract:         lg.Address.Hex(),
					EventName:        "Transfer",
					From:             from.Hex(),
					To:               to.Hex(),
					TokenID:          tokenId.String(),
					Amount:           1,
					TransactionIndex: uint64(lg.TxIndex),
					LogIndex:         uint64(lg.Index),
				})
			}
		} else if sig == erc1155transferSingleSig || sig == erc1155transferBatchSig {
			switch sig {
			case erc1155transferSingleSig:
				actions, err := decodeTransferSingle(lg)
				if err != nil {
					zap.L().Error("error decoding TransferSingle", zap.Error(err))
					continue
				}
				blocks[blockNum] = append(blocks[blockNum], actions...)

			case erc1155transferBatchSig:
				actions, err := decodeTransferBatch(lg)
				if err != nil {
					zap.L().Error("error decoding TransferBatch", zap.Error(err))
					continue
				}
				blocks[blockNum] = append(blocks[blockNum], actions...)
			}
		}
	}
	var result [][]tokens.TokenTransfer
	for b := range blocks {
		result = append(result, blocks[b])
	}

	return result
}

func decodeTransferSingle(lg types.Log) ([]tokens.TokenTransfer, error) {
	if len(lg.Topics) < 4 {
		return nil, errors.New("invalid TransferSingle topics length")
	}
	from := common.HexToAddress(lg.Topics[2].Hex())
	to := common.HexToAddress(lg.Topics[3].Hex())

	var transferData struct {
		ID    *big.Int `abi:"id"`
		Value *big.Int `abi:"value"`
	}
	if err := erc1155ABI.UnpackIntoInterface(&transferData, "TransferSingle", lg.Data); err != nil {
		return nil, err
	}

	action := tokens.TokenTransfer{
		BlockNumber:      lg.BlockNumber,
		TxHash:           lg.TxHash.Hex(),
		Contract:         lg.Address.Hex(),
		EventName:        "TransferSingle",
		From:             from.Hex(),
		To:               to.Hex(),
		TokenID:          transferData.ID.String(),
		Amount:           transferData.Value.Int64(),
		TransactionIndex: uint64(lg.TxIndex),
		LogIndex:         uint64(lg.Index),
	}
	return []tokens.TokenTransfer{action}, nil
}

func decodeTransferBatch(lg types.Log) ([]tokens.TokenTransfer, error) {
	if len(lg.Topics) < 4 {
		return nil, errors.New("invalid TransferBatch topics length")
	}

	from := common.HexToAddress(lg.Topics[2].Hex())
	to := common.HexToAddress(lg.Topics[3].Hex())

	var batchData struct {
		Ids    []*big.Int `abi:"ids"`
		Values []*big.Int `abi:"values"`
	}
	if err := erc1155ABI.UnpackIntoInterface(&batchData, "TransferBatch", lg.Data); err != nil {
		return nil, err
	}

	var actions []tokens.TokenTransfer
	for i := 0; i < len(batchData.Ids); i++ {
		actions = append(actions, tokens.TokenTransfer{
			BlockNumber:      lg.BlockNumber,
			TxHash:           lg.TxHash.Hex(),
			Contract:         lg.Address.Hex(),
			EventName:        "TransferBatch",
			From:             from.Hex(),
			To:               to.Hex(),
			TokenID:          batchData.Ids[i].String(),
			Amount:           batchData.Values[i].Int64(),
			TransactionIndex: uint64(lg.TxIndex),
			LogIndex:         uint64(lg.Index),
		})
	}
	return actions, nil
}
