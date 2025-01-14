package eth

import (
	"math/big"
	"strings"
	"testing"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
)

func MakeTransferSingleData(t *testing.T, id *big.Int, value *big.Int) []byte {
	event := erc1155ABI.Events["TransferSingle"]
	data, err := event.Inputs.NonIndexed().Pack(id, value)
	require.NoError(t, err, "failed to pack TransferSingle data")
	return data
}

func MakeTransferBatchData(t *testing.T, ids []*big.Int, values []*big.Int) []byte {
	event := erc1155ABI.Events["TransferBatch"]
	data, err := event.Inputs.NonIndexed().Pack(ids, values)
	require.NoError(t, err, "failed to pack TransferBatch data")
	return data
}

func TestDefaultEthTransactionLogsDecoder_Decode(t *testing.T) {
	decoder := &DefaultEthTransactionLogsDecoder{}

	t.Run("no logs -> empty result", func(t *testing.T) {
		res := decoder.Decode(nil)
		require.Empty(t, res, "Expect empty slice when no logs provided")
	})

	t.Run("log with no topics -> skipped", func(t *testing.T) {
		logs := []types.Log{
			{
				BlockNumber: 100,
				TxHash:      common.HexToHash("0xabc"),
			},
		}
		res := decoder.Decode(logs)
		require.Empty(t, res, "Log with no topics should be skipped")
	})

	t.Run("ERC721 Transfer with correct 4 topics -> single result", func(t *testing.T) {
		fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
		toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
		tokenId := big.NewInt(999)

		tokenIdBytes := common.LeftPadBytes(tokenId.Bytes(), 32)

		logs := []types.Log{
			{
				Address:     common.HexToAddress("0x0123456789abcdef"),
				BlockNumber: 50,
				TxHash:      common.HexToHash("0x123"),
				Topics: []common.Hash{
					erc721TransferSig,
					common.BytesToHash(fromAddr.Bytes()),
					common.BytesToHash(toAddr.Bytes()),
					common.BytesToHash(tokenIdBytes),
				},
			},
		}
		res := decoder.Decode(logs)
		require.Len(t, res, 1, "Should be one block group in the result")
		require.Len(t, res[0], 1, "Should decode exactly one transfer event")

		transfer := res[0][0]
		require.Equal(t, uint64(50), transfer.BlockNumber)
		require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000000000123", transfer.TxHash)
		require.Equal(t, "0x0000000000000000000000000123456789abcdef", strings.ToLower(transfer.Contract))
		require.Equal(t, "Transfer", transfer.EventName)
		require.Equal(t, fromAddr.Hex(), transfer.From)
		require.Equal(t, toAddr.Hex(), transfer.To)
		require.Equal(t, tokenId.String(), transfer.TokenID)
		require.EqualValues(t, 1, transfer.Amount)
	})

	t.Run("ERC721 Transfer with insufficient topics -> skipped", func(t *testing.T) {
		logs := []types.Log{
			{
				Address:     common.HexToAddress("0xcontractERC721"),
				BlockNumber: 51,
				TxHash:      common.HexToHash("0xabc"),
				Topics: []common.Hash{
					erc721TransferSig,
				},
			},
		}
		res := decoder.Decode(logs)
		require.Empty(t, res, "Insufficient topics means skip")
	})

	t.Run("ERC1155 TransferSingle with valid data -> single result", func(t *testing.T) {
		fromAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")
		toAddr := common.HexToAddress("0x4444444444444444444444444444444444444444")
		id := big.NewInt(1000)
		value := big.NewInt(500)

		logs := []types.Log{
			{
				Address:     common.HexToAddress("0xcontractERC1155"),
				BlockNumber: 60,
				TxHash:      common.HexToHash("0x321"),
				Topics: []common.Hash{
					erc1155ABI.Events["TransferSingle"].ID,
					common.HexToHash("0xoperator"),
					common.BytesToHash(fromAddr.Bytes()),
					common.BytesToHash(toAddr.Bytes()),
				},
				Data: MakeTransferSingleData(t, id, value),
			},
		}
		res := decoder.Decode(logs)
		require.Len(t, res, 1, "One block group in the result")
		require.Len(t, res[0], 1, "One decoded event")

		xfer := res[0][0]
		require.Equal(t, uint64(60), xfer.BlockNumber)
		require.Equal(t, common.HexToHash("0x321").String(), xfer.TxHash)
		require.Equal(t, "TransferSingle", xfer.EventName)
		require.Equal(t, fromAddr.Hex(), xfer.From)
		require.Equal(t, toAddr.Hex(), xfer.To)
		require.Equal(t, id.String(), xfer.TokenID)
		require.EqualValues(t, value.Int64(), xfer.Amount)
	})

	t.Run("ERC1155 TransferSingle with not enough topics -> error skipped", func(t *testing.T) {
		logs := []types.Log{
			{
				BlockNumber: 61,
				TxHash:      common.HexToHash("0x999"),
				Topics: []common.Hash{
					erc721TransferSig,
				},
				Data: nil,
			},
		}
		res := decoder.Decode(logs)
		require.Empty(t, res, "Should skip due to invalid TransferSingle topics length")
	})

	t.Run("ERC1155 TransferSingle with invalid data -> decode error skipped", func(t *testing.T) {
		fromAddr := common.HexToAddress("0x5555555555555555555555555555555555555555")
		toAddr := common.HexToAddress("0x6666666666666666666666666666666666666666")

		invalidData := []byte("somegarbage")

		logs := []types.Log{
			{
				Address:     common.HexToAddress("0xcontractERC1155"),
				BlockNumber: 62,
				TxHash:      common.HexToHash("0x777"),
				Topics: []common.Hash{
					erc1155ABI.Events["TransferSingle"].ID,
					common.HexToHash("0xoperator"),
					common.BytesToHash(fromAddr.Bytes()),
					common.BytesToHash(toAddr.Bytes()),
				},
				Data: invalidData,
			},
		}
		res := decoder.Decode(logs)
		require.Empty(t, res, "Should skip logs that fail to decode properly")
	})

	t.Run("ERC1155 TransferBatch with valid data -> multiple results", func(t *testing.T) {
		fromAddr := common.HexToAddress("0x7777777777777777777777777777777777777777")
		toAddr := common.HexToAddress("0x8888888888888888888888888888888888888888")
		ids := []*big.Int{big.NewInt(1), big.NewInt(2), big.NewInt(3)}
		values := []*big.Int{big.NewInt(10), big.NewInt(20), big.NewInt(30)}

		logs := []types.Log{
			{
				Address:     common.HexToAddress("0xcontractERC1155batch"),
				BlockNumber: 70,
				TxHash:      common.HexToHash("0xaaa"),
				Topics: []common.Hash{
					erc1155ABI.Events["TransferBatch"].ID,
					common.HexToHash("0xoperator"),
					common.BytesToHash(fromAddr.Bytes()),
					common.BytesToHash(toAddr.Bytes()),
				},
				Data: MakeTransferBatchData(t, ids, values),
			},
		}
		res := decoder.Decode(logs)
		require.Len(t, res, 1, "One block group in the result")
		require.Len(t, res[0], 3, "Three decoded events from TransferBatch")

		for i, xfer := range res[0] {
			require.Equal(t, uint64(70), xfer.BlockNumber)
			require.Equal(t, common.HexToHash("0xaaa").String(), xfer.TxHash)
			require.Equal(t, "TransferBatch", xfer.EventName)
			require.Equal(t, fromAddr.Hex(), xfer.From)
			require.Equal(t, toAddr.Hex(), xfer.To)
			require.Equal(t, ids[i].String(), xfer.TokenID)
			require.EqualValues(t, values[i].Int64(), xfer.Amount)
		}
	})

	t.Run("ERC1155 TransferBatch with not enough topics -> skipped", func(t *testing.T) {
		logs := []types.Log{
			{
				BlockNumber: 71,
				TxHash:      common.HexToHash("0xbbb"),
				Topics: []common.Hash{
					erc1155ABI.Events["TransferBatch"].ID,
					common.HexToHash("0xoperator"),
				},
				Data: nil,
			},
		}
		res := decoder.Decode(logs)
		require.Empty(t, res, "Should skip logs with invalid TransferBatch topics length")
	})

	t.Run("ERC1155 TransferBatch with decode error -> skipped", func(t *testing.T) {
		fromAddr := common.HexToAddress("0x9999999999999999999999999999999999999999")
		toAddr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

		invalidData := []byte("batchgarbage")

		logs := []types.Log{
			{
				Address:     common.HexToAddress("0xcontractERC1155batch"),
				BlockNumber: 72,
				TxHash:      common.HexToHash("0xccc"),
				Topics: []common.Hash{
					erc1155ABI.Events["TransferBatch"].ID,
					common.HexToHash("0xoperator"),
					common.BytesToHash(fromAddr.Bytes()),
					common.BytesToHash(toAddr.Bytes()),
				},
				Data: invalidData,
			},
		}
		res := decoder.Decode(logs)
		require.Empty(t, res, "Should skip logs that fail to decode TransferBatch properly")
	})

	t.Run("Multiple logs from different blocks -> group properly", func(t *testing.T) {
		fromAddr721 := common.HexToAddress("0x1111111111111111111111111111111111111111")
		toAddr721 := common.HexToAddress("0x2222222222222222222222222222222222222222")
		tokenId721 := big.NewInt(111)

		fromAddr1155 := common.HexToAddress("0x3333333333333333333333333333333333333333")
		toAddr1155 := common.HexToAddress("0x4444444444444444444444444444444444444444")
		id1155 := big.NewInt(999)
		value1155 := big.NewInt(1)

		logs := []types.Log{
			{
				Address:     common.HexToAddress("0xcontractERC721"),
				BlockNumber: 100,
				TxHash:      common.HexToHash("0x111"),
				Topics: []common.Hash{
					erc721TransferSig,
					common.BytesToHash(fromAddr721.Bytes()),
					common.BytesToHash(toAddr721.Bytes()),
					common.BytesToHash(common.LeftPadBytes(tokenId721.Bytes(), 32)),
				},
			},
			{
				Address:     common.HexToAddress("0xcontractERC1155"),
				BlockNumber: 101,
				TxHash:      common.HexToHash("0x222"),
				Topics: []common.Hash{
					erc1155ABI.Events["TransferSingle"].ID,
					common.HexToHash("0xoperator"),
					common.BytesToHash(fromAddr1155.Bytes()),
					common.BytesToHash(toAddr1155.Bytes()),
				},
				Data: MakeTransferSingleData(t, id1155, value1155),
			},
			{
				Address:     common.HexToAddress("0xcontractERC1155_2"),
				BlockNumber: 100,
				TxHash:      common.HexToHash("0x333"),
				Topics: []common.Hash{
					erc1155ABI.Events["TransferSingle"].ID,
					common.HexToHash("0xoperator"),
					common.BytesToHash(fromAddr1155.Bytes()),
					common.BytesToHash(toAddr1155.Bytes()),
				},
				Data: MakeTransferSingleData(t, big.NewInt(123), big.NewInt(456)),
			},
		}

		res := decoder.Decode(logs)
		require.Len(t, res, 2, "Expect two block groups (block 100 and block 101)")

		var block100Transfers, block101Transfers []tokens.TokenTransfer
		for _, group := range res {
			if len(group) > 0 {
				if group[0].BlockNumber == 100 {
					block100Transfers = group
				} else if group[0].BlockNumber == 101 {
					block101Transfers = group
				}
			}
		}

		require.Len(t, block100Transfers, 2, "2 transfers in block 100")
		require.Contains(t, []string{"Transfer", "TransferSingle"}, block100Transfers[0].EventName)
		require.Contains(t, []string{"Transfer", "TransferSingle"}, block100Transfers[1].EventName)

		require.Len(t, block101Transfers, 1, "1 transfer in block 101")
		require.Equal(t, "TransferSingle", block101Transfers[0].EventName)
		require.Equal(t, uint64(101), block101Transfers[0].BlockNumber)
		require.Equal(t, fromAddr1155.Hex(), block101Transfers[0].From)
		require.Equal(t, toAddr1155.Hex(), block101Transfers[0].To)
		require.Equal(t, id1155.String(), block101Transfers[0].TokenID)
		require.EqualValues(t, value1155.Int64(), block101Transfers[0].Amount)
	})

	t.Run("Unrelated or unknown topic -> ignored", func(t *testing.T) {
		randomTopic := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
		logs := []types.Log{
			{
				BlockNumber: 200,
				TxHash:      common.HexToHash("0x456"),
				Topics:      []common.Hash{randomTopic},
			},
		}
		res := decoder.Decode(logs)
		require.Empty(t, res, "Logs with unknown topic signature should be ignored")
	})
}
