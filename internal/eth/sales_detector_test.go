package eth

import (
	"context"
	"math/big"
	"testing"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/6529-Collections/6529node/internal/eth/mocks"
)

func makeTxWithValue(value *big.Int) *types.Transaction {
	return types.NewTransaction(
		0,
		common.HexToAddress("0x1234"),
		value,
		21000,
		big.NewInt(1),
		nil,
	)
}

func addressToTopic(addr common.Address) common.Hash {
	return common.BytesToHash(common.LeftPadBytes(addr.Bytes(), 32))
}

func TestDefaultSalesDetector(t *testing.T) {
	testTxHash := common.HexToHash("0xABC")
	nftTransfers := []tokens.TokenTransfer{{
		BlockNumber: 12345,
		TxHash:      "0xABC",
		From:        "0xSeller",
		To:          "0xBuyer",
		Contract:    "0xSomeNft",
		TokenID:     "42",
	}}

	t.Run("No receipt => OTHER", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(nil, nil).Once()
		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(makeTxWithValue(big.NewInt(0)), false, nil).Maybe()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, nftTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.OTHER, results[0])

		mockClient.AssertExpectations(t)
	})

	t.Run("Nonzero Value => SALE", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		baseReceipt := &types.Receipt{
			Status: types.ReceiptStatusSuccessful,
			TxHash: testTxHash,
			Logs:   []*types.Log{},
		}
		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(baseReceipt, nil).Once()

		txWithValue := makeTxWithValue(big.NewInt(1_000_000_000_000_000_000))
		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(txWithValue, false, nil).Once()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, nftTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.SALE, results[0])

		mockClient.AssertExpectations(t)
	})

	t.Run("Blur event => SALE", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		blurLog := &types.Log{
			Address: common.HexToAddress("0x1234"),
			Topics:  []common.Hash{blurSignature},
		}
		receiptWithBlur := &types.Receipt{
			Status: types.ReceiptStatusSuccessful,
			TxHash: testTxHash,
			Logs:   []*types.Log{blurLog},
		}

		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(receiptWithBlur, nil).
			Once()

		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(makeTxWithValue(big.NewInt(0)), false, nil).
			Maybe()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, nftTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.SALE, results[0])

		mockClient.AssertExpectations(t)
	})

	t.Run("Seaport => SALE", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		type spentItem struct {
			ItemType   uint8
			Token      common.Address
			Identifier *big.Int
			Amount     *big.Int
		}
		type receivedItem struct {
			ItemType   uint8
			Token      common.Address
			Identifier *big.Int
			Amount     *big.Int
			Recipient  common.Address
		}

		zeroHash := [32]byte{}

		theOffer := []spentItem{
			{
				ItemType:   2,
				Token:      common.HexToAddress("0xSomeNft"),
				Identifier: big.NewInt(42),
				Amount:     big.NewInt(1),
			},
		}
		theConsideration := []receivedItem{}

		nonIndexed := seaportAbi.Events["OrderFulfilled"].Inputs.NonIndexed()

		encodedData, err := nonIndexed.Pack(
			zeroHash,
			common.HexToAddress("0xRecipient"),
			theOffer,
			theConsideration,
		)
		require.NoError(t, err, "should pack minimal OrderFulfilled event data")

		seaportLog := &types.Log{
			Address: common.HexToAddress(openseaAddress),
			Topics: []common.Hash{
				seaportFullfilledSig,
				addressToTopic(common.HexToAddress("0xOfferer")),
				addressToTopic(common.HexToAddress("0xZone")),
			},
			Data: encodedData,
		}

		receiptWithSeaport := &types.Receipt{
			Status: types.ReceiptStatusSuccessful,
			TxHash: testTxHash,
			Logs:   []*types.Log{seaportLog},
		}

		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(receiptWithSeaport, nil).Once()
		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(makeTxWithValue(big.NewInt(0)), false, nil).
			Maybe()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, nftTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.SALE, results[0])

		mockClient.AssertExpectations(t)
	})

	t.Run("WETH aggregator => SALE", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		aggregator := common.HexToAddress("0x00000000006c3852cbEf3e08E8df289169ede581")
		seller := common.HexToAddress("0xSeller")

		wethLog := &types.Log{
			Address: wethTokenAddress,
			Topics: []common.Hash{
				erc20TransferSig,
				addressToTopic(aggregator),
				addressToTopic(seller),
			},
			Data: big.NewInt(123456).Bytes(),
		}
		receiptWithWeth := &types.Receipt{
			Status: types.ReceiptStatusSuccessful,
			TxHash: testTxHash,
			Logs:   []*types.Log{wethLog},
		}

		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(receiptWithWeth, nil).Once()

		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(makeTxWithValue(big.NewInt(0)), false, nil).
			Maybe()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, nftTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.SALE, results[0])

		mockClient.AssertExpectations(t)
	})

	t.Run("No triggers => OTHER", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		noSaleReceipt := &types.Receipt{
			Status: types.ReceiptStatusSuccessful,
			TxHash: testTxHash,
			Logs:   []*types.Log{},
		}

		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(noSaleReceipt, nil).Once()

		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(makeTxWithValue(big.NewInt(0)), false, nil).
			Maybe()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, nftTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.OTHER, results[0])

		mockClient.AssertExpectations(t)
	})

	t.Run("Mint: no sale => AIRDROP", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		mintTransfers := []tokens.TokenTransfer{{
			From:     "0x0000000000000000000000000000000000000000",
			To:       "0xSomeUser",
			Contract: "0xSomeNft",
			TokenID:  "123",
		}}

		noSaleReceipt := &types.Receipt{
			Status: types.ReceiptStatusSuccessful,
			TxHash: testTxHash,
			Logs:   []*types.Log{},
		}

		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(noSaleReceipt, nil).Once()

		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(makeTxWithValue(big.NewInt(0)), false, nil).Once()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, mintTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.AIRDROP, results[0], "MINT + no sale => AIRDROP")

		mockClient.AssertExpectations(t)
	})

	t.Run("Mint: sale => keep MINT (paid mint)", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		mintTransfers := []tokens.TokenTransfer{{
			From:     "0x0000000000000000000000000000000000000000",
			To:       "0xSomeUser",
			Contract: "0xSomeNft",
			TokenID:  "999",
		}}

		saleReceipt := &types.Receipt{
			Status: types.ReceiptStatusSuccessful,
			TxHash: testTxHash,
			Logs:   []*types.Log{},
		}
		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(saleReceipt, nil).Once()

		txWithValue := makeTxWithValue(big.NewInt(1000000))
		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(txWithValue, false, nil).Once()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, mintTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.MINT, results[0], "Paid mint remains MINT")

		mockClient.AssertExpectations(t)
	})

	t.Run("Burn: no sale => remains BURN", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		burnTransfers := []tokens.TokenTransfer{{
			From:     "0xSomeUser",
			To:       "0x0000000000000000000000000000000000000000",
			Contract: "0xSomeNft",
			TokenID:  "77",
		}}

		noSaleReceipt := &types.Receipt{
			Status: types.ReceiptStatusSuccessful,
			TxHash: testTxHash,
			Logs:   []*types.Log{},
		}

		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(noSaleReceipt, nil).Once()

		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(makeTxWithValue(big.NewInt(0)), false, nil).Once()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, burnTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.BURN, results[0], "Burn + no sale => still BURN")

		mockClient.AssertExpectations(t)
	})

	t.Run("Burn: with sale indicator => still BURN", func(t *testing.T) {
		mockClient := new(mocks.EthClient)
		detector := NewDefaultSalesDetector(mockClient)

		burnTransfers := []tokens.TokenTransfer{{
			From:     "0xSomeUser",
			To:       "0x0000000000000000000000000000000000000000",
			Contract: "0xSomeNft",
			TokenID:  "12",
		}}

		saleReceipt := &types.Receipt{
			Status: types.ReceiptStatusSuccessful,
			TxHash: testTxHash,
			Logs:   []*types.Log{},
		}
		mockClient.On("TransactionReceipt", mock.Anything, testTxHash).
			Return(saleReceipt, nil).Once()

		txWithValue := makeTxWithValue(big.NewInt(9999))
		mockClient.On("TransactionByHash", mock.Anything, testTxHash).
			Return(txWithValue, false, nil).Once()

		results, err := detector.DetectIfSale(context.Background(), testTxHash, burnTransfers)
		require.NoError(t, err)
		assert.Equal(t, tokens.BURN, results[0], "Burn remains BURN even if a sale is indicated")

		mockClient.AssertExpectations(t)
	})
}
