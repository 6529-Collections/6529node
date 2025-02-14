package eth

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

type SalesDetector interface {
	DetectIfSale(ctx context.Context, txHash common.Hash, nftTransfers []tokens.TokenTransfer) (map[int]tokens.TransferType, error)
}

type DefaultSalesDetector struct {
	ethClient    EthClient
	mu           sync.Mutex
	receiptCache map[common.Hash]*types.Receipt
}

var blurSignature = common.HexToHash("0x7dc5c0699ac8dd5250cbe368a2fc3b4a2daadb120ad07f6cccea29f83482686e")
var wethTokenAddress = common.HexToAddress("0xC02aaa39b223Fe8D0a0e5C4F27eAD9083C756Cc2")
var erc20TransferSig = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

var aggregatorAddresses = map[common.Address]bool{
	common.HexToAddress("0x00000000006c3852cbEf3e08E8df289169ede581"): true,
	common.HexToAddress("0x1E0049783F008A0085193E00003D00cd54003c71"): true,
	common.HexToAddress("0x000000000000Ad05Ccc4F10045630fb830B95127"): true,
}

var seaportAbi abi.ABI
var seaportFullfilledSig [32]byte

func init() {
	parsed, err := abi.JSON(strings.NewReader(`[
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "conduitController",
        "type": "address"
      }
    ],
    "stateMutability": "nonpayable",
    "type": "constructor"
  },
  {
    "inputs": [],
    "name": "BadContractSignature",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "BadFraction",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "token",
        "type": "address"
      },
      {
        "internalType": "address",
        "name": "from",
        "type": "address"
      },
      {
        "internalType": "address",
        "name": "to",
        "type": "address"
      },
      {
        "internalType": "uint256",
        "name": "amount",
        "type": "uint256"
      }
    ],
    "name": "BadReturnValueFromERC20OnTransfer",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint8",
        "name": "v",
        "type": "uint8"
      }
    ],
    "name": "BadSignatureV",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "CannotCancelOrder",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "ConsiderationCriteriaResolverOutOfRange",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "ConsiderationLengthNotEqualToTotalOriginal",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "orderIndex",
        "type": "uint256"
      },
      {
        "internalType": "uint256",
        "name": "considerationIndex",
        "type": "uint256"
      },
      {
        "internalType": "uint256",
        "name": "shortfallAmount",
        "type": "uint256"
      }
    ],
    "name": "ConsiderationNotMet",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "CriteriaNotEnabledForItem",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "token",
        "type": "address"
      },
      {
        "internalType": "address",
        "name": "from",
        "type": "address"
      },
      {
        "internalType": "address",
        "name": "to",
        "type": "address"
      },
      {
        "internalType": "uint256[]",
        "name": "identifiers",
        "type": "uint256[]"
      },
      {
        "internalType": "uint256[]",
        "name": "amounts",
        "type": "uint256[]"
      }
    ],
    "name": "ERC1155BatchTransferGenericFailure",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "InexactFraction",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "InsufficientNativeTokensSupplied",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "Invalid1155BatchTransferEncoding",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "InvalidBasicOrderParameterEncoding",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "conduit",
        "type": "address"
      }
    ],
    "name": "InvalidCallToConduit",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "bytes32",
        "name": "conduitKey",
        "type": "bytes32"
      },
      {
        "internalType": "address",
        "name": "conduit",
        "type": "address"
      }
    ],
    "name": "InvalidConduit",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "bytes32",
        "name": "orderHash",
        "type": "bytes32"
      }
    ],
    "name": "InvalidContractOrder",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "amount",
        "type": "uint256"
      }
    ],
    "name": "InvalidERC721TransferAmount",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "InvalidFulfillmentComponentData",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "value",
        "type": "uint256"
      }
    ],
    "name": "InvalidMsgValue",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "InvalidNativeOfferItem",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "InvalidProof",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "bytes32",
        "name": "orderHash",
        "type": "bytes32"
      }
    ],
    "name": "InvalidRestrictedOrder",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "InvalidSignature",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "InvalidSigner",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "startTime",
        "type": "uint256"
      },
      {
        "internalType": "uint256",
        "name": "endTime",
        "type": "uint256"
      }
    ],
    "name": "InvalidTime",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "fulfillmentIndex",
        "type": "uint256"
      }
    ],
    "name": "MismatchedFulfillmentOfferAndConsiderationComponents",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "enum Side",
        "name": "side",
        "type": "uint8"
      }
    ],
    "name": "MissingFulfillmentComponentOnAggregation",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "MissingItemAmount",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "MissingOriginalConsiderationItems",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "account",
        "type": "address"
      },
      {
        "internalType": "uint256",
        "name": "amount",
        "type": "uint256"
      }
    ],
    "name": "NativeTokenTransferGenericFailure",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "account",
        "type": "address"
      }
    ],
    "name": "NoContract",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "NoReentrantCalls",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "NoSpecifiedOrdersAvailable",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "OfferAndConsiderationRequiredOnFulfillment",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "OfferCriteriaResolverOutOfRange",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "bytes32",
        "name": "orderHash",
        "type": "bytes32"
      }
    ],
    "name": "OrderAlreadyFilled",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "enum Side",
        "name": "side",
        "type": "uint8"
      }
    ],
    "name": "OrderCriteriaResolverOutOfRange",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "bytes32",
        "name": "orderHash",
        "type": "bytes32"
      }
    ],
    "name": "OrderIsCancelled",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "bytes32",
        "name": "orderHash",
        "type": "bytes32"
      }
    ],
    "name": "OrderPartiallyFilled",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "PartialFillsNotEnabledForOrder",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "token",
        "type": "address"
      },
      {
        "internalType": "address",
        "name": "from",
        "type": "address"
      },
      {
        "internalType": "address",
        "name": "to",
        "type": "address"
      },
      {
        "internalType": "uint256",
        "name": "identifier",
        "type": "uint256"
      },
      {
        "internalType": "uint256",
        "name": "amount",
        "type": "uint256"
      }
    ],
    "name": "TokenTransferGenericFailure",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "orderIndex",
        "type": "uint256"
      },
      {
        "internalType": "uint256",
        "name": "considerationIndex",
        "type": "uint256"
      }
    ],
    "name": "UnresolvedConsiderationCriteria",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "orderIndex",
        "type": "uint256"
      },
      {
        "internalType": "uint256",
        "name": "offerIndex",
        "type": "uint256"
      }
    ],
    "name": "UnresolvedOfferCriteria",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "UnusedItemParameters",
    "type": "error"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "newCounter",
        "type": "uint256"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "offerer",
        "type": "address"
      }
    ],
    "name": "CounterIncremented",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "bytes32",
        "name": "orderHash",
        "type": "bytes32"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "offerer",
        "type": "address"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "zone",
        "type": "address"
      }
    ],
    "name": "OrderCancelled",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "bytes32",
        "name": "orderHash",
        "type": "bytes32"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "offerer",
        "type": "address"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "zone",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "address",
        "name": "recipient",
        "type": "address"
      },
      {
        "components": [
          {
            "internalType": "enum ItemType",
            "name": "itemType",
            "type": "uint8"
          },
          {
            "internalType": "address",
            "name": "token",
            "type": "address"
          },
          {
            "internalType": "uint256",
            "name": "identifier",
            "type": "uint256"
          },
          {
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
          }
        ],
        "indexed": false,
        "internalType": "struct SpentItem[]",
        "name": "offer",
        "type": "tuple[]"
      },
      {
        "components": [
          {
            "internalType": "enum ItemType",
            "name": "itemType",
            "type": "uint8"
          },
          {
            "internalType": "address",
            "name": "token",
            "type": "address"
          },
          {
            "internalType": "uint256",
            "name": "identifier",
            "type": "uint256"
          },
          {
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
          },
          {
            "internalType": "address payable",
            "name": "recipient",
            "type": "address"
          }
        ],
        "indexed": false,
        "internalType": "struct ReceivedItem[]",
        "name": "consideration",
        "type": "tuple[]"
      }
    ],
    "name": "OrderFulfilled",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "bytes32",
        "name": "orderHash",
        "type": "bytes32"
      },
      {
        "components": [
          {
            "internalType": "address",
            "name": "offerer",
            "type": "address"
          },
          {
            "internalType": "address",
            "name": "zone",
            "type": "address"
          },
          {
            "components": [
              {
                "internalType": "enum ItemType",
                "name": "itemType",
                "type": "uint8"
              },
              {
                "internalType": "address",
                "name": "token",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "identifierOrCriteria",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "startAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "endAmount",
                "type": "uint256"
              }
            ],
            "internalType": "struct OfferItem[]",
            "name": "offer",
            "type": "tuple[]"
          },
          {
            "components": [
              {
                "internalType": "enum ItemType",
                "name": "itemType",
                "type": "uint8"
              },
              {
                "internalType": "address",
                "name": "token",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "identifierOrCriteria",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "startAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "endAmount",
                "type": "uint256"
              },
              {
                "internalType": "address payable",
                "name": "recipient",
                "type": "address"
              }
            ],
            "internalType": "struct ConsiderationItem[]",
            "name": "consideration",
            "type": "tuple[]"
          },
          {
            "internalType": "enum OrderType",
            "name": "orderType",
            "type": "uint8"
          },
          {
            "internalType": "uint256",
            "name": "startTime",
            "type": "uint256"
          },
          {
            "internalType": "uint256",
            "name": "endTime",
            "type": "uint256"
          },
          {
            "internalType": "bytes32",
            "name": "zoneHash",
            "type": "bytes32"
          },
          {
            "internalType": "uint256",
            "name": "salt",
            "type": "uint256"
          },
          {
            "internalType": "bytes32",
            "name": "conduitKey",
            "type": "bytes32"
          },
          {
            "internalType": "uint256",
            "name": "totalOriginalConsiderationItems",
            "type": "uint256"
          }
        ],
        "indexed": false,
        "internalType": "struct OrderParameters",
        "name": "orderParameters",
        "type": "tuple"
      }
    ],
    "name": "OrderValidated",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "bytes32[]",
        "name": "orderHashes",
        "type": "bytes32[]"
      }
    ],
    "name": "OrdersMatched",
    "type": "event"
  },
  {
    "inputs": [
      {
        "components": [
          {
            "internalType": "address",
            "name": "offerer",
            "type": "address"
          },
          {
            "internalType": "address",
            "name": "zone",
            "type": "address"
          },
          {
            "components": [
              {
                "internalType": "enum ItemType",
                "name": "itemType",
                "type": "uint8"
              },
              {
                "internalType": "address",
                "name": "token",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "identifierOrCriteria",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "startAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "endAmount",
                "type": "uint256"
              }
            ],
            "internalType": "struct OfferItem[]",
            "name": "offer",
            "type": "tuple[]"
          },
          {
            "components": [
              {
                "internalType": "enum ItemType",
                "name": "itemType",
                "type": "uint8"
              },
              {
                "internalType": "address",
                "name": "token",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "identifierOrCriteria",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "startAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "endAmount",
                "type": "uint256"
              },
              {
                "internalType": "address payable",
                "name": "recipient",
                "type": "address"
              }
            ],
            "internalType": "struct ConsiderationItem[]",
            "name": "consideration",
            "type": "tuple[]"
          },
          {
            "internalType": "enum OrderType",
            "name": "orderType",
            "type": "uint8"
          },
          {
            "internalType": "uint256",
            "name": "startTime",
            "type": "uint256"
          },
          {
            "internalType": "uint256",
            "name": "endTime",
            "type": "uint256"
          },
          {
            "internalType": "bytes32",
            "name": "zoneHash",
            "type": "bytes32"
          },
          {
            "internalType": "uint256",
            "name": "salt",
            "type": "uint256"
          },
          {
            "internalType": "bytes32",
            "name": "conduitKey",
            "type": "bytes32"
          },
          {
            "internalType": "uint256",
            "name": "counter",
            "type": "uint256"
          }
        ],
        "internalType": "struct OrderComponents[]",
        "name": "orders",
        "type": "tuple[]"
      }
    ],
    "name": "cancel",
    "outputs": [
      {
        "internalType": "bool",
        "name": "cancelled",
        "type": "bool"
      }
    ],
    "stateMutability": "nonpayable",
    "type": "function"
  }
]`))
	if err != nil {
		panic("failed to parse Seaport ABI: " + err.Error())
	}
	seaportAbi = parsed
	seaportFullfilledSig = seaportAbi.Events["OrderFulfilled"].ID
}

func NewDefaultSalesDetector(client EthClient) *DefaultSalesDetector {
	return &DefaultSalesDetector{
		ethClient:    client,
		receiptCache: make(map[common.Hash]*types.Receipt),
	}
}

func (d *DefaultSalesDetector) DetectIfSale(
	ctx context.Context,
	txHash common.Hash,
	nftTransfers []tokens.TokenTransfer,
) (map[int]tokens.TransferType, error) {

	result := make(map[int]tokens.TransferType, len(nftTransfers))
	for i, tr := range nftTransfers {
		fromLower := strings.ToLower(tr.From)
		toLower := strings.ToLower(tr.To)
		switch {
		case fromLower == constants.NULL_ADDRESS:
			result[i] = tokens.MINT
		case toLower == constants.NULL_ADDRESS || toLower == constants.DEAD_ADDRESS:
			result[i] = tokens.BURN
		default:
			result[i] = tokens.SEND
		}
	}

	receipt, err := d.getOrFetchReceipt(ctx, txHash)
	if err != nil {
		return result, err
	}
	if receipt == nil {
		return result, nil
	}

	isSale, err := d.hasAnySaleIndicators(ctx, receipt, nftTransfers)
	if err != nil {
		return result, err
	}

	if isSale {
		for i := range nftTransfers {
			if result[i] == tokens.SEND {
				result[i] = tokens.SALE
			}
		}
	} else {
		for i := range nftTransfers {
			if result[i] == tokens.MINT {
				result[i] = tokens.AIRDROP
			}
		}
	}

	return result, nil
}

func (d *DefaultSalesDetector) hasAnySaleIndicators(
	ctx context.Context,
	receipt *types.Receipt,
	nftTransfers []tokens.TokenTransfer,
) (bool, error) {

	if receipt.Status == types.ReceiptStatusSuccessful && receipt.TxHash != (common.Hash{}) {
		tx, _, err := d.ethClient.TransactionByHash(ctx, receipt.TxHash)
		if err != nil {
			return false, err
		}
		if tx.Value().Sign() > 0 {
			return true, nil
		}
	}

	for _, lg := range receipt.Logs {
		if len(lg.Topics) == 0 {
			continue
		}
		signatureTopic := lg.Topics[0]

		if signatureTopic == blurSignature {
			return true, nil
		}

		if signatureTopic == seaportFullfilledSig {
			fulfilled, err := decodeSeaportOrderFulfilled(lg)
			if err != nil {
				zap.L().Warn("Seaport parse e0x00000000000000ADc04C56Bf30aC9d3c0aAF14dCrror", zap.Error(err), zap.String("txHash", lg.TxHash.Hex()))
				continue
			}

			isSale, err := matchesNFTTransfers(fulfilled, nftTransfers)
			if err != nil {
				zap.L().Warn("Seaport parse error", zap.Error(err), zap.String("txHash", lg.TxHash.Hex()))
				continue
			}
			if isSale {
				return true, nil
			}
		}

		if lg.Address == wethTokenAddress && len(lg.Topics) >= 3 && lg.Topics[0] == erc20TransferSig {
			from := common.HexToAddress(lg.Topics[1].Hex())
			to := common.HexToAddress(lg.Topics[2].Hex())
			value := new(big.Int).SetBytes(lg.Data)
			if value.Sign() <= 0 {
				continue
			}

			for _, tr := range nftTransfers {
				isFromBuyerOrAggregator := sameAddress(from, common.HexToAddress(tr.To)) || aggregatorAddresses[from]
				isToSellerOrAggregator := sameAddress(to, common.HexToAddress(tr.From)) || aggregatorAddresses[to]
				if isFromBuyerOrAggregator && isToSellerOrAggregator {
					return true, nil
				}
			}
		}
	}

	return false, nil
}

func decodeSeaportOrderFulfilled(lg *types.Log) (*SeaportOrderFulfilled, error) {
	var event SeaportOrderFulfilled
	err := seaportAbi.UnpackIntoInterface(&event, "OrderFulfilled", lg.Data)
	if err != nil {
		return nil, fmt.Errorf("unpack seaport event: %w", err)
	}
	if len(lg.Topics) >= 2 {
		event.Offerer = common.BytesToAddress(lg.Topics[1].Bytes())
	}
	if len(lg.Topics) >= 3 {
		event.Zone = common.BytesToAddress(lg.Topics[2].Bytes())
	}
	return &event, nil
}

type SeaportOrderFulfilled struct {
	OrderHash     [32]byte       `json:"orderHash"`
	Recipient     common.Address `json:"recipient"`
	Offer         []SpentItem    `json:"offer"`
	Consideration []ReceivedItem `json:"consideration"`

	Offerer common.Address
	Zone    common.Address
}

type SpentItem struct {
	ItemType   uint8          `json:"itemType"`
	Token      common.Address `json:"token"`
	Identifier *big.Int       `json:"identifier"`
	Amount     *big.Int       `json:"amount"`
}

type ReceivedItem struct {
	ItemType   uint8          `json:"itemType"`
	Token      common.Address `json:"token"`
	Identifier *big.Int       `json:"identifier"`
	Amount     *big.Int       `json:"amount"`
	Recipient  common.Address `json:"recipient"`
}

func matchesNFTTransfers(
	fulfilled *SeaportOrderFulfilled,
	nftTransfers []tokens.TokenTransfer,
) (bool, error) {

	for _, tr := range nftTransfers {
		for _, s := range fulfilled.Offer {
			if sameAddress(s.Token, common.HexToAddress(tr.Contract)) {
				tokenId, ok := new(big.Int).SetString(tr.TokenID, 10)
				if !ok {
					return false, fmt.Errorf("invalid token ID: %s", tr.TokenID)
				}
				if s.Identifier.Cmp(tokenId) == 0 {
					return true, nil
				}
			}
		}
		for _, c := range fulfilled.Consideration {
			if sameAddress(c.Token, common.HexToAddress(tr.Contract)) {
				tokenId, ok := new(big.Int).SetString(tr.TokenID, 10)
				if !ok {
					return false, fmt.Errorf("invalid token ID: %s", tr.TokenID)
				}
				if c.Identifier.Cmp(tokenId) == 0 {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (d *DefaultSalesDetector) getOrFetchReceipt(
	ctx context.Context,
	txHash common.Hash,
) (*types.Receipt, error) {
	d.mu.Lock()
	if rcp, ok := d.receiptCache[txHash]; ok {
		d.mu.Unlock()
		return rcp, nil
	}
	d.mu.Unlock()

	receipt, err := d.ethClient.TransactionReceipt(ctx, txHash)
	if err != nil {
		zap.L().Error("Error fetching transaction receipt for sale detection",
			zap.Error(err),
			zap.String("txHash", txHash.Hex()))
		return nil, err
	}

	d.mu.Lock()
	d.receiptCache[txHash] = receipt
	d.mu.Unlock()
	return receipt, nil
}

func sameAddress(a, b common.Address) bool {
	return strings.EqualFold(a.Hex(), b.Hex())
}
