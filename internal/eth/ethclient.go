package eth

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

var CreateEthClient = createEthClient

type EthClient interface {
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
	CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error)
	Close()
}

type EthHeadSubscription interface {
	Unsubscribe()
	Err() <-chan error
}

func createEthClient() (EthClient, error) {
	nodeUrl := config.Get().EthereumNodeUrl
	if nodeUrl == "" {
		return nil, errors.New("failed to configure Ethereum client - EthereumNodeUrl is not set")
	}
	client, err := ethclient.Dial(nodeUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to configure Ethereum client - %w", err)
	}
	return client, nil
}
