// Code generated by mockery v2.51.0. DO NOT EDIT.

package mocks

import (
	context "context"

	eth "github.com/6529-Collections/6529node/internal/eth"
	mock "github.com/stretchr/testify/mock"

	tokens "github.com/6529-Collections/6529node/pkg/tdh/tokens"
)

// TokensTransfersWatcher is an autogenerated mock type for the TokensTransfersWatcher type
type TokensTransfersWatcher struct {
	mock.Mock
}

// WatchTransfers provides a mock function with given fields: ctx, client, contracts, startBlock, transfersChan, latestBlockChan
func (_m *TokensTransfersWatcher) WatchTransfers(ctx context.Context, client eth.EthClient, contracts []string, startBlock uint64, transfersChan chan<- []tokens.TokenTransfer, latestBlockChan chan<- uint64) error {
	ret := _m.Called(ctx, client, contracts, startBlock, transfersChan, latestBlockChan)

	if len(ret) == 0 {
		panic("no return value specified for WatchTransfers")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, eth.EthClient, []string, uint64, chan<- []tokens.TokenTransfer, chan<- uint64) error); ok {
		r0 = rf(ctx, client, contracts, startBlock, transfersChan, latestBlockChan)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewTokensTransfersWatcher creates a new instance of TokensTransfersWatcher. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewTokensTransfersWatcher(t interface {
	mock.TestingT
	Cleanup(func())
}) *TokensTransfersWatcher {
	mock := &TokensTransfersWatcher{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
