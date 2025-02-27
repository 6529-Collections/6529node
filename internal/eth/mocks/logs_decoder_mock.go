// Code generated by mockery v2.51.0. DO NOT EDIT.

package mocks

import (
	models "github.com/6529-Collections/6529node/pkg/tdh/models"
	types "github.com/ethereum/go-ethereum/core/types"
	mock "github.com/stretchr/testify/mock"
)

// EthTransactionLogsDecoder is an autogenerated mock type for the EthTransactionLogsDecoder type
type EthTransactionLogsDecoder struct {
	mock.Mock
}

// Decode provides a mock function with given fields: allLogs
func (_m *EthTransactionLogsDecoder) Decode(allLogs []types.Log) ([][]models.TokenTransfer, error) {
	ret := _m.Called(allLogs)

	if len(ret) == 0 {
		panic("no return value specified for Decode")
	}

	var r0 [][]models.TokenTransfer
	var r1 error
	if rf, ok := ret.Get(0).(func([]types.Log) ([][]models.TokenTransfer, error)); ok {
		return rf(allLogs)
	}
	if rf, ok := ret.Get(0).(func([]types.Log) [][]models.TokenTransfer); ok {
		r0 = rf(allLogs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([][]models.TokenTransfer)
		}
	}

	if rf, ok := ret.Get(1).(func([]types.Log) error); ok {
		r1 = rf(allLogs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewEthTransactionLogsDecoder creates a new instance of EthTransactionLogsDecoder. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewEthTransactionLogsDecoder(t interface {
	mock.TestingT
	Cleanup(func())
}) *EthTransactionLogsDecoder {
	mock := &EthTransactionLogsDecoder{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
