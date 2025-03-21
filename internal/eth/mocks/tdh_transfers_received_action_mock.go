// Code generated by mockery v2.51.0. DO NOT EDIT.

package mocks

import (
	tokens "github.com/6529-Collections/6529node/pkg/tdh/models"
	mock "github.com/stretchr/testify/mock"
)

// TdhTransfersReceivedAction is an autogenerated mock type for the TdhTransfersReceivedAction type
type TdhTransfersReceivedAction struct {
	mock.Mock
}

// Handle provides a mock function with given fields: transfers
func (_m *TdhTransfersReceivedAction) Handle(transfers tokens.TokenTransferBatch) error {
	ret := _m.Called(transfers)

	if len(ret) == 0 {
		panic("no return value specified for Handle")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(tokens.TokenTransferBatch) error); ok {
		r0 = rf(transfers)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewTdhTransfersReceivedAction creates a new instance of TdhTransfersReceivedAction. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewTdhTransfersReceivedAction(t interface {
	mock.TestingT
	Cleanup(func())
}) *TdhTransfersReceivedAction {
	mock := &TdhTransfersReceivedAction{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
