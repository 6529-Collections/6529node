// Code generated by mockery v2.51.0. DO NOT EDIT.

package mocks

import (
	mempool "github.com/6529-Collections/6529node/internal/mempool"
	mock "github.com/stretchr/testify/mock"
)

// Mempool is an autogenerated mock type for the Mempool type
type Mempool struct {
	mock.Mock
}

// AddTransaction provides a mock function with given fields: tx
func (_m *Mempool) AddTransaction(tx *mempool.Transaction) error {
	ret := _m.Called(tx)

	if len(ret) == 0 {
		panic("no return value specified for AddTransaction")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*mempool.Transaction) error); ok {
		r0 = rf(tx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetTransactionsForBlock provides a mock function with given fields: maxCount
func (_m *Mempool) GetTransactionsForBlock(maxCount int) []*mempool.Transaction {
	ret := _m.Called(maxCount)

	if len(ret) == 0 {
		panic("no return value specified for GetTransactionsForBlock")
	}

	var r0 []*mempool.Transaction
	if rf, ok := ret.Get(0).(func(int) []*mempool.Transaction); ok {
		r0 = rf(maxCount)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*mempool.Transaction)
		}
	}

	return r0
}

// ReinjectOrphanedTxs provides a mock function with given fields: txs
func (_m *Mempool) ReinjectOrphanedTxs(txs []*mempool.Transaction) error {
	ret := _m.Called(txs)

	if len(ret) == 0 {
		panic("no return value specified for ReinjectOrphanedTxs")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func([]*mempool.Transaction) error); ok {
		r0 = rf(txs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RemoveTransactions provides a mock function with given fields: txs
func (_m *Mempool) RemoveTransactions(txs []*mempool.Transaction) {
	_m.Called(txs)
}

// Size provides a mock function with no fields
func (_m *Mempool) Size() int {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Size")
	}

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// NewMempool creates a new instance of Mempool. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMempool(t interface {
	mock.TestingT
	Cleanup(func())
}) *Mempool {
	mock := &Mempool{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
