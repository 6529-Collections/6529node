// Code generated by mockery v2.51.0. DO NOT EDIT.

package mocks_test

import mock "github.com/stretchr/testify/mock"

// TdhIdxTrackerDb is an autogenerated mock type for the TdhIdxTrackerDb type
type TdhIdxTrackerDb struct {
	mock.Mock
}

// GetProgress provides a mock function with no fields
func (_m *TdhIdxTrackerDb) GetProgress() (uint64, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetProgress")
	}

	var r0 uint64
	var r1 error
	if rf, ok := ret.Get(0).(func() (uint64, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() uint64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint64)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SetProgress provides a mock function with given fields: blockNumber
func (_m *TdhIdxTrackerDb) SetProgress(blockNumber uint64) error {
	ret := _m.Called(blockNumber)

	if len(ret) == 0 {
		panic("no return value specified for SetProgress")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(uint64) error); ok {
		r0 = rf(blockNumber)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewTdhIdxTrackerDb creates a new instance of TdhIdxTrackerDb. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewTdhIdxTrackerDb(t interface {
	mock.TestingT
	Cleanup(func())
}) *TdhIdxTrackerDb {
	mock := &TdhIdxTrackerDb{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
