// Code generated by mockery v2.52.1. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// NetworkTransport is an autogenerated mock type for the NetworkTransport type
type NetworkTransport struct {
	mock.Mock
}

// Close provides a mock function with no fields
func (_m *NetworkTransport) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Publish provides a mock function with given fields: topic, data
func (_m *NetworkTransport) Publish(topic string, data []byte) error {
	ret := _m.Called(topic, data)

	if len(ret) == 0 {
		panic("no return value specified for Publish")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte) error); ok {
		r0 = rf(topic, data)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Subscribe provides a mock function with given fields: topic, handler
func (_m *NetworkTransport) Subscribe(topic string, handler func([]byte)) error {
	ret := _m.Called(topic, handler)

	if len(ret) == 0 {
		panic("no return value specified for Subscribe")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, func([]byte)) error); ok {
		r0 = rf(topic, handler)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Unsubscribe provides a mock function with given fields: topic
func (_m *NetworkTransport) Unsubscribe(topic string) error {
	ret := _m.Called(topic)

	if len(ret) == 0 {
		panic("no return value specified for Unsubscribe")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(topic)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewNetworkTransport creates a new instance of NetworkTransport. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewNetworkTransport(t interface {
	mock.TestingT
	Cleanup(func())
}) *NetworkTransport {
	mock := &NetworkTransport{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
