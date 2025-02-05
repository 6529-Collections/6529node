package noop

import "github.com/6529-Collections/6529node/internal/network"

type NoopTransport struct{}

func (n *NoopTransport) Start() error {
	return nil
}

func (n *NoopTransport) Stop() error {
	return nil
}

func (n *NoopTransport) Publish(topic string, data []byte) error {
	return nil
}

func (n *NoopTransport) Subscribe(topic string, handler func(msg []byte)) error {
	return nil
}

func NewNoopTransport() network.NetworkTransport {
	return &NoopTransport{}
}
