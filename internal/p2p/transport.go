package p2p

type NetworkTransport interface {
	Start() error
	Stop() error
	Publish(topic string, data []byte) error
	Subscribe(topic string, handler func(msg []byte)) error
}

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
