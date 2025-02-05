package network

type NetworkTransport interface {
	Start() error
	Stop() error
	Publish(topic string, data []byte) error
	Subscribe(topic string, handler func(msg []byte)) error
}