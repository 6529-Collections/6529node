package network

type NetworkTransport interface {
	Close() error
	Publish(topic string, data []byte) error
	Subscribe(topic string, handler func(msg []byte)) error
	Unsubscribe(topic string) error
}
