package p2p

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoopTransport(t *testing.T) {
	var nt NetworkTransport = &NoopTransport{}
	assert.NoError(t, nt.Start())
	assert.NoError(t, nt.Publish("some-topic", []byte("hello")))
	assert.NoError(t, nt.Subscribe("some-topic", func(msg []byte) {}))
	assert.NoError(t, nt.Stop())
}
