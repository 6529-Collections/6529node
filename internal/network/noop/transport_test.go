package noop

import (
	"testing"

	"github.com/6529-Collections/6529node/internal/network"
	"github.com/stretchr/testify/assert"
)

func TestNoopTransport(t *testing.T) {
	var nt network.NetworkTransport = &NoopTransport{}
	assert.NoError(t, nt.Start())
	assert.NoError(t, nt.Publish("some-topic", []byte("hello")))
	assert.NoError(t, nt.Subscribe("some-topic", func(msg []byte) {}))
	assert.NoError(t, nt.Stop())
}