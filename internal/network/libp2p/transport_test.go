package libp2p

import (
	"testing"

	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
)

func TestLibp2pTransportStartStop(t *testing.T) {
	tr := NewLibp2pTransport()

	err := tr.Start()
	assert.NoError(t, err, "expected to start libp2p transport without error")
	err = tr.Stop()
	assert.NoError(t, err, "expected to stop libp2p transport without error")
}

func TestLibp2pTransportPublishSubscribe(t *testing.T) {
	A := NewLibp2pTransport()
	assert.NoError(t, A.Start(), "should start node A without error")
	defer A.Stop()

	B := NewLibp2pTransport()
	assert.NoError(t, B.Start(), "should start node B without error")
	defer B.Stop()

	var wg sync.WaitGroup
	wg.Add(1)

	var receivedMsg []byte
	err := B.Subscribe("chat", func(msg []byte) {
		receivedMsg = msg
		wg.Done()
	})
	assert.NoError(t, err, "subscribe should succeed")

	A_impl := A.(*Libp2pTransport)
	B_impl := B.(*Libp2pTransport)

	bAddrs := B_impl.h.Addrs()
	if len(bAddrs) == 0 {
		t.Fatalf("Node B has no addresses! Possibly an issue with host setup.")
	}

	infoB := peer.AddrInfo{
		ID:    B_impl.h.ID(),
		Addrs: bAddrs,
	}
	err = A_impl.h.Connect(A_impl.ctx, infoB)
	assert.NoError(t, err, "Node A should connect to Node B without error")

	time.Sleep(1 * time.Second)

	msgData := []byte("hello from Node A")
	err = A.Publish("chat", msgData)
	assert.NoError(t, err, "publish should succeed")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message on Node B")
	}

	assert.Equal(t, msgData, receivedMsg, "B should receive the same data A sent")
}
