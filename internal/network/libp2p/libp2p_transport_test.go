package libp2p_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/6529-Collections/6529node/internal/network/libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	logger, _ := config.Build()
	zap.ReplaceGlobals(logger)
}

func addPeerID(t *testing.T, addr multiaddr.Multiaddr, p peer.ID) multiaddr.Multiaddr {
	m, err := multiaddr.NewMultiaddr(fmt.Sprintf("/p2p/%s", p.String()))
	if err != nil {
		t.Fatalf("Failed to build peerID multiaddr: %v", err)
	}
	return addr.Encapsulate(m)
}

func mustCreateMultiaddr(addr string) multiaddr.Multiaddr {
	m, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		panic(fmt.Sprintf("failed to create multiaddr: %v", err))
	}
	return m
}

func TestHostCreation_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		maddrStr  string
		expectErr bool // whether we expect an error from NewLibp2pTransport
	}{
		{
			name:      "ValidMultiaddr",
			maddrStr:  "/ip4/127.0.0.1/tcp/0/p2p/12D3KooWTTnqpMGKQf1UYJWab6gF9y1Tyzkav4k81q2EcaLf6J8c",
			expectErr: false,
		},
		{
			name:      "MissingP2PPart",
			maddrStr:  "/ip4/127.0.0.1/tcp/1234",
			expectErr: false,
		},
		{
			name:      "TrulyInvalidMultiaddr",
			maddrStr:  "/ip4/999.999.999.999/tcp/abcd",
			expectErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			maddr, err := multiaddr.NewMultiaddr(tc.maddrStr)
			if err != nil {
				if !tc.expectErr {
					t.Fatalf("Unexpected multiaddr parse error: %v", err)
				}
				return
			}

			parentCtx := context.Background()
			transport, err := libp2p.NewLibp2pTransport(maddr, parentCtx)
			if (err != nil) != tc.expectErr {
				t.Fatalf("Expected err=%v, but got: %v", tc.expectErr, err)
			}
			if transport != nil {
				defer transport.Close()
			}
		})
	}
}

func TestGossipSubCreationFailure_Forced(t *testing.T) {
	oldFn := libp2p.NewGossipSubFn
	defer func() { libp2p.NewGossipSubFn = oldFn }()

	libp2p.NewGossipSubFn = func(ctx context.Context, h host.Host, opts ...pubsub.Option) (*pubsub.PubSub, error) {
		return nil, fmt.Errorf("forced gossipsub creation error for test coverage")
	}

	maddr := mustCreateMultiaddr("/ip4/127.0.0.1/tcp/0")
	ctx := context.Background()

	tr, err := libp2p.NewLibp2pTransport(maddr, ctx)
	if err == nil {
		t.Fatal("Expected forced gossipsub error, but got nil")
	}
	if tr != nil {
		t.Fatalf("Expected nil transport on forced error, got %#v", tr)
	}
}




func TestHostCreation_SuccessfulDial(t *testing.T) {
	t.Parallel()

	serverMaddr := mustCreateMultiaddr("/ip4/127.0.0.1/tcp/0")
	serverCtx := context.Background()
	serverTransport, err := libp2p.NewLibp2pTransport(serverMaddr, serverCtx)
	if err != nil {
		t.Fatalf("Failed to create server transport: %v", err)
	}
	defer serverTransport.Close()

	serverImpl, ok := serverTransport.(*libp2p.Libp2pTransport)
	if !ok {
		t.Fatal("serverTransport is not a *Libp2pTransport")
	}
	if len(serverImpl.HostAddrs()) == 0 {
		t.Fatal("serverTransport has no host addrs to dial")
	}
	serverPeerID := serverImpl.HostID()
	serverAddr := serverImpl.HostAddrs()[0]
	dialMaddr := addPeerID(t, serverAddr, serverPeerID)

	clientCtx := context.Background()
	clientTransport, err := libp2p.NewLibp2pTransport(dialMaddr, clientCtx)
	if err != nil {
		t.Fatalf("Expected successful dial, got error: %v", err)
	}
	defer clientTransport.Close()
}

func TestUnsubscribeEdgeCases(t *testing.T) {
	maddr := mustCreateMultiaddr("/ip4/127.0.0.1/tcp/0")
	ctx := context.Background()
	transport, err := libp2p.NewLibp2pTransport(maddr, ctx)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}
	defer transport.Close()

	err = transport.Unsubscribe("non-existent-topic")
	if err == nil {
		t.Error("Expected error when unsubscribing non-existent topic, but got nil")
	}

	topicName := "my-topic"
	if err := transport.Subscribe(topicName, func(msg []byte) {}); err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	if err := transport.Unsubscribe(topicName); err != nil {
		t.Errorf("Unsubscribe error: %v", err)
	}

	if err := transport.Unsubscribe(topicName); err == nil {
		t.Error("Expected error unsubscribing a second time, but got nil")
	}
}

func TestStopLifecycle(t *testing.T) {
	t.Parallel()

	maddr := mustCreateMultiaddr("/ip4/127.0.0.1/tcp/0")
	ctx := context.Background()
	transport, err := libp2p.NewLibp2pTransport(maddr, ctx)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}
	defer func() {
		_ = transport.Close()
	}()

	if stopErr := transport.Close(); stopErr != nil {
		t.Fatalf("Unexpected error stopping transport: %v", stopErr)
	}

	if stopErr := transport.Close(); stopErr != nil {
		t.Fatalf("Stopping a second time should not error, got: %v", stopErr)
	}

	pubErr := transport.Publish("any-topic", []byte("should fail"))
	if pubErr == nil {
		t.Error("Publish on stopped transport should return an error, got nil")
	}
}

func TestMultiplePublishSubscribe_Parallel(t *testing.T) {
	maddr := mustCreateMultiaddr("/ip4/127.0.0.1/tcp/0")
	ctx := context.Background()
	transport, err := libp2p.NewLibp2pTransport(maddr, ctx)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}
	defer transport.Close()

	var wg sync.WaitGroup
	topics := []string{"topicA", "topicB", "topicC", "topicD"}

	for _, topicName := range topics {
		if err := transport.Subscribe(topicName, func(msg []byte) {
			zap.L().Debug("Got message on topic", zap.String("topic", topicName))
		}); err != nil {
			t.Fatalf("Subscribe to %s failed: %v", topicName, err)
		}
		defer func() {
			if err := transport.Unsubscribe(topicName); err != nil {
				t.Logf("Error unsubscribing from topic %s: %v", topicName, err)
			}
		}()
	}

	publishFunc := func(topicName string, msg string) {
		defer wg.Done()
		if pErr := transport.Publish(topicName, []byte(msg)); pErr != nil {
			t.Logf("Publish error on topic %s: %v", topicName, pErr)
		}
	}

	wg.Add(len(topics))
	for _, topicName := range topics {
		go publishFunc(topicName, "Hello from concurrency test")
	}
	wg.Wait()
}

func TestStopDuringPublish(t *testing.T) {
	maddr := mustCreateMultiaddr("/ip4/127.0.0.1/tcp/0")
	ctx := context.Background()
	transport, err := libp2p.NewLibp2pTransport(maddr, ctx)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}
	defer func() {
		_ = transport.Close()
	}()

	topicName := "stop-during-publish"
	err = transport.Subscribe(topicName, func(msg []byte) {})
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	doneCh := make(chan struct{})
	var publishCount int32
	go func() {
		for {
			select {
			case <-doneCh:
				return
			default:
				if pErr := transport.Publish(topicName, []byte("spam")); pErr == nil {
					atomic.AddInt32(&publishCount, 1)
				} else {
					return
				}
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(doneCh)

	if err := transport.Close(); err != nil {
		t.Errorf("Transport close error: %v", err)
	}

	finalCount := atomic.LoadInt32(&publishCount)
	t.Logf("Successfully published %d messages before closing", finalCount)

	if err := transport.Publish(topicName, []byte("after-close")); err == nil {
		t.Error("Expected publish to fail after transport closed, but got no error")
	}
}

func TestParentContextCancellation(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	maddr := mustCreateMultiaddr("/ip4/127.0.0.1/tcp/0")
	transport, err := libp2p.NewLibp2pTransport(maddr, parentCtx)
	if err != nil {
		t.Fatalf("Failed to create transport: %v", err)
	}
	defer transport.Close()

	cancel()
	time.Sleep(300 * time.Millisecond)

	if err := transport.Publish("any-topic", []byte("test")); err == nil {
		t.Error("Expected an error when publishing on a canceled transport, got nil")
	}
	_ = transport.Close()
}
