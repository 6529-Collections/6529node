package libp2p

import (
	"context"
	"fmt"
	lp2p "github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"

	"github.com/6529-Collections/6529node/internal/network"
)

type subHolder struct {
	subscription *pubsub.Subscription
	wg           sync.WaitGroup
}

type Libp2pTransport struct {
	remoteAddr multiaddr.Multiaddr
	parentCtx  context.Context
	ctx        context.Context
	cancel     context.CancelFunc

	host          host.Host
	gossipSub     *pubsub.PubSub
	subscriptions map[string]*subHolder
	topics        map[string]*pubsub.Topic

	mutex   sync.Mutex
	running bool 
}

var NewGossipSubFn = pubsub.NewGossipSub


func NewLibp2pTransport(maddr multiaddr.Multiaddr, parentCtx context.Context) (network.NetworkTransport, error) {
    ctx, cancel := context.WithCancel(parentCtx)

    p2pHost, err := lp2p.New(
        lp2p.DefaultTransports,
        lp2p.DefaultSecurity,
        lp2p.DefaultMuxers,
        lp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
    )
    if err != nil {
        cancel()
        return nil, fmt.Errorf("failed to create libp2p host: %w", err)
    }

    // Only attempt to parse and connect if maddr is not nil
    if maddr != nil {
        peerInfo, err := peer.AddrInfoFromP2pAddr(maddr)
        if err == nil {
            // Try connecting to the bootstrap peer (if provided)
            maxRetries := 3
            for i := 1; i <= maxRetries; i++ {
                dialErr := p2pHost.Connect(ctx, *peerInfo)
                if dialErr == nil {
                    zap.L().Info("Connected to bootstrap peer",
                        zap.String("peerID", peerInfo.ID.String()),
                        zap.Int("attempt", i),
                    )
                    break
                }
                zap.L().Warn("Failed to connect to bootstrap peer",
                    zap.Error(dialErr),
                    zap.Int("attempt", i),
                    zap.Int("maxRetries", maxRetries),
                )
                if i < maxRetries {
                    time.Sleep(2 * time.Second)
                }
            }
        } else {
            zap.L().Warn("No valid peer info in bootstrap address", zap.Error(err))
        }
    } else {
        zap.L().Info("No bootstrap multiaddr provided; skipping peer connect.")
    }

    gossipSub, err := NewGossipSubFn(ctx, p2pHost)
    if err != nil {
        p2pHost.Close()
        cancel()
        return nil, fmt.Errorf("failed to create gossipsub: %w", err)
    }

    transport := &Libp2pTransport{
        remoteAddr:    maddr,
        parentCtx:     parentCtx,
        ctx:           ctx,
        cancel:        cancel,
        host:          p2pHost,
        gossipSub:     gossipSub,
        subscriptions: make(map[string]*subHolder),
        topics:        make(map[string]*pubsub.Topic),
        running:       false,
    }

    transport.mutex.Lock()
    transport.running = true
    transport.mutex.Unlock()

    zap.L().Info("Libp2pTransport started",
        zap.String("remote", fmt.Sprintf("%v", maddr)),
        zap.String("peerID", transport.host.ID().String()),
        zap.Any("listenAddrs", transport.host.Addrs()),
    )

    return transport, nil
}


func (transport *Libp2pTransport) Close() error {
	transport.mutex.Lock()
	if !transport.running {
		transport.mutex.Unlock()
		zap.L().Info("Libp2pTransport already stopped",
			zap.String("remote", transport.remoteAddr.String()),
			zap.String("peerID", transport.host.ID().String()),
		)
		return nil
	}

	transport.running = false

	subs := transport.subscriptions
	tops := transport.topics

	transport.subscriptions = make(map[string]*subHolder)
	transport.topics = make(map[string]*pubsub.Topic)

	if transport.cancel != nil {
		transport.cancel()
	}
	transport.mutex.Unlock()

	for _, holder := range subs {
		holder.subscription.Cancel()
	}

	for _, holder := range subs {
		holder.wg.Wait()
	}

	for _, topic := range tops {
		if err := topic.Close(); err != nil {
			zap.L().Error("Error closing pubsub topic",
				zap.Error(err),
				zap.String("remote", transport.remoteAddr.String()),
				zap.String("peerID", transport.host.ID().String()),
			)
		}
	}

	var err error
	if transport.host != nil {
		err = transport.host.Close()
	}

	zap.L().Info("Libp2pTransport stopped",
		zap.String("remote", transport.remoteAddr.String()),
		zap.String("peerID", transport.host.ID().String()),
	)
	return err
}

func (transport *Libp2pTransport) Publish(topicName string, data []byte) error {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()

	if !transport.running {
		return fmt.Errorf("cannot publish: transport is stopped")
	}
	if transport.gossipSub == nil {
		return fmt.Errorf("publish called before gossipsub is ready")
	}

	topic, err := transport.getOrJoinTopicLocked(topicName)
	if err != nil {
		return fmt.Errorf("failed to get or join topic %s: %w", topicName, err)
	}

	return topic.Publish(transport.ctx, data)
}

func (transport *Libp2pTransport) Subscribe(topicName string, handler func(msg []byte)) error {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()

	if !transport.running {
		return fmt.Errorf("cannot subscribe: transport is stopped")
	}
	if transport.gossipSub == nil {
		return fmt.Errorf("subscribe called before gossipsub is ready")
	}

	topic, err := transport.getOrJoinTopicLocked(topicName)
	if err != nil {
		return fmt.Errorf("failed to get or join topic %s: %w", topicName, err)
	}

	if _, exists := transport.subscriptions[topicName]; exists {
		return fmt.Errorf("already subscribed to topic: %s", topicName)
	}

	subscription, err := topic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topicName, err)
	}

	holder := &subHolder{subscription: subscription}
	holder.wg.Add(1)
	transport.subscriptions[topicName] = holder

	go func() {
		defer holder.wg.Done()
		for {
			msg, err := subscription.Next(transport.ctx)
			if err != nil {
				return
			}
			handler(msg.Data)
		}
	}()

	return nil
}

func (transport *Libp2pTransport) Unsubscribe(topicName string) error {
	transport.mutex.Lock()
	holder, exists := transport.subscriptions[topicName]
	if !exists {
		transport.mutex.Unlock()
		return fmt.Errorf("no subscription for topic: %s", topicName)
	}

	delete(transport.subscriptions, topicName)
	topic, hasTopic := transport.topics[topicName]
	if hasTopic {
		delete(transport.topics, topicName)
	}

	holder.subscription.Cancel()

	topicToClose := topic
	transport.mutex.Unlock()

	holder.wg.Wait()
	if hasTopic && topicToClose != nil {
		_ = topicToClose.Close()
	}

	return nil
}

func (transport *Libp2pTransport) getOrJoinTopicLocked(topicName string) (*pubsub.Topic, error) {
	if !transport.running {
		return nil, fmt.Errorf("transport is not running (stopped or uninitialized)")
	}

	if topic, found := transport.topics[topicName]; found {
		return topic, nil
	}
	topic, err := transport.gossipSub.Join(topicName)
	if err != nil {
		return nil, err
	}
	transport.topics[topicName] = topic
	return topic, nil
}

func (transport *Libp2pTransport) HostAddrs() []multiaddr.Multiaddr {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	return transport.host.Addrs()
}

func (transport *Libp2pTransport) HostID() peer.ID {
	return transport.host.ID()
}
