package libp2p

import (
	"context"
	"fmt"

	"github.com/6529-Collections/6529node/internal/p2p"
	lp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
)

type Libp2pTransport struct {
	h      host.Host
	ctx    context.Context
	cancel context.CancelFunc

	ps   *pubsub.PubSub
	subs map[string]*pubsub.Subscription
}

func NewLibp2pTransport() p2p.NetworkTransport {
	return &Libp2pTransport{}
}

func (l *Libp2pTransport) Start() error {
	l.ctx, l.cancel = context.WithCancel(context.Background())

	h, err := lp2p.New(
		lp2p.DefaultTransports,
		lp2p.DefaultSecurity,
		lp2p.DefaultMuxers,
		lp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		return err
	}
	l.h = h

	for _, addr := range l.h.Addrs() {
		zap.L().Info("Listening on base addr", zap.String("addr", addr.String()))
		full := fmt.Sprintf("%s/p2p/%s", addr.String(), l.h.ID().String())
		zap.L().Info("Full multiaddr (copy this for another node)", zap.String("multiaddr", full))
	}

	l.ps, err = pubsub.NewGossipSub(l.ctx, l.h)
	if err != nil {
		return fmt.Errorf("failed to create gossipsub: %w", err)
	}

	l.subs = make(map[string]*pubsub.Subscription)
	return nil
}

func (l *Libp2pTransport) Stop() error {
	if l.cancel != nil {
		l.cancel()
	}

	if l.h != nil {
		return l.h.Close()
	}
	return nil
}

func (l *Libp2pTransport) Publish(topic string, data []byte) error {
	if l.ps == nil {
		return fmt.Errorf("publish called before gossipsub is ready")
	}
	return l.ps.Publish(topic, data)
}

func (l *Libp2pTransport) Subscribe(topic string, handler func(msg []byte)) error {
	if l.ps == nil {
		return fmt.Errorf("subscribe called before gossipsub is ready")
	}

	if _, exists := l.subs[topic]; exists {
		return nil
	}

	sub, err := l.ps.Subscribe(topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}
	l.subs[topic] = sub

	go func() {
		for {
			m, err := sub.Next(l.ctx)
			if err != nil {
				return
			}
			handler(m.Data)
		}
	}()
	return nil
}

func (l *Libp2pTransport) Connect(remote string) error {
	ma, err := multiaddr.NewMultiaddr(remote)
	if err != nil {
		return fmt.Errorf("invalid multiaddr: %w", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		return fmt.Errorf("AddrInfoFromP2pAddr failed: %w", err)
	}
	if err := l.h.Connect(l.ctx, *info); err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	zap.L().Info("Successfully connected to peer", zap.String("peer", info.ID.String()))
	return nil
}
