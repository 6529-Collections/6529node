package network_creator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	multiaddr "github.com/multiformats/go-multiaddr"

	"github.com/6529-Collections/6529node/internal/network"
	"github.com/6529-Collections/6529node/internal/network/mocks"
    "github.com/stretchr/testify/mock"
)

var mockLibp2pError bool
var mockLibp2pBlock time.Duration
var originalLibp2pTransportFunc = libp2pTransportFunc


func overrideLibp2pTransportFunc(t *testing.T) {
	libp2pTransportFunc = func(ma multiaddr.Multiaddr, ctx context.Context) (network.NetworkTransport, error) {
		if mockLibp2pBlock > 0 {
			time.Sleep(mockLibp2pBlock)
		}
		if mockLibp2pError {
			return nil, errors.New("mock libp2p transport error")
		}

		mockTransport := mocks.NewNetworkTransport(t)
		mockTransport.On("Close").Maybe().Return(nil)
		mockTransport.On("Publish", mock.Anything, mock.Anything).Maybe().Return(nil)
		mockTransport.On("Subscribe", mock.Anything, mock.Anything).Maybe().Return(nil)
		mockTransport.On("Unsubscribe", mock.Anything).Maybe().Return(nil)
		

		return mockTransport, nil
	}
}

func restoreLibp2pTransportFunc() {
	libp2pTransportFunc = originalLibp2pTransportFunc
}

func resetConcurrency() {

loop:
	for {
		select {
		case <-transportConcurrency:
		default:
			break loop
		}
	}
	for i := 0; i < cap(transportConcurrency); i++ {
		transportConcurrency <- struct{}{}
	}
}

func TestNewNetworkTransport_TableDriven(t *testing.T) {
	logger := zaptest.NewLogger(t)
	defer func() { _ = logger.Sync() }()
	zap.ReplaceGlobals(logger)
	defer zap.ReplaceGlobals(zap.NewNop())

	tests := []struct {
		name          string
		bootstrapAddr string
		contextFunc   func() context.Context
		setupMock     func()
		wantErr       bool
		errContains   string
	}{
		{
			name:          "Empty Address -> no remote connect",
			bootstrapAddr: "",
			contextFunc:   func() context.Context { return context.Background() },
			setupMock: func() {
				mockLibp2pError = false
				mockLibp2pBlock = 0
			},
			wantErr:     false,
		},
		{
			name:          "Valid Multiaddr",
			bootstrapAddr: "/ip4/127.0.0.1/tcp/8080",
			contextFunc:   func() context.Context { return context.Background() },
			setupMock: func() {
				mockLibp2pError = false
				mockLibp2pBlock = 0
			},
			wantErr:  false,
		},
		{
			name:          "Invalid Multiaddr",
			bootstrapAddr: "invalid",
			contextFunc:   func() context.Context { return context.Background() },
			setupMock: func() {
				mockLibp2pError = false
				mockLibp2pBlock = 0
			},
			wantErr:     true,
			errContains: "invalid multiaddr",
		},
		{
			name:          "Context Already Canceled",
			bootstrapAddr: "/ip4/127.0.0.1/tcp/8080",
			contextFunc: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			setupMock: func() {
				mockLibp2pError = false
				mockLibp2pBlock = 0
			},
			wantErr:     true,
			errContains: "context canceled",
		},
		{
			name:          "Error from libp2pTransportFunc",
			bootstrapAddr: "/ip4/127.0.0.1/tcp/8080",
			contextFunc:   func() context.Context { return context.Background() },
			setupMock: func() {
				mockLibp2pError = true
				mockLibp2pBlock = 0
			},
			wantErr:     true,
			errContains: "mock libp2p transport error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetConcurrency()
			overrideLibp2pTransportFunc(t)
			defer restoreLibp2pTransportFunc()

			tc.setupMock()

			ctx := tc.contextFunc()
			transport, err := NewNetworkTransport(tc.bootstrapAddr, ctx)

			if tc.wantErr && err == nil {
				t.Errorf("expected error but got none")
			} else if !tc.wantErr && err != nil {
				t.Errorf("did not expect error but got: %v", err)
			}

			if tc.wantErr && tc.errContains != "" && err != nil {
				if !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("expected error to contain %q, got: %v", tc.errContains, err)
				}
			}

			if !tc.wantErr {
				if transport == nil {
					t.Error("expected a non-nil transport, got nil")
				}
			}
		})
	}
}

func TestNewNetworkTransport_ConcurrencyTokenReleasedOnError(t *testing.T) {
	logger := zaptest.NewLogger(t)
	defer func() { _ = logger.Sync() }()
	zap.ReplaceGlobals(logger)
	defer zap.ReplaceGlobals(zap.NewNop())

	resetConcurrency()
	overrideLibp2pTransportFunc(t)
	defer restoreLibp2pTransportFunc()

	mockLibp2pError = true
	mockLibp2pBlock = 0

	numCalls := 5
	var wg sync.WaitGroup
	wg.Add(numCalls)

	for i := 0; i < numCalls; i++ {
		go func() {
			defer wg.Done()
			_, _ = NewNetworkTransport("/ip4/127.0.0.1/tcp/8080", context.Background())
		}()
	}
	wg.Wait()

	available := 0

	for {
		select {
		case <-transportConcurrency:
			available++
		default:
			goto done
		}
	}
done:

	for i := 0; i < available; i++ {
		transportConcurrency <- struct{}{}
	}

	if available != cap(transportConcurrency) {
		t.Errorf("expected concurrency channel to be full (size %d), got %d",
			cap(transportConcurrency), available)
	}
}

func TestNewNetworkTransport_Concurrency(t *testing.T) {
	logger := zaptest.NewLogger(t)
	defer func() { _ = logger.Sync() }()
	zap.ReplaceGlobals(logger)
	defer zap.ReplaceGlobals(zap.NewNop())

	t.Run("Context canceled while waiting for token", func(t *testing.T) {
		resetConcurrency()
		overrideLibp2pTransportFunc(t)
		defer restoreLibp2pTransportFunc()

		mockLibp2pError = false
		mockLibp2pBlock = 500 * time.Millisecond

		const numGoroutines = 10
		startCh := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				<-startCh
				_, _ = NewNetworkTransport("/ip4/127.0.0.1/tcp/8080", context.Background())
			}()
		}

		close(startCh)
		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		transport, err := NewNetworkTransport("/ip4/127.0.0.1/tcp/8080", ctx)
		if err == nil {
			t.Errorf("expected error due to context cancellation, got nil")
		} else {
			if !strings.Contains(err.Error(), "context done before acquiring concurrency token") &&
				!strings.Contains(err.Error(), "context canceled") {
				t.Errorf("expected context-done error, got: %v", err)
			}
		}
		if transport != nil {
			t.Errorf("expected nil transport when context is canceled, got %v", transport)
		}

		wg.Wait()
	})

	t.Run("11th call eventually succeeds when a token is released", func(t *testing.T) {
		resetConcurrency()
		overrideLibp2pTransportFunc(t)
		defer restoreLibp2pTransportFunc()

		mockLibp2pError = false
		mockLibp2pBlock = 200 * time.Millisecond

		const numGoroutines = 10
		startCh := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				<-startCh
				_, _ = NewNetworkTransport("/ip4/127.0.0.1/tcp/8080", context.Background())
			}()
		}
		close(startCh)

		time.Sleep(50 * time.Millisecond)

		transport, err := NewNetworkTransport("/ip4/127.0.0.1/tcp/8080", context.Background())
		if err != nil {
			t.Errorf("did not expect error for 11th call, got: %v", err)
		}
		if transport == nil {
			t.Error("expected a valid transport, got nil")
		}

		wg.Wait()
	})
}

func TestNewNetworkTransport_Concurrency_DeadlineExpires(t *testing.T) {
	logger := zaptest.NewLogger(t)
	defer func() { _ = logger.Sync() }()
	zap.ReplaceGlobals(logger)
	defer zap.ReplaceGlobals(zap.NewNop())

	resetConcurrency()
	overrideLibp2pTransportFunc(t)
	defer restoreLibp2pTransportFunc()

	mockLibp2pError = false
	mockLibp2pBlock = 500 * time.Millisecond

	const numGoroutines = 10
	startCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			<-startCh
			_, _ = NewNetworkTransport("/ip4/127.0.0.1/tcp/8080", context.Background())
		}()
	}

	close(startCh)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	transport, err := NewNetworkTransport("/ip4/127.0.0.1/tcp/8080", ctx)
	if err == nil {
		t.Error("expected error due to context deadline, got nil")
	} else {
		if !strings.Contains(err.Error(), "context done before acquiring concurrency token") &&
			!strings.Contains(err.Error(), "context deadline exceeded") {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if transport != nil {
		t.Errorf("expected nil transport, got %v", transport)
	}

	wg.Wait()
}

func TestNewNetworkTransport_PanicDoesNotLeakTokens(t *testing.T) {
	logger := zaptest.NewLogger(t)
	defer func() { _ = logger.Sync() }()
	zap.ReplaceGlobals(logger)
	defer zap.ReplaceGlobals(zap.NewNop())

	resetConcurrency()
	overrideLibp2pTransportFunc(t)
	defer restoreLibp2pTransportFunc()

	libp2pTransportFunc = func(ma multiaddr.Multiaddr, ctx context.Context) (network.NetworkTransport, error) {
		panic("mock panic in transport constructor")
	}

	funcThatPanics := func() (tr network.NetworkTransport, err error) {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					err = e
				} else {
					err = fmt.Errorf("panic in transport constructor: %v", r)
				}
			}
		}()
		return NewNetworkTransport("/ip4/127.0.0.1/tcp/8080", context.Background())
	}

	tr, err := funcThatPanics()
	if err != nil {
		t.Logf("Expected error from panic: %v", err)
	}
	if tr != nil {
		t.Error("Expected nil transport after panic")
	}

	available := 0

loop:
	for {
		select {
		case <-transportConcurrency:
			available++
		default:
			break loop
		}
	}

	for i := 0; i < available; i++ {
		transportConcurrency <- struct{}{}
	}
	if available != cap(transportConcurrency) {
		t.Errorf("expected concurrency channel to be full after panic, got %d (want %d)",
			available, cap(transportConcurrency))
	}
}
