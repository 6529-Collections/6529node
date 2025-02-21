package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStartRPCServer_StartAndClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to open ephemeral port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closeFunc := StartRPCServer(port, ctx)
	defer closeFunc()

	time.Sleep(100 * time.Millisecond)

	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1/status", port)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if len(bodyBytes) == 0 {
		t.Errorf("expected non-empty response body for /api/v1/status")
	}

	start := time.Now()
	closeFunc()
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Fatalf("server shutdown took too long: %v", elapsed)
	}

	time.Sleep(100 * time.Millisecond)

	_, err = http.Get(url)
	if err == nil {
		t.Fatal("expected error after server shutdown, got none")
	}
}

func TestStartRPCServer_InvalidRoute(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to open ephemeral port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closeFunc := StartRPCServer(port, ctx)
	defer closeFunc()

	time.Sleep(100 * time.Millisecond)

	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1/invalid-route", port)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", resp.StatusCode)
	}

	closeFunc()
}

func TestResponseWriter_StatusCodeCapture(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	testLogger := zap.New(core)
	originalLogger := zap.L()
	zap.ReplaceGlobals(testLogger)
	defer zap.ReplaceGlobals(originalLogger)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to open ephemeral port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closeFunc := StartRPCServer(port, ctx)
	defer closeFunc()

	// Give the server time to start
	time.Sleep(100 * time.Millisecond)

	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1/status", port)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	foundStatusLog := false
	foundIPLog := false
	foundMethodLog := false
	foundPathLog := false

	for _, entry := range logs.All() {
		if entry.Message == "Request" {
			fields := entry.Context
			for _, f := range fields {
				switch f.Key {
				case "status":
					if f.Integer == int64(http.StatusOK) {
						foundStatusLog = true
					}
				case "ip":
					if f.String != "" {
						foundIPLog = true
					}
				case "method":
					if f.String == http.MethodGet {
						foundMethodLog = true
					}
				case "path":
					if f.String == "/api/v1/status" {
						foundPathLog = true
					}
				}
			}
		}
	}

	if !foundStatusLog || !foundIPLog || !foundMethodLog || !foundPathLog {
		t.Errorf("did not find expected log fields: status, ip, method, path")
	}
}

func TestServer_ConcurrentRequests(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to open ephemeral port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closeFunc := StartRPCServer(port, ctx)
	defer closeFunc()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1/status", port)

	const numRequests = 10
	errChan := make(chan error, numRequests)

	// Send multiple concurrent requests
	for i := 0; i < numRequests; i++ {
		go func() {
			resp, err := http.Get(url)
			if err != nil {
				errChan <- fmt.Errorf("failed to connect to server: %v", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errChan <- fmt.Errorf("expected 200 OK, got %d", resp.StatusCode)
				return
			}

			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				errChan <- fmt.Errorf("failed to read response body: %v", err)
				return
			}
			if len(bodyBytes) == 0 {
				errChan <- fmt.Errorf("expected non-empty response body for /api/v1/status")
				return
			}

			errChan <- nil
		}()
	}

	// Collect errors from all goroutines
	for i := 0; i < numRequests; i++ {
		if reqErr := <-errChan; reqErr != nil {
			t.Error(reqErr)
		}
	}
}
