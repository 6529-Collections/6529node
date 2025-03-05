package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStartRPCServer_StartAndClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Just to satisfy potential DB checks in your handlers:
	mock.ExpectBegin()
	mock.ExpectCommit()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closeFunc := StartRPCServer(port, db, ctx)
	defer closeFunc()

	// Give server some time to start
	time.Sleep(100 * time.Millisecond)

	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1/status", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, bodyBytes, "expected non-empty response body for /api/v1/status")

	// Now close the server
	start := time.Now()
	closeFunc()
	elapsed := time.Since(start)
	require.Less(t, elapsed, 5*time.Second, "server shutdown took too long")

	// Confirm server is closed
	time.Sleep(100 * time.Millisecond)
	_, err = http.Get(url)
	require.Error(t, err, "expected error after server shutdown, got none")
}

func TestStartRPCServer_InvalidRoute(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closeFunc := StartRPCServer(port, db, ctx)
	defer closeFunc()

	time.Sleep(100 * time.Millisecond)

	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1/invalid-route", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestResponseWriter_StatusCodeCapture(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	core, logs := observer.New(zap.InfoLevel)
	testLogger := zap.New(core)
	originalLogger := zap.L()
	zap.ReplaceGlobals(testLogger)
	defer zap.ReplaceGlobals(originalLogger)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closeFunc := StartRPCServer(port, db, ctx)
	defer closeFunc()

	time.Sleep(100 * time.Millisecond)

	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1/status", port)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)

	foundStatusLog := false
	foundIPLog := false
	foundMethodLog := false
	foundPathLog := false

	for _, entry := range logs.All() {
		if entry.Message == "Request" {
			for _, f := range entry.Context {
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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closeFunc := StartRPCServer(port, db, ctx)
	defer closeFunc()

	time.Sleep(100 * time.Millisecond)

	url := fmt.Sprintf("http://127.0.0.1:%d/api/v1/status", port)

	const numRequests = 10
	errChan := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		go func() {
			resp, err := http.Get(url)
			if err != nil {
				errChan <- fmt.Errorf("failed to connect: %v", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errChan <- fmt.Errorf("expected 200, got %d", resp.StatusCode)
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				errChan <- fmt.Errorf("failed to read body: %v", err)
				return
			}
			if len(body) == 0 {
				errChan <- fmt.Errorf("expected non-empty body")
				return
			}
			errChan <- nil
		}()
	}

	for i := 0; i < numRequests; i++ {
		require.NoError(t, <-errChan)
	}
}

func checkResponse(t *testing.T, port int, path string, expectedStatusCode int) {
	url := fmt.Sprintf("http://127.0.0.1:%d/%s", port, path)

	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatusCode, resp.StatusCode)

	body, readErr := io.ReadAll(resp.Body)
	require.NoError(t, readErr)
	require.NotEmpty(t, body, "expected some response from "+url)
}

func TestStartRPCServer_NFTsEndpoints(t *testing.T) {
	db, cleanup := testdb.SetupTestDB(t)
	defer cleanup()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closeFunc := StartRPCServer(port, db, ctx)
	defer closeFunc()

	time.Sleep(100 * time.Millisecond)

	// /nfts
	checkResponse(t, port, "api/v1/nfts", http.StatusOK)

	// /nfts/:contract
	checkResponse(t, port, "api/v1/nfts/0x123", http.StatusOK)

	// /nft/:contract/:tokenID
	checkResponse(t, port, "api/v1/nfts/0x123/1", http.StatusNotFound)

	// /nft_owners
	checkResponse(t, port, "api/v1/nft_owners", http.StatusOK)

	// /nft_owners/:contract
	checkResponse(t, port, "api/v1/nft_owners/0x123", http.StatusOK)

	// /nft_owners/:contract/:tokenID
	checkResponse(t, port, "api/v1/nft_owners/0x123/1", http.StatusOK)

	// /nft_transfers
	checkResponse(t, port, "api/v1/nft_transfers", http.StatusOK)

	// /nft_transfers/:contract
	checkResponse(t, port, "api/v1/nft_transfers/0x123", http.StatusOK)

	// /nft_transfers/:contract/:tokenID
	checkResponse(t, port, "api/v1/nft_transfers/0x123/1", http.StatusOK)
}
