package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var globalLoggerReplaceMu sync.Mutex

func TestCreateApiPath(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  Path
	}{
		{"EmptyInput", "", Path("/api/v1/")},
		{"LeadingSlash", "/myResource", Path("/api/v1/myResource")},
		{"NoLeadingSlash", "myResource", Path("/api/v1/myResource")},
		{"MultipleLeadingSlashes", "///multiple", Path("/api/v1///multiple")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := CreateApiPath(ApiV1, tc.input)
			if got != tc.want {
				t.Errorf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestSetupHandlers_ValidMethod(t *testing.T) {
	testHandler := func(r *http.Request) (any, error) {
		return map[string]string{"message": "hello"}, nil
	}

	handlersMap := MethodHandlers{
		CreateApiPath(ApiV1, "test"): {
			HTTP_GET: testHandler,
		},
	}

	server := setupTestServer(t, handlersMap)

	resp, err := http.Get(server.URL + "/api/v1/test")
	if err != nil {
		t.Fatalf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %s", ct)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}

	if body["message"] != "hello" {
		t.Fatalf("expected message 'hello', got '%s'", body["message"])
	}
}

func TestSetupHandlers_MethodNotAllowed(t *testing.T) {
	handlersMap := MethodHandlers{
		CreateApiPath(ApiV1, "unsupportedMethod"): {
			HTTP_GET: func(r *http.Request) (any, error) {
				return map[string]string{"message": "only GET is supported"}, nil
			},
		},
	}

	server := setupTestServer(t, handlersMap)

	resp, err := http.Post(server.URL+"/api/v1/unsupportedMethod", "application/json", nil)
	if err != nil {
		t.Fatalf("failed to make POST request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", resp.StatusCode)
	}
}

func TestSetupHandlers_HandlerError(t *testing.T) {
	// Lock to avoid concurrency issues with global logger replacement
	globalLoggerReplaceMu.Lock()
	defer globalLoggerReplaceMu.Unlock()

	oldLogger := zap.L()
	core, recorded := observer.New(zap.InfoLevel)
	newLogger := zap.New(core)
	zap.ReplaceGlobals(newLogger)
	defer zap.ReplaceGlobals(oldLogger)

	testHandler := func(r *http.Request) (any, error) {
		return nil, errors.New("simulated handler error")
	}

	handlersMap := MethodHandlers{
		CreateApiPath(ApiV1, "errorTest"): {
			HTTP_GET: testHandler,
		},
	}

	server := setupTestServer(t, handlersMap)

	resp, err := http.Get(server.URL + "/api/v1/errorTest")
	if err != nil {
		t.Fatalf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", resp.StatusCode)
	}

	logs := recorded.FilterMessage("failed to handle request").All()
	if len(logs) == 0 {
		t.Errorf("expected error log with 'failed to handle request', got none")
	}
}

func TestSetupHandlers_NilResponse(t *testing.T) {
	testHandler := func(r *http.Request) (any, error) {
		return nil, nil
	}

	handlersMap := MethodHandlers{
		CreateApiPath(ApiV1, "nilResponse"): {
			HTTP_GET: testHandler,
		},
	}

	server := setupTestServer(t, handlersMap)

	resp, err := http.Get(server.URL + "/api/v1/nilResponse")
	if err != nil {
		t.Fatalf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if len(bodyBytes) != 0 {
		t.Fatalf("expected empty response body, got: %q", string(bodyBytes))
	}
}

func TestSetupHandlers_JsonEncodingError(t *testing.T) {
	globalLoggerReplaceMu.Lock()
	defer globalLoggerReplaceMu.Unlock()

	oldLogger := zap.L()
	core, recorded := observer.New(zap.InfoLevel)
	newLogger := zap.New(core)
	zap.ReplaceGlobals(newLogger)
	defer zap.ReplaceGlobals(oldLogger)

	testHandler := func(r *http.Request) (any, error) {
		return map[string]interface{}{"badValue": make(chan int)}, nil
	}

	handlersMap := MethodHandlers{
		CreateApiPath(ApiV1, "encodingError"): {
			HTTP_GET: testHandler,
		},
	}

	server := setupTestServer(t, handlersMap)

	resp, err := http.Get(server.URL + "/api/v1/encodingError")
	if err != nil {
		t.Fatalf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", resp.StatusCode)
	}

	logs := recorded.FilterMessage("failed to encode response").All()
	if len(logs) == 0 {
		t.Errorf("expected error log with 'failed to encode response', got none")
	}
}

func TestSetupHandlers_UnknownPath(t *testing.T) {
	handlersMap := MethodHandlers{}

	server := setupTestServer(t, handlersMap)

	resp, err := http.Get(server.URL + "/api/v1/nonExistent")
	if err != nil {
		t.Fatalf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestSetupHandlers_ConcurrentRequests(t *testing.T) {
	testHandler := func(r *http.Request) (any, error) {
		return map[string]string{"message": "concurrent hello"}, nil
	}

	handlersMap := MethodHandlers{
		CreateApiPath(ApiV1, "concurrentTest"): {
			HTTP_GET: testHandler,
		},
	}

	server := setupTestServer(t, handlersMap)

	const concurrencyLevel = 10

	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)

	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			defer wg.Done()
			resp, err := http.Get(server.URL + "/api/v1/concurrentTest")
			if err != nil {
				t.Errorf("failed to make GET request: %v", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("expected status 200, got %d", resp.StatusCode)
				return
			}

			var body map[string]string
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Errorf("failed to decode JSON response: %v", err)
				return
			}

			if body["message"] != "concurrent hello" {
				t.Errorf("expected message 'concurrent hello', got '%s'", body["message"])
			}
		}()
	}

	wg.Wait()
}

func setupTestServer(t *testing.T, handlersMap MethodHandlers) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	SetupHandlers(mux, handlersMap)
	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		server.Close()
	})
	return server
}
