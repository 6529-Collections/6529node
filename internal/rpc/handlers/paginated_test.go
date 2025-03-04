package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/stretchr/testify/assert"
)

//------------------//
//     MOCKS
//------------------//

// mockQueryRunner is a simple mock that does nothing meaningful except satisfy the interface.
type mockQueryRunner struct {
	QueryFn    func(query string, args ...interface{}) (*sql.Rows, error)
	QueryRowFn func(query string, args ...interface{}) *sql.Row
}

func (m *mockQueryRunner) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if m.QueryFn == nil {
		return nil, errors.New("Query not implemented")
	}
	rows, err := m.QueryFn(query, args...)
	return rows, err
}

func (m *mockQueryRunner) QueryRow(query string, args ...interface{}) *sql.Row {
	if m.QueryRowFn == nil {
		return nil
	}
	return m.QueryRowFn(query, args...)
}

// mockPaginatedQuerier implements db.PaginatedQuerier[T].
type mockPaginatedQuerier[T any] struct {
	Total int
	Data  []*T
	Err   error
}

func (m *mockPaginatedQuerier[T]) GetPaginatedResponseForQuery(
	rq db.QueryRunner,
	queryOptions db.QueryOptions,
	queryParams []interface{},
) (total int, data []*T, err error) {
	if m.Err != nil {
		return 0, nil, m.Err
	}
	return m.Total, m.Data, nil
}

//----------------------//
//     TESTS
//----------------------//

func TestReturnPaginatedData(t *testing.T) {
	t.Run("HTTP, page=1 => no prev, has next", func(t *testing.T) {
		resp := PaginatedResponse[string]{
			Page:     1,
			PageSize: 10,
		}
		req := httptest.NewRequest(http.MethodGet, "http://example.com/api/v1/test", nil)
		// total bigger so next is available
		resp.ReturnPaginatedData(req, 100)

		if resp.Prev != nil {
			t.Errorf("Expected Prev to be nil, got %v", *resp.Prev)
		}
		if resp.Next == nil {
			t.Errorf("Expected Next to be non-nil, got nil")
		}
		if resp.Total != 100 {
			t.Errorf("Expected total=100, got %d", resp.Total)
		}
	})

	t.Run("HTTPS, page=2 => has prev, no next if offsetEnd >= total", func(t *testing.T) {
		resp := PaginatedResponse[string]{
			Page:     2,
			PageSize: 10,
		}
		req := httptest.NewRequest(http.MethodGet, "https://example.com/api/v1/test", nil)
		// offsetEnd = (2-1)*10 + 10 = 20, so total=20 => no next
		resp.ReturnPaginatedData(req, 20)

		if resp.Prev == nil {
			t.Errorf("Expected Prev to be non-nil, got nil")
		}
		if resp.Next != nil {
			t.Errorf("Expected Next to be nil, got %v", *resp.Next)
		}
		if resp.Total != 20 {
			t.Errorf("Expected total=20, got %d", resp.Total)
		}
	})
}

func TestExtractPagination(t *testing.T) {
	t.Run("Valid page & page_size", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com?page=3&page_size=15", nil)
		page, pageSize, err := ExtractPagination(req)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if page != 3 {
			t.Errorf("Expected page=3, got %d", page)
		}
		if pageSize != 15 {
			t.Errorf("Expected pageSize=15, got %d", pageSize)
		}
	})

	t.Run("Missing params => defaults to 1 and 10", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
		page, pageSize, err := ExtractPagination(req)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if page != 1 {
			t.Errorf("Expected default page=1, got %d", page)
		}
		if pageSize != 10 {
			t.Errorf("Expected default pageSize=10, got %d", pageSize)
		}
	})

	t.Run("Invalid page => fallback to 1, but err is returned from Atoi", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com?page=abc&page_size=xyz", nil)
		page, pageSize, err := ExtractPagination(req)
		if page != 1 {
			t.Errorf("Expected fallback page=1, got %d", page)
		}
		if pageSize != 10 {
			t.Errorf("Expected fallback pageSize=10, got %d", pageSize)
		}
		// The function returns the err from Atoi, so let's just check it is non-nil
		if err == nil {
			t.Errorf("Expected error from invalid Atoi, got nil")
		}
	})
}

func TestConvertStructToMap(t *testing.T) {
	t.Run("Successful conversion", func(t *testing.T) {
		type sample struct {
			Name  string `json:"name"`
			Count int    `json:"count"`
		}
		s := sample{Name: "Alice", Count: 42}
		m, err := ConvertStructToMap(s)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if m["name"] != "Alice" || m["count"] != float64(42) {
			t.Errorf("Expected map[name:Alice count:42], got %v", m)
		}
	})

	t.Run("Failing marshal", func(t *testing.T) {
		// Functions/channels/etc. cannot be marshaled by encoding/json
		type failing struct {
			Fn func() string `json:"fn"`
		}
		f := failing{Fn: func() string { return "hello" }}
		m, err := ConvertStructToMap(f)
		if err == nil {
			t.Errorf("Expected error from JSON marshal, got nil with map %v", m)
		}
	})
}

func TestConvertStructToMap_UnmarshalError(t *testing.T) {
	_, err := ConvertStructToMap([]byte("lala"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "json: cannot unmarshal string into Go")
}

func TestPaginatedQueryHandler(t *testing.T) {
	t.Run("Successful Query", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com?page=2&page_size=5", nil)

		mockQuerier := &mockPaginatedQuerier[string]{
			Total: 30,
			Data:  []*string{ptr("first"), ptr("second")},
		}

		mockQueryRunner := &mockQueryRunner{}

		resp, err := PaginatedQueryHandler[string](
			req,
			mockQueryRunner,
			mockQuerier,
			"some query",
			[]interface{}{},
		)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		// Check the fields of the PaginatedResponse
		if resp.Page != 2 {
			t.Errorf("Expected page=2, got %d", resp.Page)
		}
		if resp.PageSize != 5 {
			t.Errorf("Expected pageSize=5, got %d", resp.PageSize)
		}
		if resp.Total != 30 {
			t.Errorf("Expected total=30, got %d", resp.Total)
		}
		if len(resp.Data) != 2 {
			t.Errorf("Expected data of length 2, got %d", len(resp.Data))
		}
		if resp.Prev == nil {
			t.Errorf("Expected Prev to be set, got nil")
		}
		if resp.Next == nil {
			t.Errorf("Expected Next to be set, got nil")
		}
	})

	t.Run("Error from Querier", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)

		mockQuerier := &mockPaginatedQuerier[string]{
			Err: errors.New("some DB error"),
		}

		resp, err := PaginatedQueryHandler[string](
			req,
			&mockQueryRunner{},
			mockQuerier,
			"some query",
			[]interface{}{},
		)
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		// We expect an empty (zero) PaginatedResponse
		if resp.Page != 0 {
			t.Errorf("Expected zero response, got page=%d", resp.Page)
		}
		if resp.Total != 0 {
			t.Errorf("Expected zero response, got total=%d", resp.Total)
		}
		if resp.Data != nil {
			t.Errorf("Expected nil data, got %v", resp.Data)
		}
	})
}

// ptr is a helper to create a pointer to a string literal.
func ptr(s string) *string {
	return &s
}

// Verify JSON structure for coverage on "data" (just for demonstration; not strictly needed).
func TestPaginatedResponse_JSONMarshalling(t *testing.T) {
	resp := PaginatedResponse[string]{
		Page:     1,
		PageSize: 2,
		Total:    10,
		Prev:     ptr("prev-link"),
		Next:     ptr("next-link"),
		Data:     []*string{ptr("foo"), ptr("bar")},
	}
	bytes, err := json.Marshal(resp)
	if err != nil {
		t.Errorf("Expected no error marshalling, got %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(bytes, &parsed); err != nil {
		t.Errorf("Expected no error unmarshalling, got %v", err)
	}
	// We won't do a strict comparison here, but you could if desired.
	if parsed["page"].(float64) != 1 {
		t.Errorf("Expected page=1, got %v", parsed["page"])
	}
}
