package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// PaginatedResponse holds the common pagination fields.
type PaginatedResponse struct {
	Page     int     `json:"page"`
	PageSize int     `json:"page_size"`
	Total    int     `json:"total"`
	Prev     *string `json:"prev"`
	Next     *string `json:"next"`
}

// ReturnPaginatedData populates the total count and constructs absolute URLs
// for prev and next based on the request's scheme, host, and path.
func (p *PaginatedResponse) ReturnPaginatedData(r *http.Request, total int) {
	p.Total = total

	// Build the base URL (scheme://host/path).
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	// r.URL.Path is the path (e.g. /api/v1/nfts)
	// r.Host is the host/port (e.g. localhost:8080)
	baseURL := fmt.Sprintf("%s://%s%s", scheme, r.Host, r.URL.Path)

	// If page > 1, build a "prev" link
	if p.Page > 1 {
		prevPage := p.Page - 1
		prev := fmt.Sprintf("%s?page=%d&page_size=%d", baseURL, prevPage, p.PageSize)
		p.Prev = &prev
	} else {
		// Setting it to nil means "prev": null in JSON
		p.Prev = nil
	}

	// If there are more items after this page, build a "next" link
	offsetEnd := (p.Page-1)*p.PageSize + p.PageSize
	if offsetEnd < total {
		nextPage := p.Page + 1
		nxt := fmt.Sprintf("%s?page=%d&page_size=%d", baseURL, nextPage, p.PageSize)
		p.Next = &nxt
	} else {
		p.Next = nil
	}
}

// ExtractPagination reads the page and page_size from the query string
// and returns them with default fallbacks if they are missing or invalid.
func ExtractPagination(r *http.Request) (int, int, error) {
	pageStr := r.URL.Query().Get("page")
	if pageStr == "" {
		pageStr = "1"
	}
	pageSizeStr := r.URL.Query().Get("page_size")
	if pageSizeStr == "" {
		pageSizeStr = "10"
	}

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	pageSize, err := strconv.Atoi(pageSizeStr)
	if err != nil || pageSize < 1 {
		pageSize = 10
	}

	return page, pageSize, err
}

// ConvertStructToMap marshals a struct into JSON, then unmarshals it into
// a map[string]interface{}. Useful for post-processing, e.g. snake_case transformation.
func ConvertStructToMap(v interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, err
	}

	return result, nil
}
