package handlers

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/6529-Collections/6529node/internal/db"
)

// PaginatedResponse holds the common pagination fields.
type PaginatedResponse[T any] struct {
	Page     int     `json:"page"`
	PageSize int     `json:"page_size"`
	Total    int     `json:"total"`
	Prev     *string `json:"prev"`
	Next     *string `json:"next"`
	Data     []*T    `json:"data"`
}

func NewPaginatedResponse[T any](r *http.Request, page int, pageSize int, total int, data []*T) PaginatedResponse[T] {
	resp := PaginatedResponse[T]{
		Page:     page,
		PageSize: pageSize,
		Total:    total,
		Data:     data,
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	baseURL := fmt.Sprintf("%s://%s%s", scheme, r.Host, r.URL.Path)

	if resp.Page > 1 {
		prevPage := resp.Page - 1
		prev := fmt.Sprintf("%s?page=%d&page_size=%d", baseURL, prevPage, resp.PageSize)
		resp.Prev = &prev
	} else {
		resp.Prev = nil
	}

	offsetEnd := (resp.Page-1)*resp.PageSize + resp.PageSize
	if offsetEnd < total {
		nextPage := resp.Page + 1
		nxt := fmt.Sprintf("%s?page=%d&page_size=%d", baseURL, nextPage, resp.PageSize)
		resp.Next = &nxt
	} else {
		resp.Next = nil
	}
	return resp
}

func GetPaginationParams(r *http.Request) (int, int, error) {
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

func QueryPage[T any](
	r *http.Request,
	rq db.QueryRunner,
	pgQuerier db.PaginatedQuerier[T],
	query string,
	queryParams []interface{},
) (PaginatedResponse[T], error) {
	var zero PaginatedResponse[T]

	page, pageSize, _ := GetPaginationParams(r)
	queryOptions := db.QueryOptions{
		Where:     query,
		PageSize:  pageSize,
		Page:      page,
		Direction: db.QueryDirectionAsc,
	}

	total, data, err := pgQuerier.GetPaginatedResponseForQuery(rq, queryOptions, queryParams)
	if err != nil {
		return zero, err
	}

	return NewPaginatedResponse(r, page, pageSize, total, data), nil
}
