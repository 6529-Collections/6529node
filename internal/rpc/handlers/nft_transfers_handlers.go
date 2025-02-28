package handlers

import (
	"database/sql"
	"net/http"
	"regexp"
	"strings"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/internal/eth/ethdb"
)

type NFTTransfersResponse struct {
	PaginatedResponse
	Data []ethdb.NFTTransfer `json:"data"`
}

var transferDb ethdb.NFTTransferDb = ethdb.NewTransferDb()

var txHashRegex = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)

func NFTTransfersGetHandler(r *http.Request, db *sql.DB) (interface{}, error) {
	// For /api/v1/nft_transfers => parts = ["api","v1","nft_transfers"]
	// For /api/v1/nft_transfers/ => parts = ["api","v1","nft_transfers"] (the trailing slash is trimmed)
	// For /api/v1/nfts/0xABC/42 => parts = ["api","v1","nfts","0xABC","42"]
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	txHash := ""
	contract := ""
	tokenID := ""
	if len(parts) > 3 {
		if txHashRegex.MatchString(parts[3]) {
			txHash = strings.ToLower(parts[3])
		} else {
			contract = strings.ToLower(parts[3])
		}
	}
	if len(parts) > 4 {
		tokenID = parts[4]
	}

	if txHash != "" {
		query := "tx_hash = ?"
		queryParams := []interface{}{txHash}
		return NFTTransfersQueryHandler(r, db, query, queryParams)
	}

	if contract != "" && tokenID != "" {
		query := "contract = ? AND token_id = ?"
		queryParams := []interface{}{contract, tokenID}
		return NFTTransfersQueryHandler(r, db, query, queryParams)
	}

	if contract != "" {
		query := "contract = ?"
		queryParams := []interface{}{contract}
		return NFTTransfersQueryHandler(r, db, query, queryParams)
	}

	query := ""
	queryParams := []interface{}{}
	return NFTTransfersQueryHandler(r, db, query, queryParams)
}

func NFTTransfersQueryHandler(r *http.Request, rq db.QueryRunner, query string, queryParams []interface{}) (NFTTransfersResponse, error) {
	page, pageSize, _ := ExtractPagination(r)
	queryOptions := db.QueryOptions{
		Where:     query,
		PageSize:  pageSize,
		Page:      page,
		Direction: db.QueryDirectionAsc,
	}

	total, transfers, err := transferDb.GetPaginatedResponseForQuery(rq, queryOptions, queryParams)
	if err != nil {
		return NFTTransfersResponse{}, err
	}

	resp := NFTTransfersResponse{
		PaginatedResponse: PaginatedResponse{
			Page:     page,
			PageSize: pageSize,
		},
		Data: transfers,
	}

	resp.ReturnPaginatedData(r, total)

	return resp, nil
}
