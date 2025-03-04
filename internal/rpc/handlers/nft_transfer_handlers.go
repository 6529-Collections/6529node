package handlers

import (
	"database/sql"
	"net/http"
	"regexp"
	"strings"

	"github.com/6529-Collections/6529node/internal/eth/ethdb"
)

var transferDb ethdb.NFTTransferDb = ethdb.NewTransferDb()
var PaginatedNftTransferQueryHandlerFunc = PaginatedQueryHandler[ethdb.NFTTransfer]

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

	query := ""
	queryParams := []interface{}{}

	if txHash != "" {
		query = "tx_hash = ?"
		queryParams = []interface{}{txHash}
	}

	if contract != "" && tokenID != "" {
		query = "contract = ? AND token_id = ?"
		queryParams = []interface{}{contract, tokenID}
	} else if contract != "" {
		query = "contract = ?"
		queryParams = []interface{}{contract}
	}

	return PaginatedNftTransferQueryHandlerFunc(r, db, transferDb, query, queryParams)
}
