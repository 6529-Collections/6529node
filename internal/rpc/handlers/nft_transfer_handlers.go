package handlers

import (
	"database/sql"
	"net/http"
	"regexp"
	"strings"

	"github.com/6529-Collections/6529node/internal/eth/ethdb"
)

var transferDb ethdb.NFTTransferDb = ethdb.NewTransferDb()
var queryTransferPage = QueryPage[ethdb.NFTTransfer]

var txHashRegex = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)

func NFTTransfersGetHandler(r *http.Request, db *sql.DB) (interface{}, error) {
	// For /api/v1/nft_transfers => parts = ["api","v1","nft_transfers"]
	// For /api/v1/nft_transfers/<txHash> => parts = ["api","v1","nft_transfers","<txHash>"]
	// For /api/v1/nft_transfers/<contract> => parts = ["api","v1","nft_transfers","<contract>"]
	// For /api/v1/nft_transfers/<contract>/<tokenID> => parts = ["api","v1","nft_transfers","<contract>","<tokenID>"]
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

	return queryTransferPage(r, db, transferDb, query, queryParams)
}
