package handlers

import (
	"database/sql"
	"net/http"
	"strings"

	"github.com/6529-Collections/6529node/internal/eth/ethdb"
)

var nftDb ethdb.NFTDb = ethdb.NewNFTDb()

func NFTsGetHandler(r *http.Request, db *sql.DB) (interface{}, error) {
	// For /api/v1/nfts => parts = ["api","v1","nfts"]
	// For /api/v1/nfts/ => parts = ["api","v1","nfts"] (the trailing slash is trimmed)
	// For /api/v1/nfts/0xABC/42 => parts = ["api","v1","nfts","0xABC","42"]
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	contract := ""
	tokenID := ""
	if len(parts) > 3 {
		contract = strings.ToLower(parts[3])
	}
	if len(parts) > 4 {
		tokenID = parts[4]
	}

	// if contract != "" && tokenID != "" {
	// 	return PaginatedQueryHandler[ethdb.NFT](r, db, nftDb, "contract = ? AND token_id = ?", []interface{}{contract, tokenID})
	// }

	// if contract != "" {
	// 	return PaginatedQueryHandler[ethdb.NFT](r, db, nftDb, "contract = ?", []interface{}{contract})
	// }

	query := ""
	queryParams := []interface{}{}
	if contract != "" && tokenID != "" {
		query = "contract = ? AND token_id = ?"
		queryParams = []interface{}{contract, tokenID}
	} else if contract != "" {
		query = "contract = ?"
		queryParams = []interface{}{contract}
	}

	return PaginatedQueryHandler[ethdb.NFT](r, db, nftDb, query, queryParams)
}
