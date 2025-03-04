package handlers

import (
	"database/sql"
	"net/http"
	"strings"

	"github.com/6529-Collections/6529node/internal/eth/ethdb"
)

var ownerDb ethdb.NFTOwnerDb = ethdb.NewOwnerDb()

func NFTOwnersGetHandler(r *http.Request, db *sql.DB) (interface{}, error) {
	// For /api/v1/nft_owners => parts = ["api","v1","nft_owners"]
	// For /api/v1/nft_owners/ => parts = ["api","v1","nft_owners"] (the trailing slash is trimmed)
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

	query := ""
	queryParams := []interface{}{}

	if contract != "" && tokenID != "" {
		query = "contract = ? AND token_id = ?"
		queryParams = []interface{}{contract, tokenID}
	} else if contract != "" {
		query = "contract = ?"
		queryParams = []interface{}{contract}
	}

	return PaginatedQueryHandler[ethdb.NFTOwner](r, db, ownerDb, query, queryParams)
}
