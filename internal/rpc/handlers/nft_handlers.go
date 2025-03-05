package handlers

import (
	"database/sql"
	"net/http"
	"strings"

	"github.com/6529-Collections/6529node/internal/eth/ethdb"
)

var nftDb ethdb.NFTDb = ethdb.NewNFTDb()
var queryNftPage = QueryPage[ethdb.NFT]

func NFTsGetHandler(r *http.Request, db *sql.DB) (interface{}, error) {
	// For /api/v1/nfts => parts = ["api","v1","nfts"]
	// For /api/v1/nfts/<contract> => parts = ["api","v1","nfts","<contract>"]
	// For /api/v1/nfts/<contract>/<tokenID> => parts = ["api","v1","nfts","<contract>","<tokenID>"]
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	contract := ""
	tokenID := ""
	if len(parts) > 3 {
		contract = strings.ToLower(parts[3])
	}
	if len(parts) > 4 {
		tokenID = parts[4]
	}

	if contract != "" && tokenID != "" {
		return nftDb.GetNft(db, contract, tokenID)
	}

	query := ""
	queryParams := []interface{}{}

	if contract != "" {
		query = "contract = ?"
		queryParams = []interface{}{contract}
	}

	return queryNftPage(r, db, nftDb, query, queryParams)
}
