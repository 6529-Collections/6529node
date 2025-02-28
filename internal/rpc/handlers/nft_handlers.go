package handlers

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/6529-Collections/6529node/internal/eth/ethdb"
)

type NFTsResponse struct {
	PaginatedResponse
	Data []ethdb.NFT `json:"data"`
}

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

	if contract != "" && tokenID != "" {
		return NFTForContractTokenGetHandler(r, db, contract, tokenID)
	}

	if contract != "" {
		return NFTsGetListForContractHandler(r, db, contract)
	}

	return NFTsGetListHandler(r, db)
}

func NFTsGetListHandler(r *http.Request, db *sql.DB) (NFTsResponse, error) {
	page, pageSize, _ := ExtractPagination(r)

	total, nfts, err := nftDb.GetAllNfts(db, pageSize, page)
	if err != nil {
		return NFTsResponse{}, err
	}

	resp := NFTsResponse{
		PaginatedResponse: PaginatedResponse{
			Page:     page,
			PageSize: pageSize,
		},
		Data: nfts,
	}

	resp.ReturnPaginatedData(r, total)

	return resp, nil
}

func NFTsGetListForContractHandler(r *http.Request, db *sql.DB, contract string) (NFTsResponse, error) {
	page, pageSize, _ := ExtractPagination(r)

	total, nfts, err := nftDb.GetNftsForContract(db, contract, pageSize, page)
	if err != nil {
		return NFTsResponse{}, err
	}

	resp := NFTsResponse{
		PaginatedResponse: PaginatedResponse{
			Page:     page,
			PageSize: pageSize,
		},
		Data: nfts,
	}

	resp.ReturnPaginatedData(r, total)

	return resp, nil
}

func NFTForContractTokenGetHandler(r *http.Request, db *sql.DB, contract string, tokenID string) (ethdb.NFT, error) {
	nft, err := nftDb.GetNft(db, contract, tokenID)

	if err != nil {
		if err == sql.ErrNoRows {
			return ethdb.NFT{}, fmt.Errorf("not found")
		}
		return ethdb.NFT{}, err
	}

	return *nft, nil
}
