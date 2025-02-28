package handlers

import (
	"database/sql"
	"net/http"
	"strings"

	"github.com/6529-Collections/6529node/internal/eth/ethdb"
)

type NFTOwnersResponse struct {
	PaginatedResponse
	Data []ethdb.NFTOwner `json:"data"`
}

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

	if contract != "" && tokenID != "" {
		return NFTOwnersGetListForContractTokenHandler(r, db, contract, tokenID)
	}

	if contract != "" {
		return NFTOwnersGetListForContractHandler(r, db, contract)
	}

	return NFTOwnersGetListHandler(r, db)
}

func NFTOwnersGetListHandler(r *http.Request, db *sql.DB) (NFTOwnersResponse, error) {
	page, pageSize, _ := ExtractPagination(r)

	total, owners, err := ownerDb.GetAllOwners(db, pageSize, page)
	if err != nil {
		return NFTOwnersResponse{}, err
	}

	resp := NFTOwnersResponse{
		PaginatedResponse: PaginatedResponse{
			Page:     page,
			PageSize: pageSize,
		},
		Data: owners,
	}

	resp.ReturnPaginatedData(r, total)

	return resp, nil
}

func NFTOwnersGetListForContractHandler(r *http.Request, db *sql.DB, contract string) (NFTOwnersResponse, error) {
	page, pageSize, _ := ExtractPagination(r)

	total, owners, err := ownerDb.GetOwnersForContract(db, contract, pageSize, page)
	if err != nil {
		return NFTOwnersResponse{}, err
	}

	resp := NFTOwnersResponse{
		PaginatedResponse: PaginatedResponse{
			Page:     page,
			PageSize: pageSize,
		},
		Data: owners,
	}

	resp.ReturnPaginatedData(r, total)

	return resp, nil
}

func NFTOwnersGetListForContractTokenHandler(r *http.Request, db *sql.DB, contract string, tokenID string) (NFTOwnersResponse, error) {
	page, pageSize, _ := ExtractPagination(r)

	total, owners, err := ownerDb.GetOwnersForContractToken(db, contract, tokenID, pageSize, page)
	if err != nil {
		return NFTOwnersResponse{}, err
	}

	resp := NFTOwnersResponse{
		PaginatedResponse: PaginatedResponse{
			Page:     page,
			PageSize: pageSize,
		},
		Data: owners,
	}

	resp.ReturnPaginatedData(r, total)

	return resp, nil
}
