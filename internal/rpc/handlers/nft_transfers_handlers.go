package handlers

import (
	"database/sql"
	"net/http"
	"regexp"
	"strings"

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
		return NFTTransfersGetListForTxHashHandler(r, db, txHash)
	}

	if contract != "" && tokenID != "" {
		return NFTTransfersGetListForContractTokenHandler(r, db, contract, tokenID)
	}

	if contract != "" {
		return NFTTransfersGetListForContractHandler(r, db, contract)
	}

	return NFTTransfersGetListHandler(r, db)
}

func NFTTransfersGetListHandler(r *http.Request, db *sql.DB) (NFTTransfersResponse, error) {
	page, pageSize, _ := ExtractPagination(r)

	total, transfers, err := transferDb.GetAllTransfers(db, pageSize, page)
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

func NFTTransfersGetListForContractHandler(r *http.Request, db *sql.DB, contract string) (NFTTransfersResponse, error) {
	page, pageSize, _ := ExtractPagination(r)

	total, transfers, err := transferDb.GetTransfersForContract(db, contract, pageSize, page)
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

func NFTTransfersGetListForContractTokenHandler(r *http.Request, db *sql.DB, contract string, tokenID string) (NFTTransfersResponse, error) {
	page, pageSize, _ := ExtractPagination(r)

	total, transfers, err := transferDb.GetTransfersForContractToken(db, contract, tokenID, pageSize, page)
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

func NFTTransfersGetListForTxHashHandler(r *http.Request, db *sql.DB, txHash string) (NFTTransfersResponse, error) {
	page, pageSize, _ := ExtractPagination(r)

	total, transfers, err := transferDb.GetTransfersForTxHash(db, txHash, pageSize, page)
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
