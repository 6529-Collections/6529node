package handlers

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/internal/eth/ethdb"
	"github.com/stretchr/testify/require"
)

func fakeNftTransferPaginatedHandler(r *http.Request, rq db.QueryRunner, pgQuerier db.PaginatedQuerier[ethdb.NFTTransfer], query string, queryParams []interface{}) (PaginatedResponse[ethdb.NFTTransfer], error) {
	return PaginatedResponse[ethdb.NFTTransfer]{
		Page:     1,
		PageSize: 10,
		Total:    100,
		Prev:     nil,
		Next:     nil,
		Data: []*ethdb.NFTTransfer{
			{
				Contract: "0xabc",
				TokenID:  "42",
				TxHash:   "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			},
		},
	}, nil
}


func checkNftTransferPaginatedResponse(t *testing.T, result interface{}) {
	resMap, ok := result.(PaginatedResponse[ethdb.NFTTransfer])
	require.True(t, ok, "result should be a map")

	require.Equal(t, 1, resMap.Page)
	require.Equal(t, 10, resMap.PageSize)
	require.Equal(t, 100, resMap.Total)
	require.Nil(t, resMap.Prev)
	require.Nil(t, resMap.Next)
	require.Equal(t, 1, len(resMap.Data))
	require.Equal(t, "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", resMap.Data[0].TxHash)
	require.Equal(t, "0xabc", resMap.Data[0].Contract)
	require.Equal(t, "42", resMap.Data[0].TokenID)
}

func TestNFTTransfersGetHandler(t *testing.T) {
	// Save the original handler and restore it after the test.
	origHandler := PaginatedNftTransferQueryHandlerFunc
	PaginatedNftTransferQueryHandlerFunc = fakeNftTransferPaginatedHandler
	defer func() {
		PaginatedNftTransferQueryHandlerFunc = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nft_transfers", nil)

	var dummyDB *sql.DB

	result, err := NFTTransfersGetHandler(req, dummyDB)
	require.NoError(t, err)

	checkNftTransferPaginatedResponse(t, result)
}

func TestNFTTransfersGetHandler_WithContract(t *testing.T) {
	// Save the original handler and restore it after the test.
	origHandler := PaginatedNftTransferQueryHandlerFunc
	PaginatedNftTransferQueryHandlerFunc = fakeNftTransferPaginatedHandler
	defer func() {
		PaginatedNftTransferQueryHandlerFunc = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nft_transfers/0xABC", nil)

	var dummyDB *sql.DB

	result, err := NFTTransfersGetHandler(req, dummyDB)
	require.NoError(t, err)

	checkNftTransferPaginatedResponse(t, result)
}

func TestNFTTransfersGetHandler_WithContractAndTokenID(t *testing.T) {
	// Save the original handler and restore it after the test.
	origHandler := PaginatedNftTransferQueryHandlerFunc
	PaginatedNftTransferQueryHandlerFunc = fakeNftTransferPaginatedHandler
	defer func() {
		PaginatedNftTransferQueryHandlerFunc = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nft_transfers/0xABC/42", nil)

	var dummyDB *sql.DB

	result, err := NFTTransfersGetHandler(req, dummyDB)
	require.NoError(t, err)

	checkNftTransferPaginatedResponse(t, result)

}

func TestNFTTransfersGetHandler_WithTxHash(t *testing.T) {
	// Save the original handler and restore it after the test.
	origHandler := PaginatedNftTransferQueryHandlerFunc
	PaginatedNftTransferQueryHandlerFunc = fakeNftTransferPaginatedHandler
	defer func() {
		PaginatedNftTransferQueryHandlerFunc = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nft_transfers/0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", nil)

	var dummyDB *sql.DB

	result, err := NFTTransfersGetHandler(req, dummyDB)
	require.NoError(t, err)

	checkNftTransferPaginatedResponse(t, result)
}
