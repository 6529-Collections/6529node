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

func fakeNftPaginatedHandler(r *http.Request, rq db.QueryRunner, pgQuerier db.PaginatedQuerier[ethdb.NFT], query string, queryParams []interface{}) (PaginatedResponse[ethdb.NFT], error) {
	return PaginatedResponse[ethdb.NFT]{
		Page:     1,
		PageSize: 10,
		Total:    100,
		Prev:     nil,
		Next:     nil,
		Data: []*ethdb.NFT{
			{
				Contract: "0xabc",
				TokenID:  "42",
			},
		},
	}, nil
}

func TestNFTsGetHandler_WithContractAndTokenID(t *testing.T) {
	// Save the original handler and restore it after the test.
	origHandler := PaginatedNftQueryHandlerFunc
	PaginatedNftQueryHandlerFunc = fakeNftPaginatedHandler
	defer func() {
		PaginatedNftQueryHandlerFunc = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nfts/0xABC/42", nil)

	var dummyDB *sql.DB

	result, err := NFTsGetHandler(req, dummyDB)
	require.NoError(t, err)

	resMap, ok := result.(PaginatedResponse[ethdb.NFT])
	require.True(t, ok, "result should be a map")

	require.Equal(t, 1, resMap.Page)
	require.Equal(t, 10, resMap.PageSize)
	require.Equal(t, 100, resMap.Total)
	require.Nil(t, resMap.Prev)
	require.Nil(t, resMap.Next)
	require.Equal(t, 1, len(resMap.Data))
	require.Equal(t, "0xabc", resMap.Data[0].Contract)
	require.Equal(t, "42", resMap.Data[0].TokenID)
}
