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

func fakeQueryOwnerPage(r *http.Request, rq db.QueryRunner, pgQuerier db.PaginatedQuerier[ethdb.NFTOwner], query string, queryParams []interface{}) (PaginatedResponse[ethdb.NFTOwner], error) {
	return PaginatedResponse[ethdb.NFTOwner]{
		Page:     1,
		PageSize: 10,
		Total:    100,
		Prev:     nil,
		Next:     nil,
		Data: []*ethdb.NFTOwner{
			{
				Contract: "0xabc",
				TokenID:  "42",
				Owner:    "0x123",
			},
		},
	}, nil
}

func checkNftOwnerResponse(t *testing.T, result interface{}) {
	resMap, ok := result.(PaginatedResponse[ethdb.NFTOwner])
	require.True(t, ok, "result should be a map")

	require.Equal(t, 1, resMap.Page)
	require.Equal(t, 10, resMap.PageSize)
	require.Equal(t, 100, resMap.Total)
	require.Nil(t, resMap.Prev)
	require.Nil(t, resMap.Next)
	require.Equal(t, 1, len(resMap.Data))
	require.Equal(t, "0x123", resMap.Data[0].Owner)
	require.Equal(t, "0xabc", resMap.Data[0].Contract)
	require.Equal(t, "42", resMap.Data[0].TokenID)
}

func TestNFTOwnersGetHandler(t *testing.T) {
	// Save the original handler and restore it after the test.
	origHandler := queryOwnerPage
	queryOwnerPage = fakeQueryOwnerPage
	defer func() {
		queryOwnerPage = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nft_owners", nil)

	var dummyDB *sql.DB

	result, err := NFTOwnersGetHandler(req, dummyDB)
	require.NoError(t, err)

	checkNftOwnerResponse(t, result)

}

func TestNFTOwnersGetHandler_WithContract(t *testing.T) {
	// Save the original handler and restore it after the test.
	origHandler := queryOwnerPage
	queryOwnerPage = fakeQueryOwnerPage
	defer func() {
		queryOwnerPage = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nft_owners/0xABC", nil)

	var dummyDB *sql.DB

	result, err := NFTOwnersGetHandler(req, dummyDB)
	require.NoError(t, err)

	checkNftOwnerResponse(t, result)
}

func TestNFTOwnersGetHandler_WithContractAndTokenID(t *testing.T) {
	// Save the original handler and restore it after the test.
	origHandler := queryOwnerPage
	queryOwnerPage = fakeQueryOwnerPage
	defer func() {
		queryOwnerPage = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nft_owners/0xABC/42", nil)

	var dummyDB *sql.DB

	result, err := NFTOwnersGetHandler(req, dummyDB)
	require.NoError(t, err)

	checkNftOwnerResponse(t, result)
}
