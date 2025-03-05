package handlers

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/internal/eth/ethdb"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func fakeQueryNftPage(r *http.Request, rq db.QueryRunner, pgQuerier db.PaginatedQuerier[ethdb.NFT], query string, queryParams []interface{}) (PaginatedResponse[ethdb.NFT], error) {
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

func checkNftPaginatedResponse(t *testing.T, result interface{}) {
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

func TestNFTsGetHandler(t *testing.T) {
	origHandler := queryNftPage
	queryNftPage = fakeQueryNftPage
	defer func() {
		queryNftPage = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nfts", nil)

	var dummyDB *sql.DB

	result, err := NFTsGetHandler(req, dummyDB)
	require.NoError(t, err)

	checkNftPaginatedResponse(t, result)
}

func TestNFTsGetHandler_WithContract(t *testing.T) {
	origHandler := queryNftPage
	queryNftPage = fakeQueryNftPage
	defer func() {
		queryNftPage = origHandler
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nfts/0xABC", nil)

	var dummyDB *sql.DB

	result, err := NFTsGetHandler(req, dummyDB)
	require.NoError(t, err)

	checkNftPaginatedResponse(t, result)
}

func TestNFTsGetHandler_WithContractAndTokenID(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/nfts/0xABC/42", nil)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"contract", "token_id", "supply", "burnt_supply"}).
		AddRow("0xabc", "42", 100, 0)

	mock.ExpectQuery("SELECT .* FROM nfts WHERE contract = \\? AND token_id = \\?").
		WithArgs("0xabc", "42").
		WillReturnRows(rows)

	result, err := NFTsGetHandler(req, db)
	require.NoError(t, err)

	nft, ok := result.(*ethdb.NFT)
	require.True(t, ok, "result should be of type ethdb.NFT")

	require.Equal(t, "0xabc", nft.Contract)
	require.Equal(t, "42", nft.TokenID)
	require.Equal(t, uint64(100), nft.Supply)
	require.Equal(t, uint64(0), nft.BurntSupply)
}
