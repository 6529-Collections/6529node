package ethdb

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/6529-Collections/6529node/pkg/tdh/models"
)

// fakeScanner is a test double that implements the db.RowScanner interface.
type fakeScanner struct {
	values []interface{}
	err    error
}

// Scan copies fake values into the passed pointers or returns a preset error.
func (f *fakeScanner) Scan(dest ...interface{}) error {
	if f.err != nil {
		return f.err
	}
	if len(f.values) != len(dest) {
		return fmt.Errorf("expected %d values, got %d", len(dest), len(f.values))
	}
	for i, v := range f.values {
		switch ptr := dest[i].(type) {
		case *string:
			val, ok := v.(string)
			if !ok {
				return fmt.Errorf("expected string for field %d", i)
			}
			*ptr = val
		case *uint64:
			val, ok := v.(uint64)
			if !ok {
				return fmt.Errorf("expected uint64 for field %d", i)
			}
			*ptr = val
		case *models.TransferType:
			val, ok := v.(models.TransferType)
			if !ok {
				return fmt.Errorf("expected models.TransferType for field %d", i)
			}
			*ptr = val
		default:
			return fmt.Errorf("unsupported type at index %d", i)
		}
	}
	return nil
}

func TestNFTScanRow_Success(t *testing.T) {
	// Prepare fake data for NFT: Contract, TokenID, Supply, BurntSupply.
	scanner := &fakeScanner{
		values: []interface{}{"0xABCDEF", "123", uint64(100), uint64(5)},
	}
	n := &NFT{}
	err := n.ScanRow(scanner)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if n.Contract != "0xABCDEF" {
		t.Errorf("expected Contract '0xABCDEF', got %s", n.Contract)
	}
	if n.TokenID != "123" {
		t.Errorf("expected TokenID '123', got %s", n.TokenID)
	}
	if n.Supply != 100 {
		t.Errorf("expected Supply 100, got %d", n.Supply)
	}
	if n.BurntSupply != 5 {
		t.Errorf("expected BurntSupply 5, got %d", n.BurntSupply)
	}
}

func TestNFTScanRow_NoRows(t *testing.T) {
	// Simulate a no-rows condition.
	scanner := &fakeScanner{
		err: sql.ErrNoRows,
	}
	n := &NFT{}
	err := n.ScanRow(scanner)
	if err != nil {
		t.Fatalf("expected nil error for no rows, got %v", err)
	}
}

func TestNFTScanRow_Error(t *testing.T) {
	// Simulate an error other than sql.ErrNoRows.
	scanner := &fakeScanner{
		err: errors.New("scan error"),
	}
	n := &NFT{}
	err := n.ScanRow(scanner)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if err.Error() != "scan error" {
		t.Errorf("expected 'scan error', got %v", err)
	}
}

func TestNFTOwnerScanRow_Success(t *testing.T) {
	// Prepare fake data for NFTOwner: Owner, Contract, TokenID, TokenUniqueID, Timestamp.
	scanner := &fakeScanner{
		values: []interface{}{"0xOwner", "0xContract", "456", uint64(789), uint64(161718)},
	}
	o := &NFTOwner{}
	err := o.ScanRow(scanner)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if o.Owner != "0xOwner" {
		t.Errorf("expected Owner '0xOwner', got %s", o.Owner)
	}
	if o.Contract != "0xContract" {
		t.Errorf("expected Contract '0xContract', got %s", o.Contract)
	}
	if o.TokenID != "456" {
		t.Errorf("expected TokenID '456', got %s", o.TokenID)
	}
	if o.TokenUniqueID != 789 {
		t.Errorf("expected TokenUniqueID 789, got %d", o.TokenUniqueID)
	}
	if o.Timestamp != 161718 {
		t.Errorf("expected Timestamp 161718, got %d", o.Timestamp)
	}
}

func TestNFTOwnerScanRow_NoRows(t *testing.T) {
	scanner := &fakeScanner{
		err: sql.ErrNoRows,
	}
	o := &NFTOwner{}
	err := o.ScanRow(scanner)
	if err != nil {
		t.Fatalf("expected nil error for no rows, got %v", err)
	}
}

func TestNFTOwnerScanRow_Error(t *testing.T) {
	scanner := &fakeScanner{
		err: errors.New("owner scan error"),
	}
	o := &NFTOwner{}
	err := o.ScanRow(scanner)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if err.Error() != "owner scan error" {
		t.Errorf("expected 'owner scan error', got %v", err)
	}
}

func TestNFTTransferScanRow_Success(t *testing.T) {
	// Prepare fake data for NFTTransfer:
	// BlockNumber, TransactionIndex, LogIndex, TxHash, EventName, From, To,
	// Contract, TokenID, TokenUniqueID, BlockTime, Type.
	expectedType := models.TransferType("1")
	scanner := &fakeScanner{
		values: []interface{}{
			uint64(10),     // BlockNumber
			uint64(2),      // TransactionIndex
			uint64(1),      // LogIndex
			"0xTxHash",     // TxHash
			"Transfer",     // EventName
			"0xFrom",       // From
			"0xTo",         // To
			"0xContract",   // Contract
			"789",          // TokenID
			uint64(555),    // TokenUniqueID
			uint64(161800), // BlockTime
			expectedType,   // Type
		},
	}
	tObj := &NFTTransfer{}
	err := tObj.ScanRow(scanner)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if tObj.BlockNumber != 10 {
		t.Errorf("expected BlockNumber 10, got %d", tObj.BlockNumber)
	}
	if tObj.TransactionIndex != 2 {
		t.Errorf("expected TransactionIndex 2, got %d", tObj.TransactionIndex)
	}
	if tObj.LogIndex != 1 {
		t.Errorf("expected LogIndex 1, got %d", tObj.LogIndex)
	}
	if tObj.TxHash != "0xTxHash" {
		t.Errorf("expected TxHash '0xTxHash', got %s", tObj.TxHash)
	}
	if tObj.EventName != "Transfer" {
		t.Errorf("expected EventName 'Transfer', got %s", tObj.EventName)
	}
	if tObj.From != "0xFrom" {
		t.Errorf("expected From '0xFrom', got %s", tObj.From)
	}
	if tObj.To != "0xTo" {
		t.Errorf("expected To '0xTo', got %s", tObj.To)
	}
	if tObj.Contract != "0xContract" {
		t.Errorf("expected Contract '0xContract', got %s", tObj.Contract)
	}
	if tObj.TokenID != "789" {
		t.Errorf("expected TokenID '789', got %s", tObj.TokenID)
	}
	if tObj.TokenUniqueID != 555 {
		t.Errorf("expected TokenUniqueID 555, got %d", tObj.TokenUniqueID)
	}
	if tObj.BlockTime != 161800 {
		t.Errorf("expected BlockTime 161800, got %d", tObj.BlockTime)
	}
	if tObj.Type != expectedType {
		t.Errorf("expected Type %v, got %v", expectedType, tObj.Type)
	}
}

func TestNFTTransferScanRow_NoRows(t *testing.T) {
	scanner := &fakeScanner{
		err: sql.ErrNoRows,
	}
	tObj := &NFTTransfer{}
	err := tObj.ScanRow(scanner)
	if err != nil {
		t.Fatalf("expected nil error for no rows, got %v", err)
	}
}

func TestNFTTransferScanRow_Error(t *testing.T) {
	scanner := &fakeScanner{
		err: errors.New("transfer scan error"),
	}
	tObj := &NFTTransfer{}
	err := tObj.ScanRow(scanner)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if err.Error() != "transfer scan error" {
		t.Errorf("expected 'transfer scan error', got %v", err)
	}
}

// Since TokenTransferCheckpoint is just a struct without any methods,
// a basic test to instantiate it is enough for coverage.
func TestTokenTransferCheckpoint(t *testing.T) {
	ttc := TokenTransferCheckpoint{
		ID:               1,
		BlockNumber:      100,
		TransactionIndex: 2,
		LogIndex:         3,
	}
	if ttc.ID != 1 || ttc.BlockNumber != 100 || ttc.TransactionIndex != 2 || ttc.LogIndex != 3 {
		t.Errorf("unexpected TokenTransferCheckpoint values: %+v", ttc)
	}
}
