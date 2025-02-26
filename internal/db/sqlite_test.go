package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func sampleTxFunc(tx *sql.Tx) (int, error) {
	// Simply return a fixed value.
	return 42, nil
}

func sampleTxFuncErr(tx *sql.Tx) (int, error) {
	// Simulate a failure within the transaction.
	return 0, errors.New("simulated error")
}

func TestDoInTx_Success(t *testing.T) {
	// Create sqlmock database.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	// Expect a transaction to begin.
	mock.ExpectBegin()

	// Expect commit to be called.
	mock.ExpectCommit()

	ctx := context.Background()
	result, err := DoInTx(ctx, db, sampleTxFunc)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if result != 42 {
		t.Errorf("expected result 42, got %d", result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestDoInTx_FuncError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	// Expect a transaction to begin.
	mock.ExpectBegin()
	// Since the function returns an error, we expect a rollback.
	mock.ExpectRollback()

	ctx := context.Background()
	_, err = DoInTx(ctx, db, sampleTxFuncErr)
	if err == nil {
		t.Error("expected an error from sampleTxFuncErr, got nil")
	}
	if err.Error() != "failed to execute transaction: simulated error" {
		t.Errorf("unexpected error message: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDoInTx_CommitError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	// Expect a transaction to begin.
	mock.ExpectBegin()
	// Expect commit to be called but simulate an error.
	mock.ExpectCommit().WillReturnError(fmt.Errorf("commit failed"))

	ctx := context.Background()
	_, err = DoInTx(ctx, db, sampleTxFunc)
	if err == nil {
		t.Error("expected commit error, got nil")
	}
	if err.Error() != "failed to commit transaction: commit failed" {
		t.Errorf("unexpected error message: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDoInTx_BeginTxError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	// Expect BeginTx to return an error.
	mock.ExpectBegin().WillReturnError(fmt.Errorf("begin error"))

	ctx := context.Background()
	_, err = DoInTx(ctx, db, sampleTxFunc)
	if err == nil {
		t.Error("expected error on BeginTx, got nil")
	}
	if err.Error() != "failed to begin transaction: begin error" {
		t.Errorf("unexpected error message: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
