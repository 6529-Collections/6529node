package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type QueryRunner interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

func TxRunner[T any](ctx context.Context, db *sql.DB, fn func(*sql.Tx) (T, error)) (result T, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				zap.L().Error("failed to rollback transaction", zap.Error(rbErr))
			}
		} else {
			if cmErr := tx.Commit(); cmErr != nil {
				zap.L().Error("failed to commit transaction", zap.Error(cmErr))
				err = fmt.Errorf("failed to commit transaction: %w", cmErr)
			}
		}
	}()

	// Execute the user-defined function
	result, err = fn(tx)
	if err != nil {
		return result, fmt.Errorf("failed to execute transaction: %w", err)
	}

	// Check if context was canceled after fn completed
	if ctx.Err() != nil {
		// This ensures we don't commit if the context is already canceled
		err = ctx.Err()
		return result, fmt.Errorf("context canceled before commit: %w", err)
	}

	return result, nil
}

type Scannable interface {
	ScanRow(scanner RowScanner) error
}

type RowScanner interface {
	Scan(dest ...interface{}) error
}

type QueryDirection string

const (
	QueryDirectionAsc  QueryDirection = "ASC"
	QueryDirectionDesc QueryDirection = "DESC"
)

type QueryOptions struct {
	Where     string
	PageSize  int
	Page      int
	Direction QueryDirection
}

type PaginatedQuerier[T any] interface {
	GetPaginatedResponseForQuery(rq QueryRunner, queryOptions QueryOptions, queryParams []interface{}) (total int, data []*T, err error)
}

func ScanOne[T Scannable](scanner RowScanner, factory func() T) (T, error) {
	item := factory()
	if err := item.ScanRow(scanner); err != nil {
		if err == sql.ErrNoRows {
			return item, nil
		}
		return item, err
	}
	return item, nil
}

// ScanAll scans all rows into a slice of type T.
func ScanAll[T Scannable](rows *sql.Rows, factory func() T) ([]T, error) {
	var items []T
	for rows.Next() {
		item, err := ScanOne(rows, factory)
		if err != nil {
			return nil, err
		}
		// Only append non-nil items.
		items = append(items, item)
	}
	return items, nil
}

func GetPaginatedResponseForQuery[T Scannable](
	tableName string,
	rq QueryRunner,
	baseQuery string,
	queryOptions QueryOptions,
	orderColumns []string,
	queryParams []interface{},
	factory func() T,
) (total int, data []T, err error) {
	// Calculate offset based on the page and page size.
	if len(orderColumns) == 0 {
		return 0, nil, errors.New("no order columns provided")
	}
	// Build the ORDER BY clause dynamically based on queryOptions.Direction.
	var orders []string
	for _, col := range orderColumns {
		orders = append(orders, fmt.Sprintf("%s %s", col, queryOptions.Direction))
	}
	orderClause := strings.Join(orders, ", ")

	// Calculate offset.
	offset := (queryOptions.Page - 1) * queryOptions.PageSize

	// Build WHERE clause
	whereClause := ""
	if queryOptions.Where != "" {
		whereClause = fmt.Sprintf("WHERE %s", queryOptions.Where)
	}

	// Build the main query with the provided ordering clause.
	query := fmt.Sprintf("%s %s ORDER BY %s LIMIT ? OFFSET ?", baseQuery, whereClause, orderClause)
	// Append page size and offset to the query parameters.
	params := append(queryParams, queryOptions.PageSize, offset)

	// Execute the main query.
	rows, err := rq.Query(query, params...)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	// Scan rows into a slice of T.
	data, err = ScanAll[T](rows, factory)
	if err != nil {
		return 0, nil, err
	}

	// Check for any errors encountered while iterating over rows.
	if err = rows.Err(); err != nil {
		return 0, nil, err
	}

	// Build the COUNT query.
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", tableName, whereClause)
	err = rq.QueryRow(countQuery, queryParams...).Scan(&total)
	if err != nil {
		return 0, nil, err
	}

	return total, data, nil
}
