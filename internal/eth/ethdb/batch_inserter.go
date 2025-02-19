package ethdb

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"go.uber.org/zap"
)

// Default batch size
const defaultBatchSize = 100

// Insert handles batched inserts dynamically without requiring an instance
func Insert(db *sql.DB, table string, data interface{}) error {
	// Ensure data is a slice
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("data must be a slice, got %T", data)
	}

	// Ensure slice is not empty
	if v.Len() == 0 {
		return fmt.Errorf("data slice is empty")
	}

	// Extract struct fields dynamically
	elemType := v.Type().Elem()
	numFields := elemType.NumField()
	columns := make([]string, numFields)

	for i := 0; i < numFields; i++ {
		columns[i] = snakeCase(elemType.Field(i).Name) // Convert struct fields to snake_case column names
	}

	// Create SQL placeholders (?, ?, ?)
	placeholders := "(" + strings.Repeat("?, ", numFields-1) + "?)"

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Process batches
	for i := 0; i < v.Len(); i += defaultBatchSize {
		end := i + defaultBatchSize
		if end > v.Len() {
			end = v.Len()
		}

		// Prepare batch insert
		values := []interface{}{}
		batchPlaceholders := []string{}

		for j := i; j < end; j++ {
			elem := v.Index(j)
			rowValues := make([]interface{}, numFields)

			for k := 0; k < numFields; k++ {
				rowValues[k] = elem.Field(k).Interface()
			}

			values = append(values, rowValues...)
			batchPlaceholders = append(batchPlaceholders, placeholders)
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			table, strings.Join(columns, ", "), strings.Join(batchPlaceholders, ", "))

		_, err := tx.Exec(query, values...)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				zap.L().Error("Failed to rollback transaction", zap.Error(rollbackErr))
			}
			return err
		}
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// snakeCase converts CamelCase to snake_case
func snakeCase(s string) string {
	var result strings.Builder
	for i, c := range s {
		if i > 0 && c >= 'A' && c <= 'Z' {
			result.WriteRune('_')
		}
		result.WriteRune(c)
	}
	return strings.ToLower(result.String())
}
