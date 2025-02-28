package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func OpenSqlite(path string) (*sql.DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create directory for SQLite: %w", err)
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite: %w", err)
	}

	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = EXTRA;
		PRAGMA cache_size = -2000;
		PRAGMA busy_timeout = 5000;
		PRAGMA foreign_keys = ON;
	`)
	if err != nil {
		if err := db.Close(); err != nil {
			zap.L().Error("Failed to close SQLite", zap.Error(err))
		}
		return nil, fmt.Errorf("failed to set SQLite pragmas: %w", err)
	}

	if err := migrateDatabase(db); err != nil {
		if err := db.Close(); err != nil {
			zap.L().Error("Failed to close SQLite", zap.Error(err))
		}
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}
	fmt.Println("Migrations applied")
	if err = db.Ping(); err != nil {
		if err := db.Close(); err != nil {
			zap.L().Error("Failed to close SQLite", zap.Error(err))
		}
		return nil, fmt.Errorf("failed to ping SQLite: %w", err)
	}

	zap.L().Info("Successfully opened SQLite database", zap.String("path", path))
	return db, nil
}

func migrateDatabase(db *sql.DB) error {
	driver, err := sqlite3.WithInstance(db, &sqlite3.Config{
		NoTxWrap: true, // Prevent the driver from managing transactions
	})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	d, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	m, err := migrate.NewWithInstance(
		"iofs", d,
		"sqlite3", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}

	// We don't defer m.Close() here because it would close our db connection
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

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
