package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/tdx"
)

func ImportStockDayFiles(db *sql.DB, dayFileDir string, validPrefixes []string) error {
	if err := DropTable(db, StocksSchema); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	if err := CreateTable(db, StocksSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get database conn: %w", err)
	}
	defer conn.Close()

	if err := conn.Raw(func(dc any) error {
		driverConn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver conn type %T", dc)
		}

		appender, err := duckdb.NewAppenderFromConn(driverConn, "", StocksSchema.Name)
		if err != nil {
			return fmt.Errorf("new appender: %w", err)
		}
		closed := false
		defer func() {
			if closed {
				return
			}
			_ = appender.Close()
		}()

		rowValues := make([]driver.Value, 8)
		if err := tdx.StreamDayFiles(dayFileDir, validPrefixes, func(record tdx.DayKlineRecord) error {
			rowValues[0] = record.Symbol
			rowValues[1] = record.Open
			rowValues[2] = record.High
			rowValues[3] = record.Low
			rowValues[4] = record.Close
			rowValues[5] = record.Amount
			rowValues[6] = record.Volume
			rowValues[7] = record.Date
			return appender.AppendRow(rowValues...)
		}); err != nil {
			return fmt.Errorf("stream day files: %w", err)
		}

		if err := appender.Close(); err != nil {
			return fmt.Errorf("close appender: %w", err)
		}
		closed = true
		return nil
	}); err != nil {
		return fmt.Errorf("append rows: %w", err)
	}

	return nil
}

func Import1MinLineFiles(db *sql.DB, fileDir string, validPrefixes []string) error {
	return importMinLineFiles(db, OneMinLineSchema, fileDir, validPrefixes, ".01")
}

func Import5MinLineFiles(db *sql.DB, fileDir string, validPrefixes []string) error {
	return importMinLineFiles(db, FiveMinLineSchema, fileDir, validPrefixes, ".5")
}

func importMinLineFiles(db *sql.DB, schema TableSchema, fileDir string, validPrefixes []string, suffix string) error {
	if err := DropTable(db, schema); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	if err := CreateTable(db, schema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get database conn: %w", err)
	}
	defer conn.Close()

	if err := conn.Raw(func(dc any) error {
		driverConn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver conn type %T", dc)
		}

		appender, err := duckdb.NewAppenderFromConn(driverConn, "", schema.Name)
		if err != nil {
			return fmt.Errorf("new appender: %w", err)
		}
		closed := false
		defer func() {
			if closed {
				return
			}
			_ = appender.Close()
		}()

		rowValues := make([]driver.Value, 8)
		if err := tdx.StreamMinFiles(fileDir, validPrefixes, suffix, func(record tdx.MinKlineRecord) error {
			rowValues[0] = record.Symbol
			rowValues[1] = record.Open
			rowValues[2] = record.High
			rowValues[3] = record.Low
			rowValues[4] = record.Close
			rowValues[5] = record.Amount
			rowValues[6] = record.Volume
			rowValues[7] = record.Datetime
			return appender.AppendRow(rowValues...)
		}); err != nil {
			return fmt.Errorf("stream min files: %w", err)
		}

		if err := appender.Close(); err != nil {
			return fmt.Errorf("close appender: %w", err)
		}
		closed = true
		return nil
	}); err != nil {
		return fmt.Errorf("append rows: %w", err)
	}

	return nil
}

