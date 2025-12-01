package database

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

var WorkdaySchema = TableSchema{
	Name: "raw_workday",
	Columns: []string{
		"date DATE PRIMARY KEY",
	},
}

func ImportWorkday(db *sql.DB, except map[string]bool, year string) error {
	if err := CreateTable(db, WorkdaySchema); err != nil {
		return fmt.Errorf("failed to create workday table: %w", err)
	}

	yearInt, err := strconv.Atoi(year)
	if err != nil {
		return fmt.Errorf("invalid year %s: %w", year, err)
	}

	startDate := time.Date(yearInt, time.January, 1, 0, 0, 0, 0, time.UTC)
	endDate := startDate.AddDate(1, 0, 0)

	workdays := make([]time.Time, 0, 260)
	for d := startDate; d.Before(endDate); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}

		key := d.Format("20060102")
		if except[key] {
			continue
		}

		workdays = append(workdays, d)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE date >= ? AND date < ?", WorkdaySchema.Name)
	if _, err := tx.Exec(deleteQuery, startDate, endDate); err != nil {
		return fmt.Errorf("failed to clean workday table: %w", err)
	}

	if len(workdays) == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit empty workday result: %w", err)
		}
		return nil
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (date) VALUES (?)", WorkdaySchema.Name)
	stmt, err := tx.Prepare(insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, d := range workdays {
		if _, err := stmt.Exec(d); err != nil {
			return fmt.Errorf("failed to insert workday %s: %w", d.Format("2006-01-02"), err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit workday import: %w", err)
	}

	return nil
}
