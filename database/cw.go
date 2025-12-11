package database

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/tdx"
)

var CaiwuSchema = TableSchema{
	Name:    "raw_caiwu",
	Columns: buildCaiwuColumns(),
	Keys:    buildCwKeys(),
}

var caiwuColumnNames = buildCaiwuColumnNames()
var caiwuUpdateAssignments = buildCaiwuUpdateAssignments()
var caiwuReadCSVColumnDef = buildCaiwuReadCSVColumnDef()

func ImportCaiwu(db *sql.DB, rec []tdx.CWRecord) error {
	if err := CreateTable(db, CaiwuSchema); err != nil {
		return fmt.Errorf("failed to create financial table: %w", err)
	}

	if len(rec) == 0 {
		return nil
	}

	tmpFile, err := os.CreateTemp("", "caiwu-*.csv")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	if err := writer.Write(caiwuColumnNames); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, record := range rec {
		row := make([]string, 0, len(caiwuColumnNames))
		row = append(row, record.Code)

		if t, err := parseReportDate(record.ReportDate); err == nil {
			row = append(row, t.Format("2006-01-02"))
		} else {
			row = append(row, "")
		}

		if t, err := parseAnnounceDate(record.AnnounceDate); err == nil {
			row = append(row, t.Format("2006-01-02"))
		} else {
			row = append(row, "")
		}

		for _, column := range cwbase {
			if int(column.idx) < len(record.Values) {
				row = append(row, strconv.FormatFloat(float64(record.Values[column.idx]), 'f', -1, 32))
			} else {
				row = append(row, "")
			}
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row for %s: %w", record.Code, err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp CSV: %w", err)
	}

	tempTable := fmt.Sprintf("tmp_caiwu_%d", time.Now().UnixNano())
	csvPath := strings.ReplaceAll(tmpFile.Name(), "'", "''")

	createTemp := fmt.Sprintf(`
		CREATE TEMP TABLE %s AS
		SELECT * FROM read_csv('%s', HEADER=TRUE, columns=%s, nullstr='')
	`, tempTable, csvPath, caiwuReadCSVColumnDef)

	if _, err := db.Exec(createTemp); err != nil {
		return fmt.Errorf("failed to load temp financial data: %w", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTable))

	columnsList := strings.Join(caiwuColumnNames, ", ")

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s FROM %s
		ON CONFLICT (code, report_date) DO UPDATE SET %s
	`, CaiwuSchema.Name, columnsList, columnsList, tempTable, strings.Join(caiwuUpdateAssignments, ", "))

	if _, err := db.Exec(insertSQL); err != nil {
		return fmt.Errorf("failed to merge financial data: %w", err)
	}

	return nil
}

func buildCaiwuColumns() []string {
	columns := []string{
		"code VARCHAR",
		"report_date DATE",
		"announce_date DATE",
	}

	for _, column := range cwbase {
		comment := strings.ReplaceAll(column.desc, "*/", "")
		columns = append(columns, fmt.Sprintf("%s DOUBLE /* %s */", column.name, comment))
	}

	return columns
}

func buildCwKeys() []string {
	var columns []string
	columns = append(columns, "PRIMARY KEY (code, report_date)")

	return columns
}

func buildCaiwuColumnNames() []string {
	columns := []string{"code", "report_date", "announce_date"}
	for _, column := range cwbase {
		columns = append(columns, column.name)
	}
	return columns
}

func buildCaiwuUpdateAssignments() []string {
	assignments := make([]string, 0, len(caiwuColumnNames)-2)
	for _, col := range caiwuColumnNames[2:] {
		assignments = append(assignments, fmt.Sprintf("%s=excluded.%s", col, col))
	}
	return assignments
}

func parseReportDate(raw uint32) (time.Time, error) {
	if raw == 0 {
		return time.Time{}, fmt.Errorf("empty report date")
	}

	dateStr := fmt.Sprintf("%08d", raw)
	return time.Parse("20060102", dateStr)
}

func parseAnnounceDate(raw uint32) (time.Time, error) {
	if raw < 500000 { //yymmdd
		raw = raw + 20000000
	} else {
		raw = raw + 19000000
	}

	dateStr := fmt.Sprintf("%08d", raw)
	return time.Parse("20060102", dateStr)
}

func buildCaiwuReadCSVColumnDef() string {
	var builder strings.Builder
	builder.WriteString("{")
	builder.WriteString("'code': 'VARCHAR', 'report_date': 'DATE', 'announce_date': 'DATE'")
	for _, column := range cwbase {
		builder.WriteString(", ")
		builder.WriteString(fmt.Sprintf("'%s': 'DOUBLE'", column.name))
	}
	builder.WriteString("}")
	return builder.String()
}
