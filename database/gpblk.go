package database

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/tdx"
)

var (
	blkColumnNames        = buildColumnNames([]string{"code", "rdate"}, blkbase)
	blkColumnLookup       = buildColumnLookup(blkbase)
	blkUpdateAssignments  = buildUpdateAssignments(blkColumnNames[2:])
	blkReadCSVColumnDef   = buildBlkReadCSVColumnDef()
	blkPrimaryKeyConflict = "code, rdate"
)

var BlkSchema = TableSchema{
	Name:    "raw_gp_blk",
	Columns: buildColumns(blkbase),
	Keys:    buildBlkKeys(),
}

func ImportBlkdata(db *sql.DB, rec []tdx.GpRecord) error {
	if err := CreateTable(db, BlkSchema); err != nil {
		return fmt.Errorf("failed to create financial table: %w", err)
	}

	if len(rec) == 0 {
		return nil
	}

	tmpFile, err := os.CreateTemp("", "gp-blk*.csv")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	//defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	if err := writer.Write(blkColumnNames); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	aggregated := make(map[uint32]map[string]float32)
	code := ""
	for _, record := range rec {
		code = record.Code
		desc, ok := blkColumnLookup[record.RecType]
		if !ok {
			continue
		}

		key := record.ReportDate

		if aggregated[key] == nil {
			aggregated[key] = make(map[string]float32)
		}

		aggregated[key][desc.name0] = record.Val1
		if desc.name1 != "" {
			aggregated[key][desc.name1] = record.Val2
		}
	}

	var keys []uint32
	for k, _ := range aggregated {
		keys = append(keys, k)
	}
	//fmt.Printf("keys:%v\n", keys)

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	//fmt.Printf("keys:%v\n", keys)

	for _, k := range keys {
		row := make([]string, 0, len(blkColumnNames))
		row = append(row, code)
		if t, err := parseReportDate(k); err == nil {
			row = append(row, t.Format("2006-01-02"))
		} else {
			row = append(row, "")
			fmt.Printf("parse %d fail,err:%v\n", k, err)
		}

		values := aggregated[k]
		for _, col := range blkColumnNames[2:] {
			if val, ok := values[col]; ok {
				row = append(row, strconv.FormatFloat(float64(val), 'f', -1, 32))
			} else {
				row = append(row, "")
			}
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write data row: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp CSV: %w", err)
	}

	csvPath := strings.ReplaceAll(tmpFile.Name(), "'", "''")
	tempTable := fmt.Sprintf("tmp_gp_blk_%d", time.Now().UnixNano())

	createTemp := fmt.Sprintf(`
		CREATE TEMP TABLE %s AS
		SELECT * FROM read_csv('%s', HEADER=TRUE, columns=%s, nullstr='')
	`, tempTable, csvPath, blkReadCSVColumnDef)

	if _, err := db.Exec(createTemp); err != nil {
		return fmt.Errorf("failed to load temp gp data: %w", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTable))

	columnsList := strings.Join(blkColumnNames, ", ")
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s FROM %s
		ON CONFLICT (%s) DO UPDATE SET %s
	`, BlkSchema.Name, columnsList, columnsList, tempTable, blkPrimaryKeyConflict, strings.Join(blkUpdateAssignments, ", "))

	if _, err := db.Exec(insertSQL); err != nil {
		return fmt.Errorf("failed to merge gp data: %w", err)
	}

	return nil
}

func buildBlkKeys() []string {
	var columns []string
	columns = append(columns, "PRIMARY KEY (code, rdate)")

	return columns
}

func buildBlkReadCSVColumnDef() string {
	var builder strings.Builder
	builder.WriteString("{'code': 'VARCHAR', 'rdate': 'DATE'")
	for _, column := range blkbase {
		builder.WriteString(", ")
		builder.WriteString(fmt.Sprintf("'%s': 'DOUBLE'", column.name0))
		if column.name1 != "" {
			builder.WriteString(", ")
			builder.WriteString(fmt.Sprintf("'%s': 'DOUBLE'", column.name1))
		}
	}
	builder.WriteString("}")
	return builder.String()
}
