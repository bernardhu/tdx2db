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

var gpColumnNames = buildColumnNames(gpbase)
var gpColumnLookup = buildColumnLookup(gpbase)

var GpSchema = TableSchema{
	Name:    "raw_gp_base",
	Columns: buildColumns(gpbase),
	Keys:    buildGpKeys(),
}

func fixDate(raw uint32) (uint32, bool) {
	if raw < 205000 && raw > 190000 { //yyyymm
		return raw*100 + 1, true
	}

	if raw > 19000000 {
		return raw, false
	}

	return raw, false
}

func ImportGpdata(db *sql.DB, rec []tdx.GpRecord) error {
	if err := CreateTable(db, GpSchema); err != nil {
		return fmt.Errorf("failed to create financial table: %w", err)
	}

	if len(rec) == 0 {
		return nil
	}

	tmpFile, err := os.CreateTemp("", "gp-base*.csv")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	if err := writer.Write(gpColumnNames); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	aggregated := make(map[uint32]map[string]float32)
	code := ""
	mkt := ""
	for _, record := range rec {
		code = record.Code
		mkt = record.Mkt
		desc, ok := gpColumnLookup[record.RecType]
		if !ok {
			continue
		}

		key := record.ReportDate
		if key == 0 && record.RecType == 10 {
			now := time.Now()
			key = uint32(now.Year()*10000) + uint32(now.Month()*100) + uint32(now.Day())
			record.ReportDate = key
		}

		fixday, fix := fixDate(record.ReportDate)
		if fix {
			fmt.Printf("fixday form %d to %d %v\n", record.ReportDate, fixday, record)
			record.ReportDate = fixday
			key = fixday
		}
		if key == 0 {
			fmt.Printf("0day skip %v\n", record)
			continue
		}

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
		row := make([]string, 0, len(gpColumnNames))
		row = append(row, code, mkt)
		if t, err := parseReportDate(k); err == nil {
			row = append(row, t.Format("2006-01-02"))
		} else {
			row = append(row, "")
			fmt.Printf("parse %d fail,err:%v\n", k, err)
		}

		values := aggregated[k]
		for _, col := range gpColumnNames[3:] {
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
	if err := ImportCSV(db, GpSchema, csvPath); err != nil {
		return fmt.Errorf("failed to import gp csv: %w", err)
	}

	return nil
}

func ImportBlkdata(db *sql.DB, rec []tdx.GpRecord) error {
	return nil
}

func ImportMktdata(db *sql.DB, rec []tdx.GpRecord) error {
	return nil
}

func buildColumns(descs []gpColumnDesc) []string {
	columns := []string{
		"code VARCHAR",
		"mkt VARCHAR",
		"rdate DATE",
	}

	for _, column := range descs {
		columns = append(columns, fmt.Sprintf("%s DOUBLE", column.name0))
		if column.name1 != "" {
			columns = append(columns, fmt.Sprintf("%s DOUBLE", column.name1))
		}
	}

	return columns
}

func buildGpKeys() []string {
	var columns []string
	columns = append(columns, "PRIMARY KEY (code, mkt, rdate)")

	return columns
}

func buildColumnNames(descs []gpColumnDesc) []string {
	columns := []string{"code", "mkt", "rdate"}
	for _, column := range descs {
		columns = append(columns, column.name0)
		if column.name1 != "" {
			columns = append(columns, column.name1)
		}
	}
	return columns
}

func buildColumnLookup(descs []gpColumnDesc) map[byte]gpColumnDesc {
	res := make(map[byte]gpColumnDesc, len(descs))
	for _, desc := range descs {
		res[desc.typ] = desc
	}
	return res
}
