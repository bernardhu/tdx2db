package database

import (
	"fmt"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

var CaiwuSchema = TableSchema{
	Name:    "raw_caiwu",
	Columns: buildCaiwuColumns(),
	Keys:    buildCwKeys(),
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
