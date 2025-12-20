package database

import (
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

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

// ------------------------------------------------
var BlkSchema = TableSchema{
	Name:    "raw_gp_blk",
	Columns: buildColumns(blkbase),
	Keys:    buildBlkKeys(),
}

func buildBlkKeys() []string {
	var columns []string
	columns = append(columns, "PRIMARY KEY (code, rdate)")

	return columns
}

// ------------------------------------------------
var MktSchema = TableSchema{
	Name:    "raw_gp_mkt",
	Columns: buildColumns(mktbase),
	Keys:    buildMktKeys(),
}

func buildMktKeys() []string {
	var columns []string
	columns = append(columns, "PRIMARY KEY (code, rdate)")

	return columns
}
