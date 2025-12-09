package database

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/tdx"
)

var BlockCfgSchema = TableSchema{
	Name: "raw_block_cfg",
	Columns: []string{
		"name VARCHAR",
		"code VARCHAR",
		"typ VARCHAR",
		"child BOOL",
		"parent VARCHAR",
		"ref VARCHAR",
	},
}

var blockCfgColumnNames = []string{
	"name", "code", "typ", "child", "parent", "ref",
}

func ImportBlockCfgs(db *sql.DB, recs []tdx.BlockCfg) error {
	if err := DropTable(db, BlockCfgSchema); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	if err := CreateTable(db, BlockCfgSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	tmpFile, err := os.CreateTemp("", "tdx-block-cfg.csv")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	if err := writer.Write(blockCfgColumnNames); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, record := range recs {
		row := make([]string, 6)
		row[0] = record.Name
		row[1] = record.Code
		row[2] = record.Type
		row[3] = strconv.FormatBool(record.Child)
		row[4] = record.Parent
		row[5] = record.Ref
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

	if err := ImportCSV(db, BlockCfgSchema, tmpFile.Name()); err != nil {
		return fmt.Errorf("failed to import CSV: %s %w", tmpFile.Name(), err)
	}

	return nil
}
