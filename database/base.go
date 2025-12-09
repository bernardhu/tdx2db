package database

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
)

var BaseSchema = TableSchema{
	Name: "raw_base",
	Columns: []string{
		"code VARCHAR",
		"mkt VARCHAR",
		"zgb DOUBLE /*总股本*/",
		"bg DOUBLE /*流通b股*/",
		"hg DOUBLE /*流通h股*/",
		"ltag DOUBLE /*流通A股*/",
		"gdrs DOUBLE /*股东人数*/",
		"ssdate DATE /*上市日期*/",
		"mgjzc DOUBLE /*每股净资产*/",
		"mgsy DOUBLE /*每股收益*/",

		"yyzsr DOUBLE /*营业总收入 *1000*/",
		"yycb DOUBLE /*营业成本 *1000*/",
		"yylr DOUBLE /*营业利润 *1000*/",
		"zlr DOUBLE /*利润总额*1000*/",
		"jlr DOUBLE /*净利润*1000*/",
		"gmjlr DOUBLE /*归母净利润*1000*/",

		"jyxjl DOUBLE /*经营活动产生的现金流量净额 *1000*/",
		"zxjl DOUBLE /*总现金流*1000*/",

		"ldzc DOUBLE /*流动资产合计*1000*/",
		"ch DOUBLE /*存货*1000*/",
		"gdzc DOUBLE /*固定资产*1000*/",
		"wxzc DOUBLE /*无形资产*1000*/",
		"zzc DOUBLE /*总资产*1000*/",
		"ldfz DOUBLE /*流动负债*1000*/",
		"yszk DOUBLE /*应收账款*1000*/",

		"jzc DOUBLE /*归母所有者权益*1000*/",
		"cqfz DOUBLE /*少数股东权益*1000*/",
		"wfplr DOUBLE /*未分配利润*1000*/",
		"zbgjj DOUBLE /*资本公积金*1000*/",
	},
}

var baseColumnNames = []string{
	"code", "mkt", "zgb", "bg", "hg", "ltag", "gdrs", "ssdate", "mgjzc", "mgsy",
	"yyzsr", "yycb", "yylr", "zlr", "jlr", "gmjlr", "jyxjl", "zxjl",
	"ldzc", "ch", "gdzc", "wxzc", "zzc", "ldfz", "yszk",
	"jzc", "cqfz", "wfplr", "zbgjj",
}

func ImportBase(db *sql.DB, recs []*model.DbfRecord) error {
	//每次导入都重新建表
	if err := DropTable(db, BaseSchema); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	if err := CreateTable(db, BaseSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	tmpFile, err := os.CreateTemp("", "tdx-base.csv")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	if err := writer.Write(baseColumnNames); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, record := range recs {
		ssd, _ := time.Parse("20060102", fmt.Sprintf("%d", record.SSDATE))
		row := make([]string, 0, len(baseColumnNames))
		row = append(row, record.Code, record.Mkt,
			strconv.FormatFloat(record.ZGB, 'f', -1, 32),
			strconv.FormatFloat(record.BG, 'f', -1, 32),
			strconv.FormatFloat(record.HG, 'f', -1, 32),
			strconv.FormatFloat(record.LTAG, 'f', -1, 32),
			strconv.Itoa(int(record.CQTZ)),
			ssd.Format("2006-01-02"),
			strconv.FormatFloat(record.TZMGJZ, 'f', -1, 32),
			strconv.FormatFloat(record.ZGG, 'f', -1, 32),

			strconv.FormatFloat(record.ZYSY, 'f', -1, 32),
			strconv.FormatFloat(record.ZYLY, 'f', -1, 32),
			strconv.FormatFloat(record.YYLY, 'f', -1, 32),
			strconv.FormatFloat(record.LYZE, 'f', -1, 32),
			strconv.FormatFloat(record.SHLY, 'f', -1, 32),
			strconv.FormatFloat(record.JLY, 'f', -1, 32),

			strconv.FormatFloat(record.BTSY, 'f', -1, 32),
			strconv.FormatFloat(record.YYWSZ, 'f', -1, 32),

			strconv.FormatFloat(record.LDZC, 'f', -1, 32),
			strconv.FormatFloat(record.SNSYTZ, 'f', -1, 32),
			strconv.FormatFloat(record.GDZC, 'f', -1, 32),
			strconv.FormatFloat(record.WXZC, 'f', -1, 32),
			strconv.FormatFloat(record.ZZC, 'f', -1, 32),
			strconv.FormatFloat(record.LDFZ, 'f', -1, 32),
			strconv.FormatFloat(record.QTLY, 'f', -1, 32),

			strconv.FormatFloat(record.JZC, 'f', -1, 32),
			strconv.FormatFloat(record.CQFZ, 'f', -1, 32),
			strconv.FormatFloat(record.WFPLY, 'f', -1, 32),
			strconv.FormatFloat(record.ZBGJJ, 'f', -1, 32),
		)

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

	if err := ImportCSV(db, BaseSchema, tmpFile.Name()); err != nil {
		return fmt.Errorf("failed to import CSV: %s %w", tmpFile.Name(), err)
	}

	return nil
}

var BlockSchema = TableSchema{
	Name: "raw_block",
	Columns: []string{
		"block VARCHAR",
		"blocktype VARCHAR",
		"code VARCHAR",
		"refcode VARCHAR",
		"level INT",
		"total INT",
	},
}

var blockColumnNames = []string{
	"block", "blocktype", "code", "refcode", "level", "total",
}

func CheckBlocks(db *sql.DB) error {
	//每次导入都重新建表
	if err := DropTable(db, BlockSchema); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	if err := CreateTable(db, BlockSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func ImportBlocks(db *sql.DB, recs []*tdx.BlockData, typ string, filter map[string]*tdx.BlockData, refs map[string]*tdx.BlockCfg) error {
	tmpFile, err := os.CreateTemp("", "tdx-block.csv")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	if err := writer.Write(blockColumnNames); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, record := range recs {
		old, ok := filter[record.Name]
		if ok && old.Level == record.Level && old.Count == record.Count {
			fmt.Printf("⚠️ ImportBlocks skip block name:%s level:%d count:%d\n", record.Name, record.Level, record.Count)
			continue
		} else {
			filter[record.Name] = record
		}

		row := make([]string, len(blockColumnNames))
		for _, code := range record.Codes {
			ref := refs[record.Name]
			row[0] = record.Name
			row[1] = typ
			row[2] = code
			if ref == nil {
				fmt.Printf("type:%s name:%s. not found\n", typ, record.Name)
				row[3] = ""
			} else {
				row[3] = ref.Code
			}
			row[4] = strconv.Itoa(int(record.Level))
			row[5] = strconv.Itoa(int(record.Count))
			if err := writer.Write(row); err != nil {
				return fmt.Errorf("failed to write CSV row for %s: %w", code, err)
			}
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp CSV: %w", err)
	}

	if err := ImportCSV(db, BlockSchema, tmpFile.Name()); err != nil {
		return fmt.Errorf("failed to import CSV: %s %w", tmpFile.Name(), err)
	}

	return nil
}

func ImportHyBlocks(db *sql.DB, recs []tdx.HyCfg, refs map[string]*tdx.BlockCfg) error {
	tmpFile, err := os.CreateTemp("", "tdx-block.csv")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	if err := writer.Write(blockColumnNames); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, record := range recs {
		ref := refs[record.TdxHy]
		row := make([]string, len(blockColumnNames))
		row[1] = "tdxhy"
		row[2] = record.Code
		if ref == nil {
			fmt.Printf("tdxhy:%s not found\n", record.TdxHy)
			row[3] = ""
			row[0] = ""
		} else {
			row[0] = ref.Name
			row[3] = ref.Code
		}
		row[4] = "2"
		row[5] = strconv.Itoa(int(record.TdxCnt))
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row for %s: %w", record.Code, err)
		}

		ref = refs[record.SWHy]
		if ref == nil {
			fmt.Printf("swhy:%s not found\n", record.SWHy)
			row[3] = ""
			row[0] = ""
		} else {
			row[0] = ref.Name
			row[3] = ref.Code
		}
		row[1] = "swhy"
		row[2] = record.Code
		row[4] = "2"
		row[5] = strconv.Itoa(int(record.SwCnt))
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row for %s: %w", record.Code, err)
		}

		writer.Flush()
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp CSV: %w", err)
	}

	if err := ImportCSV(db, BlockSchema, tmpFile.Name()); err != nil {
		return fmt.Errorf("failed to import CSV: %s %w", tmpFile.Name(), err)
	}

	return nil
}

var DelistSchema = TableSchema{
	Name: "raw_delist",
	Columns: []string{
		"code VARCHAR",
		"name VARCHAR",
		"inlist DATE",
		"delist DATE",
		"mkt VARCHAR",
	},
}

func ImportDelist(db *sql.DB, path string) error {
	if err := DropTable(db, DelistSchema); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	if err := CreateTable(db, DelistSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := ImportCSV(db, DelistSchema, path); err != nil {
		return fmt.Errorf("failed to import CSV: %s %w", path, err)
	}

	return nil
}
