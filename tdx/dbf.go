package tdx

import (
	"fmt"
	"strconv"

	"github.com/LindsayBradford/go-dbf/godbf"
	"github.com/jing2uo/tdx2db/model"
)

// ParseBaseDbf 解析通达信dbf文件
func ParseBaseDbf(from string) ([]*model.DbfRecord, error) {
	table, err := godbf.NewFromFile(from, "UTF8")
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fmt.Printf("dbf 字段数量: %d, 记录数量: %d\n", len(table.FieldNames()), table.NumberOfRecords())

	sh := 0
	sz := 0
	bj := 0

	var res []*model.DbfRecord
	for i := 0; i < table.NumberOfRecords(); i++ {
		rec := new(model.DbfRecord)
		for j := 0; j < len(table.FieldNames()); j++ {
			name := table.FieldNames()[j]
			val := table.FieldValue(i, j)
			switch name {
			case "SC":
				if val == "0" {
					rec.Mkt = "sz"
					sz = sz + 1
				} else if val == "1" {
					rec.Mkt = "sh"
					sh = sh + 1
				} else if val == "2" {
					rec.Mkt = "bj"
					bj = bj + 1
				}
			case "GPDM":
				rec.Code = val
			case "ZGB":
				rec.ZGB, _ = strconv.ParseFloat(val, 64)
				rec.ZGB = rec.ZGB * 10000
			case "BG":
				rec.BG, _ = strconv.ParseFloat(val, 64)
				rec.BG = rec.BG * 10000
			case "HG":
				rec.HG, _ = strconv.ParseFloat(val, 64)
				rec.HG = rec.HG * 10000
			case "LTAG":
				rec.LTAG, _ = strconv.ParseFloat(val, 64)
				rec.LTAG = rec.LTAG * 10000
			case "CQTZ":
				rec.CQTZ, _ = strconv.ParseUint(val, 10, 32)
			case "SSDATE":
				rec.SSDATE, _ = strconv.ParseUint(val, 10, 32)
			case "TZMGJZ":
				rec.TZMGJZ, _ = strconv.ParseFloat(val, 64)
			case "ZGG":
				rec.ZGG, _ = strconv.ParseFloat(val, 64)
			case "ZYSY":
				rec.ZYSY, _ = strconv.ParseFloat(val, 64)
				rec.ZYSY = rec.ZYSY * 1000
			case "ZYLY":
				rec.ZYLY, _ = strconv.ParseFloat(val, 64)
				rec.ZYLY = rec.ZYLY * 1000
			case "YYLY":
				rec.YYLY, _ = strconv.ParseFloat(val, 64)
				rec.YYLY = rec.YYLY * 1000
			case "LYZE":
				rec.LYZE, _ = strconv.ParseFloat(val, 64)
				rec.LYZE = rec.LYZE * 1000
			case "SHLY":
				rec.SHLY, _ = strconv.ParseFloat(val, 64)
				rec.SHLY = rec.SHLY * 1000
			case "JLY":
				rec.JLY, _ = strconv.ParseFloat(val, 64)
				rec.JLY = rec.JLY * 1000
			case "BTSY":
				rec.BTSY, _ = strconv.ParseFloat(val, 64)
				rec.BTSY = rec.BTSY * 1000
			case "YYWSZ":
				rec.YYWSZ, _ = strconv.ParseFloat(val, 64)
				rec.YYWSZ = rec.YYWSZ * 1000
			case "LDZC":
				rec.LDZC, _ = strconv.ParseFloat(val, 64)
				rec.LDZC = rec.LDZC * 1000
			case "SNSYTZ":
				rec.SNSYTZ, _ = strconv.ParseFloat(val, 64)
				rec.SNSYTZ = rec.SNSYTZ * 1000
			case "GDZC":
				rec.GDZC, _ = strconv.ParseFloat(val, 64)
				rec.GDZC = rec.GDZC * 1000
			case "WXZC":
				rec.WXZC, _ = strconv.ParseFloat(val, 64)
				rec.WXZC = rec.WXZC * 1000
			case "ZZC":
				rec.ZZC, _ = strconv.ParseFloat(val, 64)
				rec.ZZC = rec.ZZC * 1000
			case "LDFZ":
				rec.LDFZ, _ = strconv.ParseFloat(val, 64)
				rec.LDFZ = rec.LDFZ * 1000
			case "QTLY":
				rec.QTLY, _ = strconv.ParseFloat(val, 64)
				rec.QTLY = rec.QTLY * 1000
			case "JZC":
				rec.JZC, _ = strconv.ParseFloat(val, 64)
				rec.JZC = rec.JZC * 1000
			case "CQFZ":
				rec.CQFZ, _ = strconv.ParseFloat(val, 64)
				rec.CQFZ = rec.CQFZ * 1000
			case "WFPLY":
				rec.WFPLY, _ = strconv.ParseFloat(val, 64)
				rec.WFPLY = rec.WFPLY * 1000
			case "ZBGJJ":
				rec.ZBGJJ, _ = strconv.ParseFloat(val, 64)
				rec.ZBGJJ = rec.ZBGJJ * 1000
			case "ZBNB":
				rec.ZBNB, _ = strconv.ParseUint(val, 10, 64)

			}
		}

		res = append(res, rec)
	}

	fmt.Printf("sh:%d sz:%d bj:%d\n", sh, sz, bj)
	return res, nil
}
