package tdx

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type CWRecord struct {
	Code         string
	ReportDate   uint32
	AnnounceDate uint32
	Values       []float32
}

// ParseFinancialDAT 解析通达信财务数据文件
func ParseFinancialDAT(path string) ([]CWRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	// 文件头格式 <1hI1H3L -> int16, uint32, uint16, uint32, uint32, uint32
	var (
		flag              int16
		reportDate        uint32
		maxCount          uint16
		dummy, reportSize uint32
	)

	err = binary.Read(file, binary.LittleEndian, &flag)
	if err != nil {
		return nil, err
	}
	if err = binary.Read(file, binary.LittleEndian, &reportDate); err != nil {
		return nil, err
	}
	if err = binary.Read(file, binary.LittleEndian, &maxCount); err != nil {
		return nil, err
	}
	if err = binary.Read(file, binary.LittleEndian, &dummy); err != nil {
		return nil, err
	}
	if err = binary.Read(file, binary.LittleEndian, &reportSize); err != nil {
		return nil, err
	}
	if err = binary.Read(file, binary.LittleEndian, &dummy); err != nil {
		return nil, err
	}

	fmt.Printf("report date:%d count:%d size:%d field:%d\n", reportDate, maxCount, reportSize, reportSize/4)
	// 单个股票信息头部结构 <6s1c1L -> [6]byte + 1byte + uint32
	stockItemSize := int64(6 + 1 + 4)
	reportFieldsCount := int(reportSize / 4)

	var results []CWRecord

	for i := 0; i < int(maxCount); i++ {
		offset := 20 + int64(i)*stockItemSize // header = 20 bytes
		_, err := file.Seek(offset, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("seek stock item: %w", err)
		}

		var codeBytes [6]byte
		if _, err := io.ReadFull(file, codeBytes[:]); err != nil {
			return nil, err
		}

		// skip 1 byte (flag)
		var _flag [1]byte
		if _, err := io.ReadFull(file, _flag[:]); err != nil {
			return nil, err
		}

		var dataOffset uint32
		if err := binary.Read(file, binary.LittleEndian, &dataOffset); err != nil {
			return nil, err
		}

		code := string(codeBytes[:])
		// trim nulls
		for len(code) > 0 && code[len(code)-1] == 0 {
			code = code[:len(code)-1]
		}

		// 跳到数据偏移区
		_, err = file.Seek(int64(dataOffset), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("seek data: %w", err)
		}

		values := make([]float32, reportFieldsCount)
		if err := binary.Read(file, binary.LittleEndian, &values); err != nil {
			return nil, err
		}

		results = append(results, CWRecord{
			Code:         code,
			ReportDate:   reportDate,
			AnnounceDate: uint32(values[313]), //yymmdd
			Values:       values,
		})
	}

	return results, nil
}
