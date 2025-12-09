package tdx

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
)

type GpRecord struct {
	Code       string
	Mkt        string
	RecType    byte
	ReportDate uint32
	Val1       float32
	Val2       float32
}

// ParseGpDAT 解析通达信股票数据文件
func ParseGpDAT(path, mkt, code string) ([]GpRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	const recordSize = 13
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}
	if info.Size()%recordSize != 0 {
		return nil, fmt.Errorf("invalid record size: file length %d not multiple of %d", info.Size(), recordSize)
	}

	totalRecords := int(info.Size() / recordSize)
	results := make([]GpRecord, 0, totalRecords)
	buf := make([]byte, recordSize)

	for idx := 0; ; idx++ {
		_, err := io.ReadFull(file, buf)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("file truncated at record %d", idx)
		}
		if err != nil {
			return nil, fmt.Errorf("read record %d: %w", idx, err)
		}

		rec := GpRecord{
			Code:       code,
			Mkt:        mkt,
			RecType:    buf[0],
			ReportDate: binary.LittleEndian.Uint32(buf[1:5]),
			Val1:       math.Float32frombits(binary.LittleEndian.Uint32(buf[5:9])),
			Val2:       math.Float32frombits(binary.LittleEndian.Uint32(buf[9:13])),
		}
		results = append(results, rec)
	}

	return results, nil
}
