package tdx

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

const tnfRecordSize = 360

// 文件头
type FileHeader struct {
	IPRaw     string
	Port      uint16
	RawDate   uint32
	RawTime   uint32
	Timestamp time.Time
}

// 股票记录
type StockRecord struct {
	Code       string
	Name       string
	Typ        byte //2 指数 4 债券（国债/可转债/）3 ETF/基金/b股 2股票
	PrevClose  float32
	NamePinyin string
	Mkt        string
	Scaling    float64
}

// ---------- 基础工具 ----------

// 去掉 0x00/0x01
func trimZeroAndOne(b []byte) []byte {
	for i, v := range b {
		if v == 0x00 || v == 0x01 {
			return b[:i]
		}
	}
	return b
}

// 日期时间组合
func parseTnfDateTime(rawDate, rawTime uint32) time.Time {
	if rawDate == 0 {
		return time.Time{}
	}
	year := int(rawDate / 10000)
	month := time.Month((rawDate / 100) % 100)
	day := int(rawDate % 100)

	hour := int(rawTime / 10000)
	minute := int((rawTime / 100) % 100)
	second := int(rawTime % 100)

	return time.Date(year, month, day, hour, minute, second, 0, time.Local)
}

// ---------- 文件解析 ----------
func readTnfHeader(r io.Reader) (*FileHeader, error) {
	buf := make([]byte, 50)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	ipRaw := string(trimZeroAndOne(buf[0:40]))
	port := binary.LittleEndian.Uint16(buf[40:42])
	rawDate := binary.LittleEndian.Uint32(buf[42:46])
	rawTime := binary.LittleEndian.Uint32(buf[46:50])
	return &FileHeader{
		IPRaw:     ipRaw,
		Port:      port,
		RawDate:   rawDate,
		RawTime:   rawTime,
		Timestamp: parseTnfDateTime(rawDate, rawTime),
	}, nil
}

func readTnfRecord(b []byte) *StockRecord {
	code := strings.TrimSpace(string(trimZero(b[0:6])))
	name := gbkToUTF8(trimZero(b[31:51]))
	prevClose := math.Float32frombits(binary.LittleEndian.Uint32(b[276:280]))
	pinyin := gbkToUTF8(trimZero(b[329:340]))
	typ := b[76]
	return &StockRecord{
		Code:       code,
		Name:       name,
		PrevClose:  prevClose,
		Typ:        typ,
		NamePinyin: pinyin,
	}
}

func readAllRecords(r io.Reader) ([]*StockRecord, error) {
	var records []*StockRecord
	buf := make([]byte, tnfRecordSize)
	for {
		_, err := io.ReadFull(r, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, readTnfRecord(buf))
	}
	return records, nil
}

func ReadTnfRecords(base string, cb func(*StockRecord, string)) {
	sh := base + "/shs.tnf"
	readTnf("sh", sh, cb)
	sz := base + "/szs.tnf"
	readTnf("sz", sz, cb)
	bj := base + "/bjs.tnf"
	readTnf("bj", bj, cb)
}

func readTnf(mkt, path string, cb func(*StockRecord, string)) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	header, err := readTnfHeader(f)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Path: %s IP: %s Port: %d Date: %d Time: %d Parsed: %s\n",
		path, header.IPRaw, header.Port, header.RawDate, header.RawTime, header.Timestamp.Format("2006-01-02 15:04:05"))

	records, err := readAllRecords(f)
	if err != nil {
		panic(err)
	}

	for i, r := range records {
		r.Mkt = mkt
		if r.Typ == 3 { //2 指数 4 债券（国债/可转债/）3 ETF/基金/b股 2股票
			r.Scaling = 1000
			fmt.Printf("%s:%d: Code=%s Name=%s PrevClose=%.3f Pinyin=%s typ:%d scaling:%f\n", mkt, i, r.Code, r.Name, r.PrevClose, r.NamePinyin, r.Typ, r.Scaling)
		} else if r.Typ == 4 { //2 指数 4 债券（国债/可转债/）3 ETF/基金/b股 2股票
			r.Scaling = 10000
			fmt.Printf("%s:%d: Code=%s Name=%s PrevClose=%.3f Pinyin=%s typ:%d scaling:%f\n", mkt, i, r.Code, r.Name, r.PrevClose, r.NamePinyin, r.Typ, r.Scaling)
		} else {
			r.Scaling = 100
			fmt.Printf("%s:%d: Code=%s Name=%s PrevClose=%.2f Pinyin=%s typ:%d scaling:%f\n", mkt, i, r.Code, r.Name, r.PrevClose, r.NamePinyin, r.Typ, r.Scaling)
		}

		if cb != nil {
			subtype := parseCode(r.Mkt, r.Code)
			cb(r, subtype)
		}
	}
}
