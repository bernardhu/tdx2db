package tdx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"

	"io/ioutil"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

const indexSize = 100

// 文件头
type BlockHeader struct {
	Version     string //64byte
	IndexOffset uint32
	DataOffset  uint32
	_           uint32 // 保留字段
	_           uint32 // 保留字段
	_           uint32 // 保留字段
}

type BlockIndex struct {
	Name   string //64byte
	_      uint32 // 保留字段
	_      uint32 // 保留字段
	Length uint32
	Offset uint32
	_      uint32 // 保留字段
	_      uint32 // 保留字段
	_      uint32 // 保留字段
	_      uint32 // 保留字段
	Status uint32
}

type BlockData struct {
	Name  string
	Count uint16
	Level uint16
	Codes []string
}

func trimZero(b []byte) []byte {
	for i, v := range b {
		if v == 0x00 {
			return b[:i]
		}
	}
	return b
}

// GBK 转 UTF8
func gbkToUTF8(b []byte) string {
	reader := transform.NewReader(strings.NewReader(string(b)), simplifiedchinese.GBK.NewDecoder())
	res, _ := ioutil.ReadAll(reader)
	return string(res)
}

// readCString reads a GBK encoded, null-terminated string starting from offset.
func readCString(buf []byte, offset int) (string, int, error) {
	if offset >= len(buf) {
		return "", 0, io.ErrUnexpectedEOF
	}
	idx := bytes.IndexByte(buf[offset:], 0x00)
	if idx == -1 {
		return "", 0, fmt.Errorf("missing null terminator")
	}
	start := offset
	end := offset + idx
	return gbkToUTF8(buf[start:end]), end + 1, nil
}

// ---------- 文件解析 ----------

func readHeader(r io.Reader) (*BlockHeader, error) {
	buf := make([]byte, 84)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	version := gbkToUTF8(trimZero(buf[0:64]))
	ioffset := binary.LittleEndian.Uint32(buf[64:68])
	doffset := binary.LittleEndian.Uint32(buf[68:72])
	return &BlockHeader{
		Version:     version,
		IndexOffset: ioffset,
		DataOffset:  doffset,
	}, nil
}

func readIndexs(b *BlockHeader, r io.Reader) ([]BlockIndex, error) {
	total := b.DataOffset - b.IndexOffset
	nIdx := total / indexSize
	buf := make([]byte, indexSize)
	var res []BlockIndex
	for i := 0; i < int(nIdx); i++ {
		_, err := io.ReadFull(r, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}

		name := gbkToUTF8(trimZero(buf[0:64]))
		length := binary.LittleEndian.Uint32(buf[72:76])
		offset := binary.LittleEndian.Uint32(buf[76:80])
		status := binary.LittleEndian.Uint32(buf[96:100])
		res = append(res, BlockIndex{
			Name:   name,
			Length: length,
			Offset: offset,
			Status: status,
		})
	}

	return res, nil
}

func readBlockRecord(idx []BlockIndex, r io.Reader) ([]*BlockData, error) {
	bufSize := 0
	for _, v := range idx {
		bufSize = bufSize + int(v.Length)
	}
	if bufSize == 0 {
		return nil, fmt.Errorf("no block data located")
	}

	buf := make([]byte, bufSize)
	if _, err := io.ReadFull(r, buf); err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("read blocks: %w", err)
	}

	blockLen := 0
	for _, v := range idx {
		if v.Name == "Block" && v.Length >= 2 {
			bbuf := buf[v.Offset : v.Offset+v.Length]
			blockLen = int(binary.LittleEndian.Uint16(bbuf))
			break
		}
	}
	if blockLen == 0 {
		return nil, fmt.Errorf("invalid block count")
	}

	var dataBufs []byte
	var dataSize int
	for _, v := range idx {
		if v.Name == "Val" {
			dataBufs = buf[v.Offset : v.Offset+v.Length]
			if blockLen > 0 {
				dataSize = int(v.Length) / blockLen
			}
			break
		}
	}
	if len(dataBufs) == 0 || dataSize == 0 {
		return nil, fmt.Errorf("block data section missing")
	}

	//fmt.Printf("blockLen:%d size:%d\n", blockLen, dataSize)
	res := make([]*BlockData, 0, blockLen)
	for i := 0; i < blockLen; i++ {
		start := i * dataSize
		end := (i + 1) * dataSize
		if end > len(dataBufs) {
			return nil, fmt.Errorf("block data overflow for index %d", i)
		}
		recBuf := dataBufs[start:end]
		//fmt.Printf("start:%d end:%d data:%x\n", start, end, recBuf)
		rec, err := parseBlockData(recBuf)
		if err != nil {
			return nil, fmt.Errorf("parse block %d: %w", i, err)
		}

		res = append(res, rec)
		//fmt.Printf("start:%d end:%d parsedata:%v \n", start, end, *rec)
	}
	return res, nil
}

func parseBlockData(buf []byte) (*BlockData, error) {
	offset := 0
	name := gbkToUTF8(trimZero(buf[0:9]))
	offset = offset + 9
	count := binary.LittleEndian.Uint16(buf[offset : offset+2])
	offset += 2
	level := binary.LittleEndian.Uint16(buf[offset : offset+2])
	offset += 2

	codes := make([]string, 0, count)
	for i := 0; i < int(count); i++ {
		code, next, err := readCString(buf, offset)
		if err != nil {
			return nil, fmt.Errorf("read code %d: %w", i, err)
		}
		codes = append(codes, code)
		offset = next
	}

	return &BlockData{
		Name:  name,
		Count: count,
		Level: level,
		Codes: codes,
	}, nil
}

func ReadBlock(path string) ([]*BlockData, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	header, err := readHeader(f)
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	//fmt.Printf("header %s %v\n", path, *header)

	ids, err := readIndexs(header, f)
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}

	res, err := readBlockRecord(ids, f)
	if err != nil {
		fmt.Printf("err:%v", err)
		return nil, fmt.Errorf("read block data: %w", err)
	}
	//fmt.Printf("len:%d\n", len(res))
	return res, nil
}

// sz: https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1793_ssgs&TABKEY=tab2&random=0.11267117882064226
// sh: https://www.sse.com.cn/assortment/stock/list/delisting/
// bj: ?
func ReadDelist(path string) ([]*BlockData, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	header, err := readHeader(f)
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	//fmt.Printf("header %s %v\n", path, *header)

	ids, err := readIndexs(header, f)
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}

	res, err := readBlockRecord(ids, f)
	if err != nil {
		fmt.Printf("err:%v", err)
		return nil, fmt.Errorf("read block data: %w", err)
	}
	//fmt.Printf("len:%d\n", len(res))
	return res, nil
}
