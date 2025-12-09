package tdx

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

// 生猪养殖|881112|12|1|1|X200201
type BlockCfg struct {
	Name   string
	Code   string
	Type   string // 保留字段
	Child  bool
	Parent string
	Ref    string
}

var blkCfgMap = map[string]string{
	"2":  "hy",   //行业
	"3":  "dq",   //地区
	"4":  "gn",   //概念
	"5":  "fg",   //风格
	"6":  "zs",   //指数
	"12": "yjhy", //一级行业？
}

// ReadBlockCfg 解析 datatool/vipdoc/base/tdxzs3.cfg 文件.
func ReadBlockCfg(path string) ([]BlockCfg, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open block config: %w", err)
	}
	defer f.Close()

	reader := transform.NewReader(f, simplifiedchinese.GBK.NewDecoder())
	scanner := bufio.NewScanner(reader)

	// 某些行包含较长的中文，放宽默认 64K 限制避免截断。
	const maxScanTokenSize = 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxScanTokenSize)

	var cfgs []BlockCfg
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		cfg, err := parseBlockCfgLine(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		cfgs = append(cfgs, cfg)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read block config: %w", err)
	}

	assignBlockParents(cfgs)
	return cfgs, nil
}

func parseBlockCfgLine(line string) (BlockCfg, error) {
	parts := strings.Split(line, "|")
	if len(parts) < 6 {
		return BlockCfg{}, fmt.Errorf("invalid block cfg line: %s", line)
	}

	var cleaned [6]string
	for i := 0; i < 6 && i < len(parts); i++ {
		cleaned[i] = strings.TrimSpace(parts[i])
	}

	return BlockCfg{
		Name:  cleaned[0],
		Code:  cleaned[1],
		Type:  blkCfgMap[cleaned[2]],
		Child: cleaned[4] == "1",
		Ref:   cleaned[5],
	}, nil
}

func assignBlockParents(cfgs []BlockCfg) {
	refToCode := make(map[string]string, len(cfgs))
	for _, cfg := range cfgs {
		if cfg.Ref != "" {
			refToCode[cfg.Ref] = cfg.Code
		}
	}

	for i := range cfgs {
		ref := cfgs[i].Ref
		if ref == "" {
			continue
		}
		cfgs[i].Parent = findParentCode(ref, refToCode)
	}
}

func findParentCode(ref string, refToCode map[string]string) string {
	runes := []rune(ref)
	for i := len(runes) - 1; i > 0; i-- {
		prefix := string(runes[:i])
		if parentCode, ok := refToCode[prefix]; ok {
			return parentCode
		}
	}
	return ""
}
