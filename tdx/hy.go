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
type HyCfg struct {
	Mkt    string
	Code   string
	TdxHy  string
	TdxCnt int
	SWHy   string
	SwCnt  int
}

// ReadCodeHy 解析 datatool/vipdoc/base/tdxhy.cfg 文件.
func ReadCodeHy(path string) ([]HyCfg, error) {
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

	var cfgs []HyCfg
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		cfg, err := parseCodeHyLine(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		cfgs = append(cfgs, cfg)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read block config: %w", err)
	}

	assignHyCounts(cfgs)
	return cfgs, nil
}

func parseCodeHyLine(line string) (HyCfg, error) {
	parts := strings.Split(line, "|")
	if len(parts) < 6 {
		return HyCfg{}, fmt.Errorf("invalid block code hy line: %s", line)
	}

	var cleaned [6]string
	for i := 0; i < 6 && i < len(parts); i++ {
		cleaned[i] = strings.TrimSpace(parts[i])
	}

	return HyCfg{
		Mkt:    cleaned[0],
		Code:   cleaned[1],
		TdxHy:  cleaned[2],
		TdxCnt: 0,
		SWHy:   cleaned[5],
		SwCnt:  0,
	}, nil
}

func assignHyCounts(cfgs []HyCfg) {
	tdxCnt := make(map[string]int)
	swCnt := make(map[string]int)
	for _, cfg := range cfgs {
		if cfg.TdxHy != "" {
			tdxCnt[cfg.TdxHy]++
		}
		if cfg.SWHy != "" {
			swCnt[cfg.SWHy]++
		}
	}

	for i := range cfgs {
		if cfgs[i].TdxHy != "" {
			cfgs[i].TdxCnt = tdxCnt[cfgs[i].TdxHy]
		}
		if cfgs[i].SWHy != "" {
			cfgs[i].SwCnt = swCnt[cfgs[i].SWHy]
		}
	}
}
