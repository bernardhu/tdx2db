package cmd

import (
	"fmt"
	"path/filepath"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/utils"
)

func Cw(dbPath, cwFileDir string) error {
	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}

	fmt.Printf("📦 开始处理财务目录: %s\n", cwFileDir)
	err := utils.CheckDirectory(cwFileDir)
	if err != nil {
		return err
	}

	//hashes := make(map[string]string)
	targetPath := filepath.Join(cwFileDir, "gpcw.txt")
	url := "https://data.tdx.com.cn/tdxfin/gpcw.txt"
	status, err := utils.DownloadFile(url, targetPath)
	switch status {
	case 200:
		fmt.Print("✅ 已下载gpcw.txt\n")

	case 404:
		fmt.Printf("🟡 gpcw.txt 无法访问\n")
		return nil
	default:
		if err != nil {
			return nil
		}
	}

	return nil
}
