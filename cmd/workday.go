package cmd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

func Workday(dbPath, dayFileDir, year string) error {
	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}

	fmt.Printf("📦 开始处理日线目录: %s\n", dayFileDir)
	err := utils.CheckDirectory(dayFileDir)
	if err != nil {
		return err
	}
	fmt.Println("🐢 开始下载工作日数据")
	targetPath := filepath.Join(dayFileDir, "workday.zip")
	urlTemplate := "https://www.tdx.com.cn/products/autoup/Except%s.zip"
	url := fmt.Sprintf(urlTemplate, year)
	status, err := utils.DownloadFile(url, targetPath)
	switch status {
	case 200:
		fmt.Printf("✅ 已下载 %s 的数据\n", year)

		if err := utils.UnzipFile(targetPath, dayFileDir); err != nil {
			fmt.Printf("⚠️ 解压文件 %s 失败: %v\n", targetPath, err)
			return err
		}
	case 404:
		fmt.Printf("🟡 %s 非交易日或数据尚未更新\n", year)
		return nil
	default:
		if err != nil {
			return nil
		}
	}

	fmt.Println("🔥 下载完成")

	var files []string
	suffix := "txt"
	err = filepath.WalkDir(dayFileDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, suffix) {
			files = append(files, path)
			return nil
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to traverse directory %s: %w", dayFileDir, err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no valid '%s' files found with the given prefixes", suffix)
	}

	fmt.Printf("📦 获取到工作日文件: %v\n", files)
	wds := make(map[string]bool)
	for _, p := range files {
		bs, err := os.ReadFile(p)
		if err != nil {
			fmt.Printf("⚠️ 读取工作日文件 %s 失败: %v\n", p, err)
			continue
		}

		arr := bytes.Split(bs, []byte("="))
		if len(arr) == 2 {
			sarr := bytes.Split(bs, []byte(","))
			for _, v := range sarr {
				sv := string(v)
				if sv != "" {
					wds[sv] = true
				}
			}
		}
	}

	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	if err := database.ImportWorkday(db, wds, year); err != nil {
		return fmt.Errorf("failed to import stock CSV: %w", err)
	}

	if err := cleanDayFiles(dayFileDir); err != nil {
		return fmt.Errorf("failed to clean workday files: %w", err)
	}

	fmt.Println("🚀 股票数据导入成功")
	return nil
}

func cleanDayFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to remove %s: %w", path, err)
		}
	}

	return nil
}
