package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

func Gp(dbPath, gpFileDir string) error {
	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}
	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	fmt.Printf("📦 开始处理股票目录: %s\n", gpFileDir)
	err = utils.CheckDirectory(gpFileDir)
	if err != nil {
		return err
	}

	targetPath := filepath.Join(gpFileDir, "gpszsh.txt")
	existingHashes, err := loadHashes(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read existing gpcw cache: %w", err)
	}

	url := "https://data.tdx.com.cn/tdxgp/gpszsh.txt"
	status, err := utils.DownloadFile(url, targetPath)
	if err != nil {
		return fmt.Errorf("failed to download gpcw.txt: %w", err)
	}

	switch status {
	case 200:
		fmt.Print("✅ 已下载 gpszsh.txt\n")
	case 404:
		fmt.Printf("🟡 gpszsh.txt 无法访问\n")
		return nil
	default:
		fmt.Printf("⚠️ gpszsh.txt 返回状态码: %d\n", status)
		return nil
	}

	latestHashes, err := loadHashes(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read latest gpcw.txt: %w", err)
	}

	updatedFiles, olds, news := diffHashes(existingHashes, latestHashes)
	if len(updatedFiles) == 0 {
		fmt.Println("ℹ️ 没有新的股票文件需要更新")
		return nil
	}

	sort.Strings(updatedFiles)
	fmt.Printf("🌟 发现 %d 个新的股票文件: %v oldhash:%v newhash:%v\n", len(updatedFiles), updatedFiles, olds, news)

	for _, v := range updatedFiles {
		url := fmt.Sprintf("https://data.tdx.com.cn/tdxgp/%s", v)
		targetPath := filepath.Join(gpFileDir, v)

		cmd := exec.Command("wget", "-O", targetPath, url)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			fmt.Printf("⚠️ wget 下载 %s 失败: %v\n", url, err)
			continue
		}

		fmt.Printf("✅ 已下载 %s %s\n", url, targetPath)
		/*
			recs, err := tdx.ParseFinancialDAT(targetPath)
			if err != nil {
				return fmt.Errorf("failed to parse file %s: %w", targetPath, err)
			}
			fmt.Printf("✅ 已解析财务数据%s\n", targetPath)

			err = database.ImportCaiwu(db, recs)
			if err != nil {
				return fmt.Errorf("failed to import cw file %w", err)
			}
			fmt.Printf("✅ 已导入财务数据%s\n", targetPath)
		*/
	}

	return nil
}
