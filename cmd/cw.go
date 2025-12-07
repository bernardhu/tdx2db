package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

func Cw(dbPath, cwFileDir string) error {
	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}
	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	fmt.Printf("📦 开始处理财务目录: %s\n", cwFileDir)
	err = utils.CheckDirectory(cwFileDir)
	if err != nil {
		return err
	}

	targetPath := filepath.Join(cwFileDir, "gpcw.txt")
	existingHashes, err := loadHashes(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read existing gpcw cache: %w", err)
	}

	url := "https://data.tdx.com.cn/tdxfin/gpcw.txt"
	status, err := utils.DownloadFile(url, targetPath)
	if err != nil {
		return fmt.Errorf("failed to download gpcw.txt: %w", err)
	}

	switch status {
	case 200:
		fmt.Print("✅ 已下载 gpcw.txt\n")
	case 404:
		fmt.Printf("🟡 gpcw.txt 无法访问\n")
		return nil
	default:
		fmt.Printf("⚠️ gpcw.txt 返回状态码: %d\n", status)
		return nil
	}

	latestHashes, err := loadHashes(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read latest gpcw.txt: %w", err)
	}

	updatedFiles, olds, news := diffHashes(existingHashes, latestHashes)
	if len(updatedFiles) == 0 {
		fmt.Println("ℹ️ 没有新的财务文件需要更新")
		return nil
	}

	sort.Strings(updatedFiles)
	fmt.Printf("🌟 发现 %d 个新的财务文件: %v oldhash:%v newhash:%v\n", len(updatedFiles), updatedFiles, olds, news)

	for _, v := range updatedFiles {
		url := fmt.Sprintf("https://data.tdx.com.cn/tdxfin/%s", v)
		targetPath := filepath.Join(cwFileDir, v)

		cmd := exec.Command("wget", "-O", targetPath, url)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			fmt.Printf("⚠️ wget 下载 %s 失败: %v\n", url, err)
			continue
		}

		fmt.Printf("✅ 已下载 %s %s\n", url, targetPath)
		if err := utils.UnzipFile(targetPath, cwFileDir); err != nil {
			return fmt.Errorf("failed to unzip file %s: %w", targetPath, err)
		}

		dataPath := strings.ReplaceAll(targetPath, "zip", "dat")

		recs, err := tdx.ParseFinancialDAT(dataPath)
		if err != nil {
			return fmt.Errorf("failed to parse file %s: %w", dataPath, err)
		}
		fmt.Printf("✅ 已解析财务数据%s\n", dataPath)

		err = database.ImportCaiwu(db, recs)
		if err != nil {
			return fmt.Errorf("failed to import cw file %w", err)
		}
		fmt.Printf("✅ 已导入财务数据%s\n", dataPath)

	}

	err = database.CreateXjllbView(db)
	if err == nil {
		fmt.Print("✅ 已更新现金流量表视图\n")
	}
	err = database.CreateLrbView(db)
	if err == nil {
		fmt.Print("✅ 已更新利润表视图\n")
	}
	err = database.CreateZcfzbView(db)
	if err == nil {
		fmt.Print("✅ 已更新资产负债表视图\n")
	}

	return nil
}

func loadHashes(path string) (map[string]string, error) {
	hashes := make(map[string]string)

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return hashes, nil
		}
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) < 2 {
			continue
		}

		name := strings.TrimSpace(parts[0])
		hash := strings.TrimSpace(parts[1])
		if name == "" || hash == "" {
			continue
		}
		hashes[name] = hash
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return hashes, nil
}

func diffHashes(oldHashes, newHashes map[string]string) ([]string, []string, []string) {
	var updated []string
	var hashold []string
	var hashnew []string
	for name, newHash := range newHashes {
		if oldHash, ok := oldHashes[name]; !ok || oldHash != newHash {
			updated = append(updated, name)
			hashold = append(hashold, oldHashes[name])
			hashnew = append(hashnew, newHash)
		}
	}
	return updated, hashold, hashnew
}
