package cmd

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

var CW_FILE_URL = "https://data.tdx.com.cn/tdxfin/"
var CW_ALL_URL = "https://data.tdx.com.cn/vipdoc/"

func Cw(dbPath, cwFileDir string, download bool) error {
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
	fmt.Printf("🌟 发现 %d 个新的财务文件: %v oldhash:%v newhash:%v download:%v\n", len(updatedFiles), updatedFiles, olds, news, download)

	if len(updatedFiles) > 50 && download {
		fmt.Printf("❕will try download all\n")
		zipPath := filepath.Join(cwFileDir, "tdxfin.zip")
		if err := downloadFile(zipPath, "tdxfin.zip", CW_ALL_URL, true); err != nil {
			return err
		}
		if err := removeGlob(filepath.Join(cwFileDir, "gpcw*.dat")); err != nil {
			return err
		}
		if err := removeGlob(filepath.Join(cwFileDir, "gpcw*.zip")); err != nil {
			return err
		}
		if err := unzip(zipPath, cwFileDir); err != nil {
			return fmt.Errorf("failed to unzip file %s: %w", zipPath, err)
		}
		download = false
	}

	if download {
		for _, v := range updatedFiles {
			targetPath := filepath.Join(cwFileDir, v)
			if err := downloadFile(targetPath, v, CW_FILE_URL, true); err != nil {
				return err
			}
			if err := unzip(targetPath, cwFileDir); err != nil {
				return fmt.Errorf("failed to unzip file %s: %w", targetPath, err)
			}
		}
	}

	allFiles := make([]string, 0, len(latestHashes))
	for f := range latestHashes {
		if strings.HasSuffix(f, ".zip") {
			allFiles = append(allFiles, f)
		}
	}
	sort.Strings(allFiles)
	if len(allFiles) == 0 {
		fmt.Println("ℹ️ 未发现 CW 文件，跳过重建")
		return nil
	}

	if err := rebuildCwTableFromFiles(db, cwFileDir, allFiles); err != nil {
		return err
	}

	err = database.CreateCwViews(db)
	if err != nil {
		fmt.Printf("❌ 更新财务视图失败%v\n", err)
	} else {
		fmt.Print("✅ 已更新财务视图\n")
	}

	return nil
}

func rebuildCwTableFromFiles(db *sql.DB, cwFileDir string, zipFiles []string) error {
	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(zipFiles) {
		workerCount = len(zipFiles)
	}

	fmt.Printf("🚀 CW 重建: files=%d workers=%d\n", len(zipFiles), workerCount)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan string, workerCount*2)
	batches := make(chan database.CwRebuildBatch, workerCount*2)

	writerErrCh := make(chan error, 1)
	go func() {
		err := database.RebuildCwTable(ctx, db, batches)
		if err != nil {
			cancel()
		}
		writerErrCh <- err
	}()

	var workerErr error
	var workerErrOnce sync.Once
	setWorkerErr := func(err error) {
		workerErrOnce.Do(func() {
			workerErr = err
			cancel()
		})
	}

	var processed atomic.Int64
	total := int64(len(zipFiles))

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-jobs:
					if !ok {
						return
					}

					datName := strings.TrimSuffix(v, ".zip") + ".dat"
					datPath := filepath.Join(cwFileDir, datName)

					recs, err := tdx.ParseFinancialDAT(datPath)
					if err != nil {
						setWorkerErr(fmt.Errorf("failed to parse file %s: %w", datPath, err))
						return
					}

					if deduped, dupCount := dedupCwRecords(recs); dupCount > 0 {
						recs = deduped
					}

					if len(recs) > 0 {
						select {
						case batches <- database.CwRebuildBatch{Records: recs}:
						case <-ctx.Done():
							return
						}
					}

					n := processed.Add(1)
					if n%10 == 0 || n == total {
						fmt.Printf("📈 CW 进度: %d/%d\n", n, total)
					}
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, f := range zipFiles {
			select {
			case jobs <- f:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	close(batches)

	writerErr := <-writerErrCh
	if workerErr != nil {
		return workerErr
	}
	if writerErr != nil && writerErr != context.Canceled {
		return writerErr
	}
	return nil
}

func removeGlob(pattern string) error {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, p := range matches {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func dedupCwRecords(recs []tdx.CWRecord) ([]tdx.CWRecord, int) {
	type cwKey struct {
		code   string
		report uint32
	}

	dupCount := 0
	seen := make(map[cwKey]*tdx.CWRecord, len(recs))
	out := make([]tdx.CWRecord, 0, len(recs))

	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		key := cwKey{code: r.Code, report: r.ReportDate}
		if val, ok := seen[key]; ok {
			dupCount++
			fmt.Print(formatCwRecordDiff(r, val))
			continue
		}
		seen[key] = &recs[i]
		out = append(out, r)
	}

	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}

	return out, dupCount
}

func formatCwRecordDiff(rec tdx.CWRecord, old *tdx.CWRecord) string {
	var b strings.Builder
	fmt.Fprintf(&b, "dup cw code=%s report=%d announce=%d\n", rec.Code, rec.ReportDate, rec.AnnounceDate)

	maxLen := len(old.Values)

	for i := 0; i < maxLen; i++ {
		if old.Values[i] != rec.Values[i] {
			fmt.Fprintf(&b, "  %s[old:new]: %v -> %v\n", database.CwBaseDesc(i), old.Values[i], rec.Values[i])
		}
	}

	return b.String()
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
