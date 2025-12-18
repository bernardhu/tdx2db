package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

var GP_FILE_URL = "https://data.tdx.com.cn/tdxgp/"
var GP_ALL_URL = "https://data.tdx.com.cn/vipdoc/"

func Gp2(dbPath, gpFileDir string, download bool) error {
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
	filterHashes(existingHashes)
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

	filterHashes(latestHashes)
	updatedFiles, olds, news := diffHashes(existingHashes, latestHashes)
	if len(updatedFiles) == 0 {
		fmt.Println("ℹ️ 没有新的股票文件需要更新")
		return nil
	}

	sort.Strings(updatedFiles)
	fmt.Printf("🌟 发现 %d 个股票文件变更 oldhash:%v newhash:%v\n", len(updatedFiles), olds, news)

	updatedSet := make(map[string]struct{}, len(updatedFiles))
	for _, f := range updatedFiles {
		updatedSet[f] = struct{}{}
	}
	/*
		if len(updatedSet) > 2000 { //全部下载算了
			fmt.Printf("❕will try download all\n")
			zipPath := filepath.Join(gpFileDir, "tdxgp.zip")
			if err := downloadFile(zipPath, "tdxgp.zip", GP_ALL_URL, true); err != nil {
				return err
			}
			cmd := exec.Command("rm", "-f", gpFileDir+"/*.dat")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			if err := cmd.Run(); err != nil {
				fmt.Printf("⚠️ 删除旧文件失败\n")
				return nil
			}

			if err := utils.UnzipFile(zipPath, gpFileDir); err != nil {
				return fmt.Errorf("failed to unzip file %s: %w", targetPath, err)
			}
			return nil
		}
	*/

	allFiles := make([]string, 0, len(latestHashes))
	for f := range latestHashes {
		allFiles = append(allFiles, f)
	}
	sort.Strings(allFiles)

	var stockFiles []string
	var blkFiles []string
	var mktFiles []string
	for _, f := range allFiles {
		_, _, res := parseFileName(f)
		typ := res
		if res == "ashare" {
			typ = "stock"
		}
		switch typ {
		case "stock":
			stockFiles = append(stockFiles, f)
		case "tdx":
			blkFiles = append(blkFiles, f)
		case "mkt":
			mktFiles = append(mktFiles, f)
		}
	}

	if err := rebuildGpBaseFromFiles(db, gpFileDir, stockFiles, updatedSet, download); err != nil {
		return err
	}

	// 其他小文件（板块/市场）仍按原逻辑单线程导入
	for _, v := range append(blkFiles, mktFiles...) {
		targetPath := filepath.Join(gpFileDir, v)
		if err := downloadFile(targetPath, v, GP_FILE_URL, download); err != nil {
			return err
		}

		mkt, code, res := parseFileName(v)
		typ := res
		if res == "ashare" {
			typ = "stock"
		}

		recs, err := tdx.ParseGpDAT(targetPath, mkt, code)
		if err != nil {
			return fmt.Errorf("failed to parse file %s: %w", targetPath, err)
		}

		if typ == "tdx" { //block
			if err := database.ImportBlkdata(db, recs); err != nil {
				return fmt.Errorf("failed to import blk file %w", err)
			}
		} else if typ == "mkt" { //mkt
			if err := database.ImportMktdata(db, recs); err != nil {
				return fmt.Errorf("failed to import mkt file %w", err)
			}
		}
	}

	fmt.Printf("开始创建视图\n")
	err = database.CreateGpViews(db)
	if err != nil {
		fmt.Printf("创建视图失败 err: %v\n", err)
	}
	err = database.CreateMktViews(db)
	if err != nil {
		fmt.Printf("创建视图失败 err: %v\n", err)
	}
	err = database.CreateBlkViews(db)
	if err != nil {
		fmt.Printf("创建视图失败 err: %v\n", err)
	}
	return nil
}

func rebuildGpBaseFromFiles(db *sql.DB, gpFileDir string, files []string, updatedSet map[string]struct{}, download bool) error {
	if len(files) == 0 {
		fmt.Println("ℹ️ 未发现 stock 类型文件，跳过 GP 重建")
		return nil
	}

	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(files) {
		workerCount = len(files)
	}

	fmt.Printf("🚀 GP 全量重建: files=%d workers=%d\n", len(files), workerCount)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan string, workerCount*2)
	batches := make(chan database.GpWideBatch, workerCount*2)

	writerErrCh := make(chan error, 1)
	go func() {
		err := database.RebuildGpBase(ctx, db, batches)
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
	total := int64(len(files))

	//process file
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

					targetPath := filepath.Join(gpFileDir, v)
					if err := downloadFile(targetPath, v, GP_FILE_URL, download); err != nil {
						setWorkerErr(err)
						return
					}

					mkt, code, _ := parseFileName(v)
					recs, err := tdx.ParseGpDAT(targetPath, mkt, code)
					if err != nil {
						setWorkerErr(fmt.Errorf("failed to parse file %s: %w", targetPath, err))
						return
					}

					batch, err := database.AggregateGpRecords(recs)
					if err != nil {
						setWorkerErr(fmt.Errorf("failed to aggregate file %s: %w", targetPath, err))
						return
					}

					batches <- batch

					n := processed.Add(1)
					if n%200 == 0 || n == total {
						fmt.Printf("📈 GP 进度: %d/%d\n", n, total)
					}
				}
			}
		}()
	}

	//dispatch file
	go func() {
		defer close(jobs)
		for _, f := range files {
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

// https://data.tdx.com.cn/vipdoc/tdxgp.zip
func downloadFile(targetPath, fileName, urlbase string, download bool) error {
	if !download {
		return nil
	}

	url := urlbase + fileName
	cmd := exec.Command("wget", "-O", targetPath, url)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("⚠️ wget 下载 %s 失败: %w", url, err)
	}
	return nil
}
