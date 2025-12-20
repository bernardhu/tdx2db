package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

func parseCode(mkt, code string) string {
	icode, err := strconv.Atoi(code)
	if err != nil {
		return "unknown"
	}

	switch mkt {
	case "sh": //https://www.sse.com.cn/lawandrules/guide/stock/jyglywznylc/zn/c/c_20251020_10795329.shtml
		first := icode / 100000
		sec := icode / 1000
		if first == 9 {
			if sec == 999 {
				return "mkt"
			}
			return "bshare"
		} else if first == 6 {
			return "ashare"
		} else if first == 5 {
			if sec == 508 {
				return "reits"
			} else if sec == 506 {
				return "lof"
			} else if sec == 511 || sec == 517 || sec == 520 || sec == 551 || sec == 588 || sec == 589 {
				return "etf"
			} else {
				return "fund"
			}
		} else if first == 0 {
			if sec == 0 {
				return "index"
			} else {
				return "bond"
			}
		} else {
			if sec == 100 || sec == 110 || sec == 111 || sec == 113 || sec == 118 || sec == 126 || sec == 181 || sec == 190 || sec == 191 || sec == 193 || sec == 195 {
				return "kzz"
			}
			if sec == 880 || sec == 881 {
				return "tdx"
			}

			return "bond"
		}
	case "sz": //http://www.szse.cn/marketServices/technicalservice/doc/
		sec := icode / 1000
		if sec <= 4 || (sec >= 300 && sec <= 309) {
			return "ashare"
		} else if sec == 123 || sec == 127 || sec == 128 {
			return "kzz"
		} else if sec == 150 || sec == 151 || sec == 184 {
			return "fund"
		} else if sec == 158 || sec == 159 {
			return "etf"
		} else if sec >= 160 && sec <= 179 {
			return "lof"
		} else if sec == 180 {
			return "reits"
		} else if sec >= 200 && sec <= 209 {
			return "bshare"
		} else if sec >= 970 {
			return "index"
		} else {
			return "bond"
		}
	case "bj": //https://www.bseinfo.net/jygl_list/200021626.html
		base := icode / 10000
		sec := icode / 1000
		if sec == 810 {
			return "kzz"
		} else if sec == 899 {
			return "index"
		} else if sec == 840 || sec == 841 {
			return "yysg" //要约收购、要约回购
		} else if sec == 850 {
			return "option" //股权激励期权
		} else if sec == 400 || sec == 420 {
			return "stock" //两网公司及退市公司股票
		} else if sec == 820 {
			return "yxg" //优先股票证券代码
		}

		//43 83 87 退出
		// 92 stock
		// 82 bond
		// 89 index 899050 北证50 /899601 北证专精特新

		if base == 92 || base == 88 {
			return "stock"
		}

		return "unknown"
	default:
		return "unknown"
	}
}

func parseFileName(n string) (string, string, string) {
	mkt := ""
	if strings.HasPrefix(n, "gpsz") {
		mkt = "sz"
	} else if strings.HasPrefix(n, "gpbj") {
		mkt = "bj"
	} else if strings.HasPrefix(n, "gpsh") {
		mkt = "sh"
	}

	code := strings.TrimSuffix(n, ".dat")
	code = code[4:]

	res := parseCode(mkt, code)

	return mkt, code, res
}

func filterHashes(hash map[string]string) {
	for k, _ := range hash {
		mkt, code, res := parseFileName(k)
		if res != "ashare" && res != "tdx" && res != "mkt" && res != "stock" {
			fmt.Printf("skip:%s mkt:%s code:%s res:%s\n", k, mkt, code, res)
			delete(hash, k)
		}
	}
}

var GP_FILE_URL = "https://data.tdx.com.cn/tdxgp/"
var GP_ALL_URL = "https://data.tdx.com.cn/vipdoc/"

func Gp(dbPath, gpFileDir string, download bool) error {
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

	fmt.Printf("🌟 发现 %d 个股票文件变更 oldhash:%v newhash:%v\n", len(updatedFiles), olds, news)

	if len(updatedFiles) > 2000 && download { //全部下载算了
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

		if err := unzip(zipPath, gpFileDir); err != nil {
			return fmt.Errorf("failed to unzip file %s: %w", zipPath, err)
		}
		download = false //后续不用下载了
	}

	if download {
		for _, v := range updatedFiles {
			targetPath := filepath.Join(gpFileDir, v)
			if err := downloadFile(targetPath, v, GP_FILE_URL, download); err != nil {
				return err
			}
		}
	}

	//全量rebuild
	allFiles := make([]string, 0, len(latestHashes))
	for f := range latestHashes {
		if strings.HasSuffix(f, ".dat") {
			allFiles = append(allFiles, f)
		}
	}

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

	if err := rebuildGpTablesFromFiles(db, gpFileDir, stockFiles, blkFiles, mktFiles); err != nil {
		return err
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

func rebuildGpTablesFromFiles(db *sql.DB, gpFileDir string, stockFiles, blkFiles, mktFiles []string) error {
	files := make([]string, 0, len(stockFiles)+len(blkFiles)+len(mktFiles))
	files = append(files, stockFiles...)
	files = append(files, blkFiles...)
	files = append(files, mktFiles...)
	if len(files) == 0 {
		fmt.Println("ℹ️ 未发现 GP 文件，跳过重建")
		return nil
	}

	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(files) {
		workerCount = len(files)
	}

	rebuildBase := len(stockFiles) > 0
	rebuildBlk := len(blkFiles) > 0
	rebuildMkt := len(mktFiles) > 0

	fmt.Printf("🚀 GP 重建: stock=%d blk=%d mkt=%d workers=%d\n", len(stockFiles), len(blkFiles), len(mktFiles), workerCount)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan string, workerCount*2)
	batches := make(chan database.GpRebuildBatch, workerCount*2)

	writerErrCh := make(chan error, 1)
	go func() {
		err := database.RebuildGpTables(ctx, db, rebuildBase, rebuildBlk, rebuildMkt, batches)
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
					mkt, code, res := parseFileName(v)
					recs, err := tdx.ParseGpDAT(targetPath, mkt, code)
					if err != nil {
						setWorkerErr(fmt.Errorf("failed to parse file %s: %w", targetPath, err))
						return
					}

					typ := res
					if res == "ashare" {
						typ = "stock"
					}

					var items []database.GpWideBatch
					var kind database.GpRebuildKind

					switch typ {
					case "stock":
						kind = database.GpRebuildBase
					case "tdx":
						kind = database.GpRebuildBlk
					case "mkt":
						kind = database.GpRebuildMkt
					default:
						continue
					}

					items, err = database.AggregateGpBatches(recs, kind)
					if err != nil {
						setWorkerErr(fmt.Errorf("failed to aggregate file %s: %w", targetPath, err))
						return
					}

					for _, batch := range items {
						if len(batch.Rows) == 0 {
							continue
						}
						select {
						case batches <- database.GpRebuildBatch{Kind: kind, Batch: batch}:
						case <-ctx.Done():
							return
						}
					}

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

func unzip(zipPath, destDir string) error {
	cmd := exec.Command("unzip", "-o", zipPath, "-d", destDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return utils.UnzipFile(zipPath, destDir)
		}
		return err
	}
	return nil
}
