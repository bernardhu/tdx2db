package cmd

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

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

func filerHashes(hash map[string]string) {
	for k, _ := range hash {
		mkt, code, res := parseFileName(k)
		if res != "ashare" && res != "tdx" && res != "mkt" && res != "stock" {
			fmt.Printf("skip:%s mkt:%s code:%s res:%s\n", k, mkt, code, res)
			delete(hash, k)
		}
	}
}

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
	filerHashes(existingHashes)
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

	filerHashes(latestHashes)
	updatedFiles, olds, news := diffHashes(existingHashes, latestHashes)
	if len(updatedFiles) == 0 {
		fmt.Println("ℹ️ 没有新的股票文件需要更新")
		return nil
	}

	sort.Strings(updatedFiles)
	fmt.Printf("🌟 发现 %d 个新的股票文件: %v oldhash:%v newhash:%v\n", len(updatedFiles), updatedFiles, olds, news)

	for _, v := range updatedFiles {
		targetPath := filepath.Join(gpFileDir, v)
		/*
			url := fmt.Sprintf("https://data.tdx.com.cn/tdxgp/%s", v)
			cmd := exec.Command("wget", "-O", targetPath, url)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			if err := cmd.Run(); err != nil {
				fmt.Printf("⚠️ wget 下载 %s 失败: %v\n", url, err)
				continue
			}

			fmt.Printf("✅ 已下载 %s %s\n", url, targetPath)
		*/
		mkt, code, res := parseFileName(v)
		typ := res
		if res == "ashare" {
			typ = "stock"
		}

		recs, err := tdx.ParseGpDAT(targetPath, mkt, code)
		if err != nil {
			return fmt.Errorf("failed to parse file %s: %w", targetPath, err)
		}

		if typ == "stock" { //gp
			err = database.ImportGpdata(db, recs)
			if err != nil {
				return fmt.Errorf("failed to import gp file %w", err)
			}
		} else if typ == "tdx" { //block
			err = database.ImportBlkdata(db, recs)
			if err != nil {
				return fmt.Errorf("failed to import blk file %w", err)
			}
		} else if typ == "mkt" { //mkt
			err = database.ImportMktdata(db, recs)
			if err != nil {
				return fmt.Errorf("failed to import mkt file %w", err)
			}
		}
		//fmt.Printf("typ:%s data:%v\n", typ, recs)
		fmt.Printf("✅ 已解析股票数据%s\n", targetPath)

	}

	return nil
}
