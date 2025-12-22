package cmd

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

type XdxrIndex map[string][]model.XdxrData

func Cron(dbPath string, minline string) error {

	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}
	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	latestStockDate, err := database.GetStockTableLatestDate(db)
	if err != nil {
		return fmt.Errorf("failed to get latest date from database: %w", err)
	}
	fmt.Printf("📅 日线数据的最新日期为 %s\n", latestStockDate.Format("2006-01-02"))

	err = UpdateStocksDaily(db)
	if err != nil {
		return fmt.Errorf("failed to update daily stock data: %w", err)
	}

	err = UpdateStocksMinLine(db, minline)
	if err != nil {
		return fmt.Errorf("failed to update minute-line stock data: %w", err)
	}

	err = UpdateGbbq(db)
	if err != nil {
		return fmt.Errorf("failed to update GBBQ: %w", err)
	}

	err = UpdateFactors(db)
	if err != nil {
		return fmt.Errorf("failed to calculate factors: %w", err)
	}

	fmt.Printf("🔄 更新前复权数据视图 (%s)\n", database.QfqViewName)
	if err := database.CreateQfqView(db); err != nil {
		return fmt.Errorf("failed to create qfq view: %w", err)
	}

	fmt.Printf("🔄 更新后复权数据视图 (%s)\n", database.HfqViewName)
	if err := database.CreateHfqView(db); err != nil {
		return fmt.Errorf("failed to create hfq view: %w", err)
	}

	fmt.Println("🚀 今日任务执行成功")
	return nil
}

func UpdateStocksDaily(db *sql.DB) error {
	latestDate, err := database.GetStockLatestDate(db)
	if err != nil {
		return fmt.Errorf("failed to get stocks latest date from database: %w", err)
	}
	fmt.Printf("stocks最新日期为 %v\n", latestDate)

	fmt.Printf("🐢 开始导入日线数据 (drop + append)\n")
	if err := database.ImportStockDayFiles(db, VipdocDir2, ValidPrefixes, false, latestDate); err != nil {
		return fmt.Errorf("failed to import stock day files: %w", err)
	}
	fmt.Println("📊 日线数据导入成功")

	return nil
}

func UpdateStocksMinLine(db *sql.DB, minline string) error {
	if minline == "" {
		return nil
	}

	parts := strings.Split(minline, ",")
	for _, p := range parts {
		switch p {
		case "1":
			if err := database.Import1MinLineFiles(db, VipdocDir2, ValidPrefixes); err != nil {
				return fmt.Errorf("failed to import 1-minute line files: %w", err)
			}
			fmt.Println("📊 1分钟数据导入成功")

		case "5":
			if err := database.Import5MinLineFiles(db, VipdocDir2, ValidPrefixes); err != nil {
				return fmt.Errorf("failed to import 5-minute line files: %w", err)
			}
			fmt.Println("📊 5分钟数据导入成功")
		}
	}
	return nil
}

func UpdateGbbq(db *sql.DB) error {
	fmt.Println("🐢 开始下载股本变迁数据")

	gbbqFile, err := getGbbqFile(DataDir)
	if err != nil {
		return fmt.Errorf("failed to download GBBQ file: %w", err)
	}
	gbbqCSV := filepath.Join(DataDir, "gbbq.csv")
	if _, err := tdx.ConvertGbbqFile2Csv(gbbqFile, gbbqCSV); err != nil {
		return fmt.Errorf("failed to convert GBBQ to CSV: %w", err)
	}

	if err := database.ImportGbbqCsv(db, gbbqCSV); err != nil {
		return fmt.Errorf("failed to import GBBQ CSV into database: %w", err)
	}

	fmt.Printf("🔄 更新除权除息数据视图 (%s)\n", database.XdxrViewName)
	if err := database.CreateXdxrView(db); err != nil {
		return fmt.Errorf("failed to create xdxr view: %w", err)
	}

	fmt.Printf("🔄 更新市值换手数据视图 (%s)\n", database.TurnoverViewName)
	if err := database.CreateTurnoverView(db); err != nil {
		return fmt.Errorf("failed to create turnover view: %w", err)
	}

	fmt.Println("📈 股本变迁数据导入成功")
	return nil
}

func UpdateFactors(db *sql.DB) error {
	csvPath := filepath.Join(DataDir, "factors.csv")

	outFile, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file %s: %w", csvPath, err)
	}
	defer outFile.Close()

	fmt.Println("📟 计算所有股票前收盘价")
	// 构建 GBBQ 索引
	xdxrIndex, err := buildXdxrIndex(db)

	if err != nil {
		return fmt.Errorf("failed to build GBBQ index: %w", err)
	}

	symbols, err := database.QueryAllSymbols(db)
	if err != nil {
		return fmt.Errorf("failed to query all stock symbols: %w", err)
	}

	// 定义结果通道
	type result struct {
		rows string
		err  error
	}
	results := make(chan result, len(symbols))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)

	// 启动写入协程
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for res := range results {
			if res.err != nil {
				fmt.Printf("错误：%v\n", res.err)
				continue
			}
			if _, err := outFile.WriteString(res.rows); err != nil {
				fmt.Printf("写入 CSV 失败：%v\n", err)
			}
		}
	}()

	// 并发处理每个符号
	for _, symbol := range symbols {
		wg.Add(1)
		sem <- struct{}{}
		go func(sym string) {
			defer wg.Done()
			defer func() { <-sem }()
			stockData, err := database.QueryStockData(db, sym, nil, nil)
			if err != nil {
				results <- result{"", fmt.Errorf("failed to query stock data for symbol %s: %w", sym, err)}
				return
			}
			xdxrData := getXdxrByCode(xdxrIndex, sym)

			factors, err := tdx.CalculateFqFactor(stockData, xdxrData)
			if err != nil {
				results <- result{"", fmt.Errorf("failed to calculate factor for symbol %s: %w", sym, err)}
				return
			}
			// 将因子格式化为 CSV 行
			var sb strings.Builder
			for _, factor := range factors {
				row := fmt.Sprintf("%s,%s,%.4f,%.4f,%.4f,%.4f\n",
					factor.Symbol,
					factor.Date.Format("2006-01-02"),
					factor.Close,
					factor.PreClose,
					factor.QfqFactor,
					factor.HfqFactor,
				)
				sb.WriteString(row)
			}
			results <- result{sb.String(), nil}
		}(symbol)
	}

	// 等待所有处理n完成并关闭结果通道
	go func() {
		wg.Wait()
		close(results)
	}()

	// 等待写入协程完成
	writerWg.Wait()

	if err := database.ImportFactorCsv(db, csvPath); err != nil {
		return fmt.Errorf("failed to import factor data: %w", err)
	}
	fmt.Println("🔢 复权因子导入成功")

	return nil
}

func buildXdxrIndex(db *sql.DB) (XdxrIndex, error) {
	index := make(XdxrIndex)

	xdxrData, err := database.QueryAllXdxr(db)
	if err != nil {
		return nil, fmt.Errorf("failed to query xdxr data: %w", err)
	}

	for _, data := range xdxrData {
		code := data.Code
		index[code] = append(index[code], data)
	}

	return index, nil
}

func getXdxrByCode(index XdxrIndex, symbol string) []model.XdxrData {
	code := symbol[2:]
	if data, exists := index[code]; exists {
		return data
	}
	return []model.XdxrData{}
}

func getGbbqFile(cacheDir string) (string, error) {
	zipPath := filepath.Join(cacheDir, "gbbq.zip")
	gbbqURL := "http://www.tdx.com.cn/products/data/data/dbf/gbbq.zip"
	if _, err := utils.DownloadFile(gbbqURL, zipPath); err != nil {
		return "", fmt.Errorf("failed to download GBBQ zip file: %w", err)
	}

	unzipPath := filepath.Join(cacheDir, "gbbq-temp")
	if err := utils.UnzipFile(zipPath, unzipPath); err != nil {
		return "", fmt.Errorf("failed to unzip GBBQ file: %w", err)
	}

	return filepath.Join(unzipPath, "gbbq"), nil
}
