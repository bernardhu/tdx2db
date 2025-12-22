package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

func rmdir(path string) {
	cmd := exec.Command("rm", "-rf", path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fmt.Printf("⚠️ 删除目录%s失败\n", path)
	}
}

func Init(dbPath, dayFileDir string) error {
	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}

	rmdir(dayFileDir + "/bj")
	rmdir(dayFileDir + "/sh")
	rmdir(dayFileDir + "/sz")
	zipPath := filepath.Join(dayFileDir, "hsjday.zip")
	if err := downloadFile(zipPath, "hsjday.zip", CW_ALL_URL, true); err != nil {
		return err
	}

	if err := unzip(zipPath, dayFileDir); err != nil {
		return fmt.Errorf("failed to unzip file %s: %v.", zipPath, err)
	}

	rmdir(zipPath)

	fmt.Printf("📦 开始处理日线目录: %s\n", dayFileDir)
	err := utils.CheckDirectory(dayFileDir)
	if err != nil {
		return err
	}

	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	fmt.Println("🐢 开始导入日线数据 (drop + append)")
	if err := database.ImportStockDayFiles(db, dayFileDir, ValidPrefixes, true, nil); err != nil {
		return fmt.Errorf("failed to import stock day files: %w", err)
	}
	fmt.Println("🚀 股票数据导入成功")

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

	return nil
}
