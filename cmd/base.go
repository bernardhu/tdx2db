package cmd

import (
	"fmt"
	"path/filepath"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

func Base(dbPath, baseFileDir string) error {
	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}
	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	fmt.Printf("📦 开始处base目录: %s\n", baseFileDir)
	err = utils.CheckDirectory(baseFileDir)
	if err != nil {
		return err
	}
	/*
		targetPath := filepath.Join(baseFileDir, "base.zip")
		url := "https://www.tdx.com.cn/products/data/data/dbf/base.zip"
		cmd := exec.Command("wget", "-O", targetPath, url)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			fmt.Printf("⚠️ wget 下载 %s 失败: %v\n", url, err)
			return err
		}

		fmt.Printf("✅ 已下载 %s %s\n", url, targetPath)
		if err := utils.UnzipFile(targetPath, baseFileDir); err != nil {
			return fmt.Errorf("failed to unzip file %s: %w", targetPath, err)
		}
	*/
	dbfPath := filepath.Join(baseFileDir, "base.dbf")
	recs, err := tdx.ParseBaseDbf(dbfPath)
	if err != nil {
		return fmt.Errorf("failed to parse file %s: %w", dbfPath, err)
	}

	err = database.ImportBase(db, recs)
	if err != nil {
		return fmt.Errorf("failed to import base file %w", err)
	}
	fmt.Printf("✅ 已导入base数据%s\n", dbfPath)

	blkPath := filepath.Join(baseFileDir, "block.dat")
	tdx.ReadBlock(blkPath)
	fmt.Printf("✅ 已导入一般板块数据%s\n", blkPath)

	blkPath = filepath.Join(baseFileDir, "block_gn.dat")
	tdx.ReadBlock(blkPath)
	fmt.Printf("✅ 已导入概念板块数据%s\n", blkPath)

	blkPath = filepath.Join(baseFileDir, "block_fg.dat")
	tdx.ReadBlock(blkPath)
	fmt.Printf("✅ 已导入风格板块数据%s\n", blkPath)

	blkPath = filepath.Join(baseFileDir, "block_zs.dat")
	tdx.ReadBlock(blkPath)
	fmt.Printf("✅ 已导入指数板块数据%s\n", blkPath)
	return nil
}
