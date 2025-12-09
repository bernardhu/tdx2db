package cmd

import (
	"fmt"
	"os"
	"os/exec"
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
	//read bloclcfg
	cfgPath := filepath.Join(baseFileDir, "tdxzs3.cfg")
	crecs, err := tdx.ReadBlockCfg(cfgPath)
	if err != nil {
		return fmt.Errorf("failed to read block cfg file %s: %w", cfgPath, err)
	}

	err = database.ImportBlockCfgs(db, crecs)
	if err != nil {
		return fmt.Errorf("failed to import block cfg file %w", err)
	}

	refs := make(map[string]*tdx.BlockCfg)
	for k, v := range crecs {
		refs[v.Ref] = &crecs[k]
		if v.Code == "880638" {
			fmt.Printf("add ref:%s. to refs", v.Ref)
		}
	}

	//read base.dbf
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

	blockFilter := make(map[string]*tdx.BlockData)
	database.CheckBlocks(db)
	//-------------------hy data--------------------
	hyPath := filepath.Join(baseFileDir, "tdxhy.cfg")
	hrecs, err := tdx.ReadCodeHy(hyPath)
	if err == nil {
		err = database.ImportHyBlocks(db, hrecs, refs)
		if err == nil {
			fmt.Printf("✅ 已导入行业数据%s\n", hyPath)
		} else {
			fmt.Printf("❌ 导入行业数据%s 失败 %v\n", hyPath, err)
		}
	} else {
		fmt.Printf("❌ 读取行业数据%s 失败 %v\n", hyPath, err)
	}

	//-------------------block data--------------------
	blkPath := filepath.Join(baseFileDir, "block.dat")
	brecs, err := tdx.ReadBlock(blkPath)
	if err == nil {
		err = database.ImportBlocks(db, brecs, "normal", blockFilter, refs)
		if err == nil {
			fmt.Printf("✅ 已导入一般板块数据%s\n", blkPath)
		} else {
			fmt.Printf("❌ 导入一般板块数据%s 失败 %v\n", blkPath, err)
		}
	} else {
		fmt.Printf("❌ 读取一般板块数据%s 失败 %v\n", blkPath, err)
	}

	blkPath = filepath.Join(baseFileDir, "block_gn.dat")
	brecs, err = tdx.ReadBlock(blkPath)
	if err == nil {
		err = database.ImportBlocks(db, brecs, "concept", blockFilter, refs)
		if err == nil {
			fmt.Printf("✅ 已导入概念板块数据%s\n", blkPath)
		} else {
			fmt.Printf("❌ 导入概念板块数据%s 失败 %v\n", blkPath, err)
		}
	} else {
		fmt.Printf("❌ 读取概念板块数据%s 失败 %v\n", blkPath, err)
	}

	blkPath = filepath.Join(baseFileDir, "block_fg.dat")
	brecs, err = tdx.ReadBlock(blkPath)
	if err == nil {
		err = database.ImportBlocks(db, brecs, "style", blockFilter, refs)
		if err == nil {
			fmt.Printf("✅ 已导入风格板块数据%s\n", blkPath)
		} else {
			fmt.Printf("❌ 导入风格板块数据%s 失败 %v\n", blkPath, err)
		}
	} else {
		fmt.Printf("❌ 读取风格板块数据%s 失败 %v\n", blkPath, err)
	}

	blkPath = filepath.Join(baseFileDir, "block_zs.dat")
	brecs, err = tdx.ReadBlock(blkPath)
	if err == nil {
		err = database.ImportBlocks(db, brecs, "index", blockFilter, refs)
		if err == nil {
			fmt.Printf("✅ 已导入指数板块数据%s\n", blkPath)
		} else {
			fmt.Printf("❌ 导入指数板块数据%s 失败 %v\n", blkPath, err)
		}
	} else {
		fmt.Printf("❌ 读取指数板块数据%s 失败 %v\n", blkPath, err)
	}

	//-------------------delist data--------------------
	/*
		SELECT EXTRACT(YEAR FROM delist) AS y, COUNT(*) AS cnt FROM raw_delist GROUP BY y ORDER BY y;
	*/
	cmd := exec.Command("python", "delist.py")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("⚠️ python 获取沪深退市名单失败: %v\n", err)
		return err
	}

	delistPath := filepath.Join(baseFileDir, "delist.csv")
	err = database.ImportDelist(db, delistPath)
	if err == nil {
		fmt.Printf("✅ 已导入退市数据%s\n", delistPath)
	} else {
		fmt.Printf("❌ 导入退市数据%s 失败 %v\n", delistPath, err)
	}

	return nil
}
