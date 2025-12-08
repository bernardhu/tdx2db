import akshare as ak
import pandas as pd


def normalize_sz(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.rename(
        columns={
            "证券代码": "code",
            "证券简称": "name",
            "上市日期": "inlist",
            "终止上市日期": "delist",
        }
    )[["code", "name", "inlist", "delist"]].copy()
    normalized["mkt"] = "sz"
    return normalized


def normalize_sh(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.rename(
        columns={
            "公司代码": "code",
            "公司简称": "name",
            "上市日期": "inlist",
            "暂停上市日期": "delist",
        }
    )[["code", "name", "inlist", "delist"]].copy()
    normalized["mkt"] = "sh"
    return normalized


stock_info_sz_delist_df = normalize_sz(ak.stock_info_sz_delist(symbol="终止上市公司"))
print(stock_info_sz_delist_df)
stock_info_sh_delist_df = normalize_sh(ak.stock_info_sh_delist(symbol="全部"))
print(stock_info_sh_delist_df)

combined = pd.concat([stock_info_sz_delist_df, stock_info_sh_delist_df], ignore_index=True)
combined.to_csv("datatool/vipdoc/base/delist.csv", index=False, encoding="utf-8-sig")
