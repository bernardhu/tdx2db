package database

import "database/sql"

var blkViews = []ColumnViews{
	{
		name: "v_blk_valuation_size",
		from: "raw_gp_blk",
		desc: "板块估值与规模视图",
		fields: []ColumnView{
			{name: "code"},
			{name: "rdate"},
			{name: "f50", alias: "pe_ttm_overall", desc: "市盈率TTM(整体法)"},
			{name: "f51", alias: "pe_ttm_avg", desc: "市盈率TTM(算术平均)"},
			{name: "f60", alias: "pb_mrq_overall", desc: "市净率MRQ(整体法)"},
			{name: "f61", alias: "pb_mrq_avg", desc: "市净率MRQ(算术平均)"},
			{name: "f70", alias: "ps_ttm_overall", desc: "市销率TTM(整体法)"},
			{name: "f71", alias: "ps_ttm_avg", desc: "市销率TTM(算术平均)"},
			{name: "f80", alias: "pc_ttm_overall", desc: "市现率TTM(整体法)"},
			{name: "f81", alias: "pc_ttm_avg", desc: "市现率TTM(算术平均)"},
			{name: "f100", alias: "mkt_cap_overall", desc: "板块总市值(亿元,整体法)"},
			{name: "f101", alias: "mkt_cap_avg", desc: "板块总市值(亿元,算术平均)"},
			{name: "f110", alias: "float_mkt_cap_overall", desc: "板块流通市值(亿元,整体法)"},
			{name: "f111", alias: "float_mkt_cap_avg", desc: "板块流通市值(亿元,算术平均)"},
			{name: "f190", alias: "free_float_cap_overall", desc: "板块自由流通市值(亿元,整体法)"},
			{name: "f191", alias: "free_float_cap_avg", desc: "板块自由流通市值(亿元,算术平均)"},
			{name: "f180", alias: "div_yield_avg", desc: "板块股息率(算术平均)"},
			{name: "f181", alias: "div_yield_overall", desc: "板块股息率(整体法)"},
		},
	},
	{
		name: "v_blk_breadth_sentiment",
		from: "raw_gp_blk",
		desc: "板块情绪与宽度视图",
		fields: []ColumnView{
			{name: "code"},
			{name: "rdate"},
			{name: "f90", alias: "adv_cnt", desc: "上涨家数"},
			{name: "f91", alias: "dcl_cnt", desc: "下跌家数"},
			{name: "f120", alias: "up_limit_cnt", desc: "涨停家数"},
			{name: "f121", alias: "ever_up_limit_cnt", desc: "曾涨停家数"},
			{name: "f130", alias: "down_limit_cnt", desc: "跌停家数"},
			{name: "f131", alias: "ever_down_limit_cnt", desc: "曾跌停家数"},
			{name: "f140", alias: "blk_height_no_st", desc: "市场高度(不含ST/未开板新股)"},
			{name: "f141", alias: "blk_ge2_limit_cnt_no_st", desc: "2板及以上涨停个数(不含ST/未开板新股)"},
			{name: "f170", alias: "open_turnover_amt", desc: "开盘成交金额(万元)"},
		},
	},
	{
		name: "v_blk_leverage_north",
		from: "raw_gp_blk",
		desc: "板块杠杆与北向资金视图",
		fields: []ColumnView{
			{name: "code"},
			{name: "rdate"},
			{name: "f150", alias: "mrg_balance_blk", desc: "沪深京融资余额(万元)"},
			{name: "f151", alias: "short_balance_blk", desc: "沪深京融券余额(万元)"},
			{name: "f160", alias: "sh_hk_inflow_blk", desc: "沪股通流入金额(亿元)"},
			{name: "f161", alias: "sz_hk_inflow_blk", desc: "深股通流入金额(亿元)"},
		},
	},
}

func CreateBlkViews(db *sql.DB) error {
	for _, view := range blkViews {
		if err := createView(db, view); err != nil {
			return err
		}
	}
	return nil
}
