package database

import (
	"database/sql"
	"fmt"
	"strings"
)

var gpViews = []ColumnViews{
	{
		name: "v_gp_core_snapshot",
		from: "raw_gp_base",
		desc: "基础特征 / 流动性视图",
		fields: []ColumnView{
			{name: "code"},
			{name: "mkt"},
			{name: "rdate"},
			{name: "f10", alias: "holder_cnt", desc: "股东户数(户)"},
			{name: "f160", alias: "mkt_value", desc: "总市值(万元)"},
			{name: "f210", alias: "dividend_yield", desc: "股息率(%)"},
			{name: "f250", alias: "open_volume", desc: "开盘成交量(手)"},
			{name: "f251", alias: "after_close_volume", desc: "盘后固定成交量(手)"},
			{name: "f270", alias: "mkt_pop_rank", desc: "市场人气排名"},
			{name: "f271", alias: "industry_pop_rank", desc: "行业人气排名"},
		},
	},
	{
		name: "v_gp_leverage_risk",
		from: "raw_gp_base",
		desc: "融资融券 + 转融券 + 质押风险视图",
		fields: []ColumnView{
			{name: "code"},
			{name: "mkt"},
			{name: "rdate"},
			{name: "f30", alias: "margin_balance", desc: "融资余额(万元)"},
			{name: "f31", alias: "short_balance", desc: "融券余量(股)"},
			{name: "f110", alias: "margin_buy_amt", desc: "融资买入额(万元)"},
			{name: "f111", alias: "margin_repay_amt", desc: "融资偿还额(万元)"},
			{name: "f120", alias: "short_sell_qty", desc: "融券卖出量(股)"},
			{name: "f121", alias: "short_repay_qty", desc: "融券偿还量(股)"},
			{name: "f130", alias: "margin_net_buy", desc: "融资净买入(万元)"},
			{name: "f131", alias: "short_net_sell", desc: "融券净卖出(股)"},
			{name: "f310", alias: "trsb_begin_qty", desc: "转融券期初余量(股)"},
			{name: "f311", alias: "trsb_end_qty", desc: "转融券期末余量(股)"},
			{name: "f320", alias: "trsb_lent_qty", desc: "转融券融出数量(股)"},
			{name: "f321", alias: "trsb_lent_value", desc: "转融券融出市值(元)"},
			{name: "f190", alias: "pledge_unrestricted", desc: "每周无限售股份质押数(万)"},
			{name: "f191", alias: "pledge_restricted", desc: "每周有限售股份质押数(万)"},
			{name: "f200", alias: "pledge_ratio", desc: "每周股票质押比例(%)"},
		},
	},
	{
		name: "v_gp_flow_all",
		from: "raw_gp_base",
		desc: "资金博弈 / 龙虎榜 / 北向 / 大宗",
		fields: []ColumnView{
			{name: "code"},
			{name: "mkt"},
			{name: "rdate"},
			{name: "f60", alias: "north_hold", desc: "陆股通持股量(股)"},
			{name: "f70", alias: "north_net_buy", desc: "陆股通市场净买入(万元)"},
			{name: "f20", alias: "lhb_total_buy", desc: "龙虎榜买入总计(万元)"},
			{name: "f21", alias: "lhb_total_sell", desc: "龙虎榜卖出总计(万元)"},
			{name: "f80", alias: "lhb_inst_sell_num", desc: "龙虎榜机构卖方机构个数"},
			{name: "f81", alias: "lhb_inst_sell_amt", desc: "龙虎榜机构卖出金额(万元)"},
			{name: "f90", alias: "lhb_inst_buy_num", desc: "龙虎榜机构买方机构个数"},
			{name: "f91", alias: "lhb_inst_buy_amt", desc: "龙虎榜机构买入金额(万元)"},
			{name: "f170", alias: "lhb_broker_buy_amt", desc: "龙虎榜营业部买入金额(万元)"},
			{name: "f171", alias: "lhb_broker_sell_amt", desc: "龙虎榜营业部卖出金额(万元)"},
			{name: "f180", alias: "lhb_north_buy_amt", desc: "龙虎榜沪深股通买入金额(万元)"},
			{name: "f181", alias: "lhb_north_sell_amt", desc: "龙虎榜沪深股通卖出金额(万元)"},
			{name: "f370", alias: "lhb_cont_days", desc: "龙虎榜上榜类型连续交易日(天)"},
			{name: "f40", alias: "block_trade_avg_price", desc: "大宗交易成交均价(元)"},
			{name: "f41", alias: "block_trade_amount", desc: "大宗交易成交额(万元)"},
		},
	},
	{
		name: "v_gp_limit_events",
		from: "raw_gp_base",
		desc: "涨跌停 & 异动视图",
		fields: []ColumnView{
			{name: "code"},
			{name: "mkt"},
			{name: "rdate"},
			{name: "f140", alias: "up_limit_amount", desc: "涨停金额(万元)"},
			{name: "f141", alias: "up_limit_open_times", desc: "涨停开板次数"},
			{name: "f150", alias: "limit_status", desc: "涨跌停状态"},
			{name: "f151", alias: "limit_order_amount", desc: "封单金额(万元)"},
			{name: "f220", alias: "limit_flow_ratio", desc: "涨跌停封流比"},
			{name: "f221", alias: "limit_double_flow_ratio", desc: "涨跌停封封流比"},
			{name: "f240", alias: "first_up_limit_time", desc: "首次涨停时间"},
			{name: "f241", alias: "up_limit_max_order", desc: "涨停最大封单额(万)"},
			{name: "f360", alias: "auction_up_limit_buy", desc: "竞价涨停买入金额(万元)"},
			{name: "f330", alias: "down_limit_amount", desc: "跌停金额(万元)"},
			{name: "f331", alias: "down_limit_open_times", desc: "跌停开板次数"},
			{name: "f340", alias: "first_down_limit_time", desc: "跌停首次跌停时间"},
			{name: "f341", alias: "down_limit_max_order", desc: "跌停最大封单额(万)"},
		},
	},
	{
		name: "v_gp_corp_actions",
		from: "raw_gp_base",
		desc: "股东行为 / 回购 / 增减持 / 分红",
		fields: []ColumnView{
			{name: "code"},
			{name: "mkt"},
			{name: "rdate"},
			{name: "f50", alias: "share_chg_avg_price", desc: "增减持成交均价(元)"},
			{name: "f51", alias: "share_chg_qty", desc: "增减持变动股数(股)"},
			{name: "f350", alias: "share_increase_qty", desc: "增持数量(股)"},
			{name: "f351", alias: "share_decrease_qty", desc: "减持数量(股)"},
			{name: "f230", alias: "plan_increase_qty", desc: "拟增持数量(万股)"},
			{name: "f231", alias: "plan_decrease_qty", desc: "拟减持数量(万股)"},
			{name: "f260", alias: "plan_increase_amt", desc: "拟增持金额(万元)"},
			{name: "f261", alias: "plan_decrease_amt", desc: "拟减持金额(万元)"},
			{name: "f280", alias: "buyback_avg_price", desc: "股票回购均价(元)"},
			{name: "f281", alias: "buyback_qty", desc: "股票回购数量(万股)"},
			{name: "f300", alias: "dividend_amount", desc: "派息金额(万元)"},
			{name: "f301", alias: "bonus_share_qty", desc: "送转数量(股)"},
		},
	},
	{
		name: "v_gp_events_research",
		from: "raw_gp_base",
		desc: "事件 & 机构调研视图",
		fields: []ColumnView{
			{name: "code"},
			{name: "mkt"},
			{name: "rdate"},
			{name: "f290", alias: "is_resume_trade", desc: "是否复牌日"},
			{name: "f291", alias: "rename_flag", desc: "是否更名日"},
			{name: "f100", alias: "inst_research_cnt_3m", desc: "近3月机构调研次数"},
			{name: "f101", alias: "inst_num_3m", desc: "近3月调研机构数量"},
		},
	},
}

func CreateGpViews(db *sql.DB) error {
	for _, view := range gpViews {
		if err := createView(db, view); err != nil {
			return err
		}
	}
	return nil
}

func createView(db *sql.DB, view ColumnViews) error {
	columns := make([]string, 0, len(view.fields))
	for _, field := range view.fields {
		columns = append(columns, formatColumn(field))
	}

	query := fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS
		SELECT %s
		FROM %s;
	`, view.name, strings.Join(columns, ",\n\t\t"), view.from)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("failed to create view %s: %w", view.name, err)
	}
	return nil
}

func formatColumn(field ColumnView) string {
	expr := field.name
	if field.alias != "" {
		expr = fmt.Sprintf("%s AS %s", field.name, field.alias)
	}
	if field.desc != "" {
		clean := strings.ReplaceAll(field.desc, "*/", "")
		expr = fmt.Sprintf("%s /* %s */", expr, clean)
	}
	return expr
}
