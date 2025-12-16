-- =====================================
-- 1. 杠杆 & 质押 & 转融券（市场层面风险）
-- =====================================
CREATE OR REPLACE VIEW v_mkt_leverage_pledge AS
SELECT
    mkt,
    rdate,

    -- 融资融券（全市场汇总）
    f10  AS mrg_balance_all,          -- 沪深京融资余额(万元)
    f11  AS short_balance_all,        -- 沪深京融券余额(万元)
    f250 AS mrg_buy_amt_all,          -- 沪深京融资买入额(万元)
    f251 AS short_sell_qty_all,       -- 沪深京融券卖出量(万股)

    -- 质押率（分市场）
    f210 AS pledge_unrestr_sz,        -- 深市无限售质押率(%)
    f211 AS pledge_unrestr_sh,        -- 沪市无限售质押率(%)
    f220 AS pledge_restr_sz,          -- 深市有限售质押率(%)
    f221 AS pledge_restr_sh,          -- 沪市有限售质押率(%)
    f260 AS pledge_ratio_mkt_week,    -- 每周市场质押比例(%)

    -- 转融券
    f370 AS trsb_lent_value_mkt,      -- 转融券融出市值(亿元)
    f371 AS trsb_end_balance_mkt      -- 转融券期末余额(亿元)
FROM raw_gp_m k t;

-- ===========================
-- 2. 股指期货（指数对冲 / 情绪）
-- ===========================
CREATE OR REPLACE VIEW v_mkt_index_futures AS
SELECT
    mkt,
    rdate,
    f50 AS if50_net_pos,              -- 上证50股指期货净持仓(手)
    f60 AS ih300_net_pos,             -- 沪深300股指期货净持仓(手)
    f70 AS ic500_net_pos              -- 中证500股指期货净持仓(手)
FROM raw_gp_mkt;

-- ==========================================
-- 3. 北向 + ETF + 流动性 / 宏观投放
-- ==========================================
CREATE OR REPLACE VIEW v_mkt_north_etf_liquidity AS
SELECT
    mkt,
    rdate,

    -- 北向流入 & 净买入
    f20  AS sh_hk_inflow,             -- 沪股通流入金额(亿元)
    f21  AS sz_hk_inflow,             -- 深股通流入金额(亿元)
    f200 AS sh_hk_net_buy,            -- 沪股通净买入额(亿元)
    f201 AS sz_hk_net_buy,            -- 深股通净买入额(亿元)
    f400 AS north_total_turnover,     -- 陆股通成交总额(亿元)
    f401 AS north_total_trades,       -- 陆股通成交总笔(万笔)

    -- 宏观流动性
    f270 AS omo_net_injection,        -- 央行公开市场净投放(亿元)

    -- ETF 维度（份额 & 金额）
    f80  AS etf_size_units,           -- ETF基金规模(亿份)
    f81  AS etf_net_create_units,     -- ETF净申赎(亿份)
    f380 AS etf_size_value,           -- ETF基金规模(亿元)
    f381 AS etf_net_create_value      -- ETF净申赎(亿元)
FROM raw_gp_mkt;

-- =======================
-- 4. 龙虎榜资金结构视图
-- =======================
CREATE OR REPLACE VIEW v_mkt_lhb_all AS
SELECT
    mkt,
    rdate,

    -- 总体龙虎榜
    f160 AS lhb_total_buy,            -- 龙虎榜买入总金额(亿元)
    f161 AS lhb_total_sell,           -- 龙虎榜卖出总金额(亿元)

    -- 机构
    f170 AS lhb_inst_buy,             -- 龙虎榜机构买入金额(亿元)
    f171 AS lhb_inst_sell,            -- 龙虎榜机构卖出金额(亿元)

    -- 营业部
    f180 AS lhb_broker_buy,           -- 龙虎榜营业部买入金额(亿元)
    f181 AS lhb_broker_sell,          -- 龙虎榜营业部卖出金额(亿元)

    -- 沪深股通方向的龙虎榜
    f190 AS lhb_north_buy,            -- 龙虎榜沪深股通买入金额(亿元)
    f191 AS lhb_north_sell            -- 龙虎榜沪深股通卖出金额(亿元)
FROM raw_gp_mkt;

-- =========================================
-- 5. 涨跌停 / 连板 / 打板资金（情绪核心）
-- =========================================
CREATE OR REPLACE VIEW v_mkt_sentiment_boards AS
SELECT
    mkt,
    rdate,

    -- 全市场涨跌停（含ST口径）
    f30  AS up_limit_cnt,             -- 涨停股个数
    f31  AS ever_up_limit_cnt,        -- 曾涨停股个数
    f40  AS down_limit_cnt,           -- 跌停股个数
    f41  AS ever_down_limit_cnt,      -- 曾跌停股个数

    -- 连板 & 涨跌停（剔除 ST / 未开板）
    f230 AS cons_limit_cnt_with_st,   -- 连板股个数(含ST/未开板新股)
    f231 AS cons_limit_cnt_no_st,     -- 连板股个数(不含ST/未开板新股)
    f240 AS up_limit_cnt_no_st,       -- 涨停股个数(不含ST/未开板新股)
    f241 AS down_limit_cnt_no_st,     -- 跌停股个数(不含ST股)
    f360 AS ever_up_limit_cnt_ex_st,  -- 曾涨停股个数(剔除ST/未开板新股)
    f361 AS ever_down_limit_cnt_ex_st,-- 曾跌停股个数(剔除ST股)

    -- 市场高度 & 多板
    f300 AS mkt_height_no_st,         -- 市场高度(不含ST/未开板新股)
    f301 AS up_limit_ge2_cnt_no_st,   -- 2板以上涨停个数(不含ST/未开板新股)

    -- 打板资金 & 封单金额
    f150 AS hit_board_success_cap,    -- 打板资金封板成功资金(亿元)
    f151 AS hit_board_fail_cap,       -- 打板资金封板失败资金(亿元)
    f330 AS up_limit_order_cap,       -- 涨停封单金额(亿元)
    f331 AS down_limit_order_cap,     -- 跌停封单金额(亿元)

    -- 换手板 & 回封率
    f350 AS turnover_board_cnt,       -- 换手板家数
    f351 AS re_seal_rate,             -- 回封率(%)

    -- ≥5% 大涨大跌家数
    f390 AS up_ge5pct_cnt,            -- 涨幅≥5%家数
    f391 AS down_ge5pct_cnt           -- 跌幅≥5%家数
FROM raw_gp_mkt;

-- ==========================================
-- 6. 宽度 / 新高新低 / 成交量（Breadth & Momentum）
-- ==========================================
CREATE OR REPLACE VIEW v_mkt_breadth_momentum AS
SELECT
    mkt,
    rdate,

    -- 历史新高 / 新低
    f280 AS his_high_cnt,             -- 历史新高股票个数
    f281 AS his_low_cnt,              -- 历史新低股票个数

    -- 120 日新高 / 新低
    f290 AS high_120d_cnt,            -- 120天新高股票个数
    f291 AS low_120d_cnt,             -- 120天新低股票个数

    -- 20 日新高 / 新低
    f320 AS high_20d_cnt,             -- 20天新高股票个数
    f321 AS low_20d_cnt,              -- 20天新低股票个数

    -- 涨跌家数
    f310 AS adv_cnt,                  -- 涨家数(剔除停牌)
    f311 AS dcl_cnt,                  -- 跌家数(剔除停牌)

    -- 上涨/下跌股成交量
    f340 AS adv_volume,               -- 上涨股成交量(万手)
    f341 AS dcl_volume                -- 下跌股成交量(万手)
FROM raw_gp_mkt;

-- ==========================================
-- 7. 投资者结构 & 大宗 & 解禁 & 分红募资
-- ==========================================
CREATE OR REPLACE VIEW v_mkt_participants_corp AS
SELECT
    mkt,
    rdate,

    -- 新增账户
    f90  AS new_individual_inv,       -- 新增自然人数量(户)
    f91  AS new_non_indiv_inv,        -- 新增非自然人数量(户)

    -- 增减持（金额口径）
    f100 AS inc_amount_mkt,           -- 增持额(万元)
    f101 AS dec_amount_mkt,           -- 减持额(万元)

    -- 大宗溢价 / 折价
    f110 AS block_premium_amt,        -- 溢价大宗成交额(万元)
    f111 AS block_discount_amt,       -- 折价大宗成交额(万元)

    -- 解禁规模
    f120 AS unlock_plan_amt,          -- 限售解禁计划额(亿元)
    f121 AS unlock_actual_amt,        -- 限售解禁实际上市额(亿元)

    -- 分红 & 募资
    f130 AS total_dividend_amt,       -- 市场总分红额(亿元)
    f140 AS total_fundraising_amt     -- 市场总募资额(亿元)
FROM raw_g p_base;
