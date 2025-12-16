-- ==========================================
-- 1. 板块估值 & 规模视图
--    估值、总市值、流通市值、自由流通市值、股息率
-- ==========================================
CREATE OR REPLACE VIEW v_blk_valuation_size AS
SELECT
    blk_code,
    rdate,

    -- 估值：整体法 & 算术平均
    f50  AS pe_ttm_overall,           -- 市盈率TTM(整体法)
    f51  AS pe_ttm_avg,               -- 市盈率TTM(算术平均)
    f60  AS pb_mrq_overall,           -- 市净率MRQ(整体法)
    f61  AS pb_mrq_avg,               -- 市净率MRQ(算术平均)
    f70  AS ps_ttm_overall,           -- 市销率TTM(整体法)
    f71  AS ps_ttm_avg,               -- 市销率TTM(算术平均)
    f80  AS pc_ttm_overall,           -- 市现率TTM(整体法)
    f81  AS pc_ttm_avg,               -- 市现率TTM(算术平均)

    -- 规模：总市值 / 流通市值 / 自由流通
    f100 AS mkt_cap_overall,          -- 板块总市值(亿元,整体法)
    f101 AS mkt_cap_avg,              -- 板块总市值(亿元,算术平均)
    f110 AS float_mkt_cap_overall,    -- 板块流通市值(亿元,整体法)
    f111 AS float_mkt_cap_avg,        -- 板块流通市值(亿元,算术平均)
    f190 AS free_float_cap_overall,   -- 板块自由流通市值(亿元,整体法)
    f191 AS free_float_cap_avg,       -- 板块自由流通市值(亿元,算术平均)

    -- 股息率
    f180 AS div_yield_avg,            -- 板块股息率(算术平均)
    f181 AS div_yield_overall         -- 板块股息率(整体法)
FROM raw_gp_blk;

-- ==========================================
-- 2. 板块情绪 & 宽度视图
--    涨跌家数、涨跌停、连板高度、开盘成交
-- ==========================================
CREATE OR REPLACE VIEW v_blk_breadth_sentiment AS
SELECT
    blk_code,
    rdate,

    -- 涨跌家数
    f90  AS adv_cnt,                  -- 上涨家数
    f91  AS dcl_cnt,                  -- 下跌家数

    -- 涨跌停家数
    f120 AS up_limit_cnt,             -- 涨停家数
    f121 AS ever_up_limit_cnt,        -- 曾涨停家数
    f130 AS down_limit_cnt,           -- 跌停家数
    f131 AS ever_down_limit_cnt,      -- 曾跌停家数

    -- 板块高度（连板强度）
    f140 AS blk_height_no_st,         -- 市场高度(不含ST/未开板新股)
    f141 AS blk_ge2_limit_cnt_no_st,  -- 2板及以上涨停个数(不含ST/未开板新股)

    -- 早盘资金
    f170 AS open_turnover_amt         -- 开盘成交金额(万元)
FROM raw_gp_blk;

-- ==========================================
-- 3. 板块杠杆 & 北向视图
--    融资融券 + 沪/深股通板块流入
-- ==========================================
CREATE OR REPLACE VIEW v_blk_leverage_north AS
SELECT
    blk_code,
    rdate,

    -- 杠杆
    f150 AS mrg_balance_blk,          -- 沪深京融资余额(万元)
    f151 AS short_balance_blk,        -- 沪深京融券余额(万元)

    -- 北向（板块维度）
    f160 AS sh_hk_inflow_blk,         -- 沪股通流入金额(亿元)
    f161 AS sz_hk_inflow_blk          -- 深股通流入金额(亿元)
FROM raw_gp_blk;
