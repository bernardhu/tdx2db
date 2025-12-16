-- ===========================
-- 1. 基础特征 / 流动性视图
-- ===========================
CREATE OR REPLACE VIEW v_gp_core_snapshot AS
SELECT
    code,
    mkt,
    rdate,
    f10  AS holder_cnt,              -- 股东户数(户)
    f160 AS mkt_value,               -- 总市值(万元)
    f210 AS dividend_yield,          -- 股息率(%)
    f250 AS open_volume,             -- 开盘成交量(手)
    f251 AS after_close_volume,      -- 盘后固定成交量(手)
    f270 AS mkt_pop_rank,            -- 市场人气排名
    f271 AS industry_pop_rank        -- 行业人气排名
FROM raw_gp_base;

-- =======================================
-- 2. 融资融券 + 转融券 + 质押风险视图
-- =======================================
CREATE OR REPLACE VIEW v_gp_leverage_risk AS
SELECT
    code,
    mkt,
    rdate,

    -- 融资融券
    f30  AS margin_balance,          -- 融资余额(万元)
    f31  AS short_balance,           -- 融券余量(股)
    f110 AS margin_buy_amt,          -- 融资买入额(万元)
    f111 AS margin_repay_amt,        -- 融资偿还额(万元)
    f120 AS short_sell_qty,          -- 融券卖出量(股)
    f121 AS short_repay_qty,         -- 融券偿还量(股)
    f130 AS margin_net_buy,          -- 融资净买入(万元)
    f131 AS short_net_sell,          -- 融券净卖出(股)

    -- 转融券
    f310 AS trsb_begin_qty,          -- 转融券期初余量(股)
    f311 AS trsb_end_qty,            -- 转融券期末余量(股)
    f320 AS trsb_lent_qty,           -- 转融券融出数量(股)
    f321 AS trsb_lent_value,         -- 转融券融出市值(元)

    -- 质押
    f190 AS pledge_unrestricted,     -- 每周无限售股份质押数(万)
    f191 AS pledge_restricted,       -- 每周有限售股份质押数(万)
    f200 AS pledge_ratio             -- 每周股票质押比例(%)
FROM raw_gp_base;

-- ====================================
-- 3. 资金博弈 / 龙虎榜 / 北向 / 大宗
-- ====================================
CREATE OR REPLACE VIEW v_gp_flow_all AS
SELECT
    code,
    mkt,
    rdate,

    -- 北向
    f60 AS north_hold,               -- 陆股通持股量(股)
    f70 AS north_net_buy,            -- 陆股通市场净买入(万元)

    -- 龙虎榜总体
    f20 AS lhb_total_buy,            -- 龙虎榜买入总计(万元)
    f21 AS lhb_total_sell,           -- 龙虎榜卖出总计(万元)

    -- 龙虎榜机构
    f80 AS lhb_inst_sell_num,        -- 龙虎榜机构卖方机构个数
    f81 AS lhb_inst_sell_amt,        -- 龙虎榜机构卖出金额(万元)
    f90 AS lhb_inst_buy_num,         -- 龙虎榜机构买方机构个数
    f91 AS lhb_inst_buy_amt,         -- 龙虎榜机构买入金额(万元)

    -- 龙虎榜营业部 & 沪深股通
    f170 AS lhb_broker_buy_amt,      -- 龙虎榜营业部买入金额(万元)
    f171 AS lhb_broker_sell_amt,     -- 龙虎榜营业部卖出金额(万元)
    f180 AS lhb_north_buy_amt,       -- 龙虎榜沪深股通买入金额(万元)
    f181 AS lhb_north_sell_amt,      -- 龙虎榜沪深股通卖出金额(万元)

    -- 上榜连续天数
    f370 AS lhb_cont_days,           -- 龙虎榜上榜类型连续交易日(天)

    -- 大宗交易
    f40 AS block_trade_avg_price,    -- 大宗交易成交均价(元)
    f41 AS block_trade_amount        -- 大宗交易成交额(万元)
FROM raw_gp_base;

-- ===========================
-- 4. 涨跌停 & 异动视图
-- ===========================
CREATE OR REPLACE VIEW v_gp_limit_events AS
SELECT
    code,
    mkt,
    rdate,

    -- 涨停侧
    f140 AS up_limit_amount,         -- 涨停金额(万元)
    f141 AS up_limit_open_times,     -- 涨停开板次数
    f150 AS limit_status,            -- 涨跌停状态
    f151 AS limit_order_amount,      -- 封单金额(万元)
    f220 AS limit_flow_ratio,        -- 涨跌停封流比
    f221 AS limit_double_flow_ratio, -- 涨跌停封封流比
    f240 AS first_up_limit_time,     -- 首次涨停时间
    f241 AS up_limit_max_order,      -- 涨停最大封单额(万)
    f360 AS auction_up_limit_buy,    -- 竞价涨停买入金额(万元)

    -- 跌停侧
    f330 AS down_limit_amount,       -- 跌停金额(万元)
    f331 AS down_limit_open_times,   -- 跌停开板次数
    f340 AS first_down_limit_time,   -- 跌停首次跌停时间
    f341 AS down_limit_max_order     -- 跌停最大封单额(万)
FROM raw_gp_base;

-- ======================================
-- 5. 股东行为 / 回购 / 增减持 / 分红
-- ======================================
CREATE OR REPLACE VIEW v_gp_corp_actions AS
SELECT
    code,
    mkt,
    rdate,

    -- 实际增减持
    f50  AS share_chg_avg_price,     -- 增减持成交均价(元)
    f51  AS share_chg_qty,           -- 增减持变动股数(股)
    f350 AS share_increase_qty,      -- 增持数量(股)
    f351 AS share_decrease_qty,      -- 减持数量(股)

    -- 计划增减持
    f230 AS plan_increase_qty,       -- 拟增持数量(万股)
    f231 AS plan_decrease_qty,       -- 拟减持数量(万股)
    f260 AS plan_increase_amt,       -- 拟增持金额(万元)
    f261 AS plan_decrease_amt,       -- 拟减持金额(万元)

    -- 回购
    f280 AS buyback_avg_price,       -- 股票回购均价(元)
    f281 AS buyback_qty,             -- 股票回购数量(万股)

    -- 分红送转
    f300 AS dividend_amount,         -- 派息金额(万元)
    f301 AS bonus_share_qty          -- 送转数量(股)
FROM raw_gp_base;

-- ===========================
-- 6. 事件 & 机构调研视图
-- ===========================
CREATE OR REPLACE VIEW v_gp_events_research AS
SELECT
    code,
    mkt,
    rdate,

    -- 事件标记
    f290 AS is_resume_trade,         -- 是否复牌日(0/停牌n天后复牌)
    f291 AS rename_flag,             -- 是否更名日(0/1/2/3/4/5)

    -- 调研情况
    f100 AS inst_research_cnt_3m,    -- 近3月机构调研次数
    f101 AS inst_num_3m              -- 近3月调研机构数量
FROM raw_gp_base;