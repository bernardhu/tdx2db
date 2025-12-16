-- ===========================
-- 1. 前复权日线视图
-- ===========================
CREATE OR REPLACE VIEW v_qfq_stocks AS
SELECT
    s.symbol,
    s.date,
    s.volume,
    s.amount,
    ROUND(s.open  * f.qfq_factor, 2) AS open,
    ROUND(s.high  * f.qfq_factor, 2) AS high,
    ROUND(s.low   * f.qfq_factor, 2) AS low,
    ROUND(s.close * f.qfq_factor, 2) AS close,
    t.turnover
FROM raw_stocks_daily s
JOIN raw_adjust_factor f ON s.symbol = f.symbol AND s.date = f.date
LEFT JOIN v_turnover t ON s.symbol = t.symbol AND s.date = t.date;

-- ===========================
-- 2. 后复权日线视图
-- ===========================
CREATE OR REPLACE VIEW v_hfq_stocks AS
SELECT
    s.symbol,
    s.date,
    s.volume,
    s.amount,
    ROUND(s.open  * f.hfq_factor, 2) AS open,
    ROUND(s.high  * f.hfq_factor, 2) AS high,
    ROUND(s.low   * f.hfq_factor, 2) AS low,
    ROUND(s.close * f.hfq_factor, 2) AS close,
    t.turnover
FROM raw_stocks_daily s
JOIN raw_adjust_factor f ON s.symbol = f.symbol AND s.date = f.date
LEFT JOIN v_turnover t ON s.symbol = t.symbol AND s.date = t.date;

