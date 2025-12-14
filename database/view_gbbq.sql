-- ===========================
-- 1. 除权除息 / 分红送配视图
-- ===========================
CREATE OR REPLACE VIEW v_xdxr AS
SELECT
    date,
    code,
    c1 AS fenhong,
    c2 AS peigujia,
    c3 AS songzhuangu,
    c4 AS peigu
FROM raw_gbbq
WHERE category = 1;

-- ======================
-- 2. 换手率 / 市值视图
-- ======================
CREATE OR REPLACE VIEW v_turnover AS
WITH base_cc AS (
    SELECT
        date,
        code,
        c3 AS float_shares,
        c4 AS total_shares
    FROM raw_gbbq
    WHERE category IN (2, 3, 5, 7, 8, 9, 10)
),
expanded AS (
    SELECT
        d.date,
        d.symbol,
        LAST_VALUE(base_cc.float_shares IGNORE NULLS)
            OVER (PARTITION BY d.symbol ORDER BY d.date) AS float_shares,
        LAST_VALUE(base_cc.total_shares IGNORE NULLS)
            OVER (PARTITION BY d.symbol ORDER BY d.date) AS total_shares
    FROM raw_stocks_daily d
    LEFT JOIN base_cc
        ON base_cc.code = SUBSTR(d.symbol, 3)
        AND base_cc.date = d.date
)
SELECT
    r.date,
    r.symbol,
    ROUND(r.volume / (e.float_shares * 10000), 4) AS turnover,
    ROUND(e.float_shares * 10000 * r.close, 4) AS circ_mv,
    ROUND(e.total_shares * 10000 * r.close, 4) AS total_mv
FROM raw_stocks_daily r
JOIN expanded e
    ON r.symbol = e.symbol
    AND r.date = e.date;

