--查询连续3个季度营收增长
WITH q AS (
    SELECT
        code,
        rdate,
        adate,
        operating_revenue_q,
        LAG(operating_revenue_q, 1) OVER (
            PARTITION BY code
            ORDER BY rdate
        ) AS rev_1q_ago,  -- 前一季度
        LAG(operating_revenue_q, 2) OVER (
            PARTITION BY code
            ORDER BY rdate
        ) AS rev_2q_ago,  -- 前两季度
        LAG(operating_revenue_q, 3) OVER (
            PARTITION BY code
            ORDER BY rdate
        ) AS rev_3q_ago,  -- 前3季度
        ROW_NUMBER() OVER (
            PARTITION BY code
            ORDER BY rdate DESC
        ) AS rn            -- 标记最新一季
    FROM v_cw_stmt_core
),
rev_grow3 AS (
SELECT
    code,
    rdate,
    adate,
    operating_revenue_q,
    rev_1q_ago,
    rev_2q_ago,
    rev_3q_ago
FROM q
WHERE rn = 1                           -- 只看最新一季
  AND operating_revenue_q > 1.1*rev_1q_ago   -- 本季 > 上一季
  AND rev_1q_ago > 1.1*rev_2q_ago           -- 上一季 > 上上季
  AND rev_2q_ago > 1.1*rev_3q_ago         -- 上一季 > 上上季
),
latest_mkt AS (
    -- 从 v_gp_core_snapshot 里取每个 code 最近的一条市值快照
    SELECT
        code,
        mkt_value,
        ROW_NUMBER() OVER (
            PARTITION BY code, mkt
            ORDER BY rdate DESC
        ) AS rn
    FROM v_gp_core_snapshot
)

SELECT
    g.code,
    g.operating_revenue_q,
    g.rev_1q_ago,
    g.rev_2q_ago,
    g.rev_3q_ago,
    g.operating_revenue_q/g.rev_3q_ago as wide,
    s.mkt_value   -- 加上最新市值(万元)
FROM rev_grow3 g
LEFT JOIN latest_mkt s
    ON g.code = s.code
   AND s.rn   = 2
order by wide desc