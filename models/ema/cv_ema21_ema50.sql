WITH base AS (
  SELECT
    c.token_address,
    c.monitoring_session_id,
    c.session_start,
    c.session_end,
    c.extended_start,
    c.extended_end,
    c.in_pre_extension,
    c.in_core_monitoring,
    c.in_post_extension,
    SAFE_CAST(c.volume AS FLOAT64) AS volume,
    TIMESTAMP_TRUNC(c.price_timestamp, HOUR) AS ts_hour,
    AVG(SAFE_CAST(c.price_usd AS FLOAT64)) AS price,
    SAFE_CAST(t.circSupply AS FLOAT64) AS total_supply
  FROM {{ ref('cv_filter_prep_72_ext_windows') }} c
  LEFT JOIN {{ source('core', 'token_metadata_jup_tmp') }} t
    ON c.token_address = t.id
    AND t.organicScoreLabel <> 'low'
    AND t.mcap >= 100000000
  GROUP BY
    c.token_address, c.monitoring_session_id, c.session_start, c.session_end,
    c.extended_start, c.extended_end, c.in_pre_extension, c.in_core_monitoring, c.in_post_extension,
    ts_hour, t.circSupply, c.volume
),
indexed AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.token_address, b.monitoring_session_id
      ORDER BY b.ts_hour
    ) AS rn_current
  FROM base b
),

-- Compute curr_rn (the current row's index) ONCE with a window function.
-- We **won't** use any window functions inside other window functions after this.
with_curr AS (
  SELECT
    i.*,
    MAX(rn_current) OVER (
      PARTITION BY i.token_address, i.monitoring_session_id
      ORDER BY i.ts_hour
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS curr_rn
  FROM indexed i
),

ema AS (
  SELECT
    c.*,
    -- smoothing factors s = 1 - alpha, where alpha = 2/(N+1)
    -- N=21 => s21, N=50 => s50
    (
      SUM(c.price * POW(1 - 2.0/22.0, c.curr_rn - c.rn_current)) OVER (
        PARTITION BY c.token_address, c.monitoring_session_id
        ORDER BY c.ts_hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
      /
      SUM(POW(1 - 2.0/22.0, c.curr_rn - c.rn_current)) OVER (
        PARTITION BY c.token_address, c.monitoring_session_id
        ORDER BY c.ts_hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    ) AS ema_21,
    (
      SUM(c.price * POW(1 - 2.0/51.0, c.curr_rn - c.rn_current)) OVER (
        PARTITION BY c.token_address, c.monitoring_session_id
        ORDER BY c.ts_hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
      /
      SUM(POW(1 - 2.0/51.0, c.curr_rn - c.rn_current)) OVER (
        PARTITION BY c.token_address, c.monitoring_session_id
        ORDER BY c.ts_hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    ) AS ema_50
  FROM with_curr c
),

diffs AS (
  SELECT
    e.*,
    (ema_21 - ema_50) AS delta,
    LAG(ema_21 - ema_50) OVER (
      PARTITION BY token_address, monitoring_session_id
      ORDER BY ts_hour
    ) AS prev_delta
  FROM ema e
)

SELECT
  d.*,
  -- CROSS UP: 21 crosses above 50
  CASE
    WHEN prev_delta IS NOT NULL AND prev_delta < 0 AND delta >= 0 THEN 1 ELSE 0
  END AS label_entry,
  -- CROSS DOWN: 21 crosses below 50
  CASE
    WHEN prev_delta IS NOT NULL AND prev_delta > 0 AND delta <= 0 THEN 1 ELSE 0
  END AS label_close
FROM diffs d
ORDER BY token_address, monitoring_session_id, ts_hour
