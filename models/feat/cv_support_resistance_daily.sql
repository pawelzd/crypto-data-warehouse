
{% set lookback_bars = var('lookback_bars', 3) %}
{% set min_touches = var('min_touches', 2) %}
{% set score_pct = var('score_pct', 0.5) %}
{% set recency_half_life_days = var('recency_half_life_days', 180) %}
{% set atr_lookback_days = var('atr_lookback_days', 270) %}
{% set bin_pct = var('bin_pct', 0.0125) %}
{% set eps = var('eps', 1e-9) %}

-- === 0) Aggregate hourly → daily ===
WITH daily AS (
  SELECT
    token_address,
    DATE(TIMESTAMP(price_timestamp)) AS day,
    ARRAY_AGG(open  ORDER BY TIMESTAMP(price_timestamp) ASC  LIMIT 1)[OFFSET(0)] AS open,
    MAX(high) AS high,
    MIN(low)  AS low,
    ARRAY_AGG(close ORDER BY TIMESTAMP(price_timestamp) DESC LIMIT 1)[OFFSET(0)] AS close,
    SUM(volume) AS volume
  FROM {{ ref('token_ohlcv') }}
  GROUP BY token_address, day
),

-- === 1) ATR prep ===
ordered AS (
  SELECT token_address, TIMESTAMP(day) AS ts, open, high, low, close, volume
  FROM daily
),
with_prev AS (
  SELECT *, LAG(close) OVER (PARTITION BY token_address ORDER BY ts) AS prev_close
  FROM ordered
),
tr AS (
  SELECT
    *,
    GREATEST(
      high - low,
      ABS(high - IFNULL(prev_close, close)),
      ABS(low  - IFNULL(prev_close, close))
    ) AS true_range
  FROM with_prev
),
atr AS (
  SELECT
    *,
    AVG(true_range) OVER (
      PARTITION BY token_address ORDER BY ts
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS atr14
  FROM tr
),

-- Last close (for percentage bin size)
last_px AS (
  SELECT token_address,
         LAST_VALUE(close) OVER (PARTITION BY token_address ORDER BY ts
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
  FROM ordered
  QUALIFY ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY ts DESC)=1
),

-- Recent median ATR to avoid early hyper-volatility dominating
median_atr AS (
  SELECT DISTINCT
    a.token_address,
    PERCENTILE_CONT(a.atr14, 0.5) OVER (PARTITION BY a.token_address) AS med_atr
  FROM atr a
  WHERE DATE(a.ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ atr_lookback_days }} DAY)
),

-- === 2) Swing points (highs + lows) ===
swing_points AS (
  -- highs
  SELECT a.token_address, a.ts, a.high AS level, a.volume, a.atr14
  FROM atr a
  QUALIFY
    high = MAX(high) OVER (
      PARTITION BY token_address ORDER BY ts
      ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
    )
    AND (high - MIN(low) OVER (
           PARTITION BY token_address ORDER BY ts
           ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
         )) >= 0.5 * a.atr14

  UNION ALL

  -- lows
  SELECT a.token_address, a.ts, a.low AS level, a.volume, a.atr14
  FROM atr a
  QUALIFY
    low = MIN(low) OVER (
      PARTITION BY token_address ORDER BY ts
      ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
    )
    AND (MAX(high) OVER (
           PARTITION BY token_address ORDER BY ts
           ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
         ) - low) >= 0.5 * a.atr14
),

-- === 3) Adaptive binning (ATR-or-% of price, whichever is larger) ===
binned AS (
  SELECT
    s.*,
    m.med_atr,
    lp.last_close,
    -- ensure strictly positive bin_size
    GREATEST(0.75 * m.med_atr, {{ bin_pct }} * lp.last_close, {{ eps }}) AS bin_size,
    -- bin_center needs a numeric (not NULL), so guard with eps above
    ROUND(s.level / GREATEST(0.75 * m.med_atr, {{ bin_pct }} * lp.last_close, {{ eps }}))
      * GREATEST(0.75 * m.med_atr, {{ bin_pct }} * lp.last_close, {{ eps }}) AS bin_center,
    DATE_DIFF(CURRENT_DATE(), DATE(s.ts), DAY) AS age_days
  FROM swing_points s
  JOIN median_atr m USING (token_address)
  JOIN last_px   lp USING (token_address)
),

-- === 4) Score bins ===
scored AS (
  SELECT
    token_address,
    bin_center AS level,
    COUNT(*) AS touches,
    -- avoid divide-by-zero in the exponential by guarding half-life
    SUM(
      (1 + LOG10(GREATEST(volume,1)))
      * POWER(0.5, SAFE_DIVIDE(age_days, NULLIF({{ recency_half_life_days }}, 0)))
    ) AS score,
    MIN(ts) AS first_touch,
    MAX(ts) AS last_touch
  FROM binned
  GROUP BY token_address, level
  HAVING COUNT(*) >= {{ min_touches }}
)

-- === Quantiles per token (for dynamic percentile lookup) ===
, quant AS (
  -- granularity: 1000 → ~0.1% steps. Adjust if you want.
  SELECT
    token_address,
    APPROX_QUANTILES(score, 1000) AS q
  FROM scored
  GROUP BY token_address
)

-- === Dynamic percentile per token, based on dispersion & density ===
, token_stats AS (
  SELECT
    s.token_address,
    MIN(s.level) AS min_level,
    MAX(s.level) AS max_level,
    SAFE_DIVIDE(STDDEV_SAMP(s.score), NULLIF(AVG(s.score), 0)) AS cv_score,
    COUNT(*) AS n_bins
  FROM scored s
  GROUP BY s.token_address
),
bin_ref AS (
  -- reuse the same bin_size used when building bins; guard with eps
  SELECT DISTINCT
    m.token_address,
    GREATEST(0.75 * m.med_atr, {{ bin_pct }} * lp.last_close, {{ eps }}) AS bin_size
  FROM median_atr m
  JOIN (
    SELECT token_address,
           LAST_VALUE(close) OVER (PARTITION BY token_address ORDER BY ts
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
    FROM ordered
  ) lp USING (token_address)
),
dyn_cut AS (
  SELECT
    t.token_address,
    -- how many non-overlapping bands could fit across the range
    SAFE_DIVIDE(t.max_level - t.min_level, NULLIF(b.bin_size, 0)) AS range_bins,
    t.n_bins,
    t.cv_score,
    b.bin_size,
    -- dynamic percentile (tuneable)
    GREATEST(
      0.50,
      LEAST(
        0.80,
        0.60
        + 0.15 * LEAST(2.0, t.cv_score) -- dispersion → stricter
        + 0.10 * LEAST(
            2.0,
            SAFE_DIVIDE(
              t.n_bins,
              NULLIF(CAST(ROUND(SAFE_DIVIDE(t.max_level - t.min_level, NULLIF(b.bin_size, 0))) AS INT64), 1)
            )
          ) -- density → stricter
      )
    ) AS score_pct_dynamic
  FROM token_stats t
  JOIN bin_ref b USING (token_address)
)

-- === Compute the threshold value from quantiles array =========
, thresholds AS (
  SELECT
    d.token_address,
    -- convert percentile to array index (0..1000)
    CAST(ROUND(d.score_pct_dynamic * 1000) AS INT64) AS idx,
    q.q
  FROM dyn_cut d
  JOIN quant q USING (token_address)
)

-- === Final selection using dynamic threshold ==================
SELECT
  s.token_address,
  s.level,
  s.touches,
  s.score,
  s.first_touch,
  s.last_touch
FROM scored s
JOIN thresholds t USING (token_address)
WHERE s.score >= t.q[OFFSET(GREATEST(0, LEAST(1000, t.idx)))]
ORDER BY s.token_address, s.level

