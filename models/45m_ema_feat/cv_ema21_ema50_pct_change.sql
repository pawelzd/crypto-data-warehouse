-- Replace the table below with your source (or keep your CTE chain and start from "window_stats")
DECLARE bin_width_pct FLOAT64 DEFAULT 5.0;   -- bucket width in percentage points
DECLARE min_pct FLOAT64 DEFAULT -100.0;      -- lower bound (%)
DECLARE max_pct FLOAT64 DEFAULT  200.0;      -- upper bound (%)

WITH base AS (
  SELECT
    SAFE_MULTIPLY(realized_return, 100.0) AS pct_gain
  FROM `crypto-trading-474111.45m_ema_feat.cv_ema21_ema50_max_gain`
  WHERE realized_return IS NOT NULL
),
-- Optionally clamp extreme outliers so the histogram looks readable
clamped AS (
  SELECT
    GREATEST(LEAST(pct_gain, max_pct), min_pct) AS pct_gain
  FROM base
),
bucketed AS (
  SELECT
    -- left edge of bucket
    bin_width_pct * FLOOR(pct_gain / bin_width_pct)           AS bin_left,
    -- right edge (exclusive)
    bin_width_pct * FLOOR(pct_gain / bin_width_pct) + bin_width_pct AS bin_right
  FROM clamped
)
SELECT
  bin_left,
  bin_right,
  CONCAT(FORMAT('%g', bin_left), ' to ', FORMAT('%g', bin_right), '%') AS bin_label,
  COUNT(*) AS n
FROM bucketed
GROUP BY bin_left, bin_right
ORDER BY bin_left
