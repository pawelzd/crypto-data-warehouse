WITH emas AS (
  SELECT *
  FROM {{ source('ema', 'ema_21_50_lables') }}
),

base AS (
  SELECT 
      e.*,
      t.open, 
      t.close, 
      t.high, 
      t.low
  FROM emas e
  INNER JOIN {{ ref('token_ohlcv') }} t
    ON e.token_address = t.token_address
   AND e.ts_hour       = t.price_timestamp
),

-- Order rows and create a per-trade id that increments on each entry signal
ordered AS (
  SELECT
      b.*,
      ROW_NUMBER() OVER (
        PARTITION BY b.token_address, b.monitoring_session_id 
        ORDER BY b.ts_hour
      ) AS rn,
      SUM(CASE WHEN b.label_entry = 1 THEN 1 ELSE 0 END) OVER (
        PARTITION BY b.token_address, b.monitoring_session_id
        ORDER BY b.ts_hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS trade_id
  FROM base b
),

-- For each trade: find the first close row (its row number) and the entry price
trade_bounds AS (
  SELECT
      o.*,
      /* first row_number where label_close=1 within this trade */
      MIN(CASE WHEN o.label_close = 1 THEN o.rn END) OVER (
        PARTITION BY o.token_address, o.monitoring_session_id, o.trade_id
      ) AS close_rn,
      /* entry price (close at the entry row) for the trade */
      MAX(CASE WHEN o.label_entry = 1 THEN o.close END) OVER (
        PARTITION BY o.token_address, o.monitoring_session_id, o.trade_id
      ) AS entry_price
  FROM ordered o
),

-- In case a trade never gets a close signal, cap the window at the last row in that trade
trade_last AS (
  SELECT
      tb.*,
      MAX(tb.rn) OVER (
        PARTITION BY tb.token_address, tb.monitoring_session_id, tb.trade_id
      ) AS last_rn_in_trade
  FROM trade_bounds tb
),

-- Max price achieved from entry until the close (or end if no close)
perf AS (
  SELECT
      tl.*,
      MAX(CASE 
            WHEN tl.rn <= COALESCE(tl.close_rn, tl.last_rn_in_trade) 
            THEN tl.high                -- use 'close' instead of 'high' if you prefer
          END
      ) OVER (
        PARTITION BY tl.token_address, tl.monitoring_session_id, tl.trade_id
      ) AS max_price_until_close
  FROM trade_last tl
),

final AS (
  SELECT
      p.*,
      CASE 
        WHEN p.entry_price IS NOT NULL AND p.max_price_until_close IS NOT NULL
          THEN (p.max_price_until_close - p.entry_price) / p.entry_price * 100.0
      END AS max_gain_pct
  FROM perf p
)

-- keep all rows; if you only want the entry rows, add: WHERE label_entry = 1
SELECT *
FROM final
