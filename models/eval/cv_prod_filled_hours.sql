

WITH base AS (
  SELECT address,
         TIMESTAMP_TRUNC(datetime, HOUR) AS hour_ts,
         price,
         volume
  FROM {{ source('streamed_datapublic', 'public_historical_prices') }}
  QUALIFY ROW_NUMBER() OVER (
            PARTITION BY address, TIMESTAMP_TRUNC(datetime, HOUR)
            ORDER BY datetime DESC
         ) = 1
),
bounds AS (
  SELECT address,
         MIN(hour_ts) AS start_ts,
         MAX(hour_ts) AS end_ts
  FROM base
  GROUP BY address
),
hours AS (
  -- generate a complete hourly series between first and last seen timestamps per address
  SELECT address, ts AS hour_ts
  FROM bounds,
  UNNEST(GENERATE_TIMESTAMP_ARRAY(start_ts, end_ts, INTERVAL 1 HOUR)) AS ts
),
joined AS (
  SELECT h.address,
         h.hour_ts,
         b.price  AS price_raw,
         b.volume AS volume_raw
  FROM hours h
  LEFT JOIN base b
    ON b.address = h.address
   AND b.hour_ts = h.hour_ts
),
filled AS (
  SELECT
    address,
    hour_ts,
    -- forward-fill last known price; before the first known point remains NULL
    LAST_VALUE(price_raw IGNORE NULLS) OVER (
      PARTITION BY address
      ORDER BY hour_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS price,
    IFNULL(volume_raw, 0) AS volume
  FROM joined
)
SELECT address, hour_ts AS datetime, price, volume
FROM filled
ORDER BY address, datetime