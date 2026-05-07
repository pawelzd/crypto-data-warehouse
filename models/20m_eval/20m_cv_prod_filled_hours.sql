{{ config(
    materialized = 'view'
) }}
with preprebase AS (

    SELECT distinct address,
         TIMESTAMP_TRUNC(datetime, HOUR) AS hour_ts,
         price,
         volume
  FROM {{ source('streamed_datapublic', 'public_historical_prices') }}
  UNION ALL
  select * from `20m_eval.tmp_test_toprod_data`

),
prebase AS (
  SELECT *
  FROM preprebase
  where address not in (
    select address
    from {{ source('streamed_datapublic', 'public_historical_prices') }}
    group by address
    having abs(avg(price) - 1) <= 0.05
  )
), base AS (
  SELECT address,
         hour_ts,
         price,
         volume,
         ROW_NUMBER() OVER (
           PARTITION BY address, hour_ts
           ORDER BY hour_ts DESC
         ) AS rn
  FROM prebase
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
FROM filled b
where not exists (select 1 from {{ref('token_missing_data_h')}} tmd where b.address = tmd.token_address and tmd.chain='sol')
    and not exists (select 1 from {{ref('scam_h_union')}} su where su.chain='sol' and b.address = su.token_address)
