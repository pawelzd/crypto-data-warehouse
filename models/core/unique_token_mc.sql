SELECT
    tp.token_address,
    MAX(tm.total_supply * tp.price_usd) AS max_mc,
    (tm.total_supply*tp.price_usd) AS mktcap
  FROM {{ ref('birdeye_ohlcv') }} tp
  LEFT JOIN {{ref('birdeye_market_data') }} tm
    ON tp.token_address = tm.token_address
  GROUP BY tp.token_address, (tm.total_supply*tp.price_usd)
