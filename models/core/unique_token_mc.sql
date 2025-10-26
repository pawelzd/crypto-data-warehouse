SELECT
    tp.token_chain_id,
    tp.token_address,
    tp.price_timestamp,
    MAX(tm.total_supply * tp.price_usd) AS max_mc,
    (tm.total_supply*tp.price_usd) AS mktcap
  FROM {{ ref('token_ohlcv_view') }} tp
  LEFT JOIN {{ref('birdeye_market_data') }} tm
    ON tp.token_chain_id = tm.token_chain_id
  GROUP BY tp.token_chain_id, tp.token_address, tp.price_timestamp, (tm.total_supply*tp.price_usd)
