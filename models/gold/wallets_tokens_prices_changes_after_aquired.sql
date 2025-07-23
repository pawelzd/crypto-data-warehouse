SELECT
    wallet_address,
    token_address,
    first_acquired_timestamp,
    price_timestamp,
    transaction_count,
    asset_amount,
    price_usd,
    price_when_acquired,
    price_change

FROM
    {{ ref('wallets_tokens_prices_changes') }} 
WHERE
    price_timestamp >= first_acquired_timestamp