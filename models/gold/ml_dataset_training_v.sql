SELECT 
    wh.wallet_address,
    dt.*,
FROM
    {{ ref('ml_dataset_training_wo_wallets_v') }} AS dt
INNER JOIN
    {{ ref('ml_wallets_highly_held_tokens_mv') }} AS wh
    ON dt.token_address = wh.token_address
    AND dt.first_acquired_timestamp = wh.first_acquired_timestamp