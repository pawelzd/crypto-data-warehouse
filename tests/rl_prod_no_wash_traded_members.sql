-- Wash-traded tokens (curated in rl_prod_universe_membership_v1_state,
-- wash_traded_addresses) must not be members from wash_effective_from onward.
-- Returns rows (== failures) for any such member week.
SELECT token_address, week_start
FROM {{ ref('rl_prod_universe_membership_v1_state') }}
WHERE in_universe_pit
  AND week_start >= DATE '2026-10-12'
  AND token_address IN (
    'D4BPL1zvhhJbxUdgi2qVUtjx4jeQWyUr2PAUjKc9rN5x',  -- MUSK
    '7pKXpFsnZS5BB4Eydk3uZ84FeKDSvkv1z4Hv5ayQ28RV',  -- WYT
    'C8fU5GdfAt5mnw2RK7HE6XJGFNxHpaskZMkXxdm88888',  -- CTM
    '2bpT3ksMdwdZ6DuHyq3FDUr7HDwvZ5DRZoT1fUPALJaH'   -- RIV
  )
