SELECT
  week_start,
  COUNT(DISTINCT rule_version) AS rule_versions,
  COUNT(DISTINCT rule_config_hash) AS config_hashes
FROM {{ ref('rl_prod_universe_membership_pit') }}
GROUP BY week_start
HAVING rule_versions != 1
    OR config_hashes != 1
    OR COUNTIF(rule_config_hash IS NULL OR LENGTH(rule_config_hash) != 64) > 0
