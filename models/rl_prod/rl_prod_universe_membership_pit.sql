{{ config(materialized='view') }}

{% set universe_version = var('rl_prod_universe_version', 'v1') %}

{% if universe_version == 'v2' %}
-- The physical v2 state is produced by the shared full/live Python builder.
SELECT *
FROM {{ source('rl_prod_artifacts', 'universe_membership_v2_state') }}

{% elif universe_version == 'v1' %}
-- Safe default while v2 is evaluated beside the frozen retrain baseline.
-- Compatibility columns keep downstream compilation stable without claiming
-- that v1 was built by the v2 quality contract.
--
-- Reads rl_prod_universe_membership_v1_state rather than the snapshot directly.
-- That model passes the frozen range through unchanged and appends later weeks
-- under the same v1 rules; the snapshot alone stops at 2026-07-06, which made
-- every later week resolve to in_universe_pit = FALSE here.
SELECT
  token_address,
  first_observed_date,
  week_start,
  trailing_bar_count,
  trailing_bar_count AS observed_hour_count_30d,
  CAST(NULL AS INT64) AS longest_gap_hours_30d,
  trailing_bar_count AS feature_mktcap_hour_count_30d,
  median_mktcap_30d,
  median_dollar_vol_30d,
  CAST(NULL AS FLOAT64) AS median_dollar_amihud_30d,
  CAST(NULL AS FLOAT64) AS p90_effective_spread_30d_bps,
  CAST(NULL AS FLOAT64) AS top50_median_dollar_vol_30d,
  CAST(NULL AS FLOAT64) AS dynamic_volume_floor_30d,
  dollar_volume_rank_30d,
  TRUE AS scam_clean,
  FALSE AS is_born_bad,
  CAST([] AS ARRAY<STRING>) AS born_bad_patterns,
  TRUE AS continuity_pass,
  median_mktcap_30d >= 20000000 AS size_pass,
  TRUE AS volume_pass,
  TRUE AS amihud_pass,
  TRUE AS spread_pass,
  TRUE AS quality_pass,
  meets_entry_rule AS entry_quality_pass,
  CAST(IF(meets_entry_rule, 2, 0) AS INT64) AS entry_quality_streak,
  low_mktcap_streak,
  bad_volume_streak,
  CAST(0 AS INT64) AS quality_fail_streak,
  meets_entry_rule,
  NOT (low_mktcap_streak >= 2 OR bad_volume_streak >= 4) AS passes_exit_rules,
  in_universe_pit,
  entered_this_week,
  exited_this_week,
  FALSE AS cap_blocked,
  FALSE AS cap_boundary_evicted,
  CAST(NULL AS INT64) AS cap_seat_number,
  CAST(NULL AS TIMESTAMP) AS latest_observed_timestamp,
  CAST(NULL AS TIMESTAMP) AS latest_feature_timestamp,
  CAST(NULL AS DATE) AS latest_scam_window_end,
  'universe_pit_v1_snapshot_20260713' AS rule_version,
  REPEAT('0', 64) AS rule_config_hash,
  generated_at
FROM {{ ref('rl_prod_universe_membership_v1_state') }}

{% else %}
  {{ exceptions.raise_compiler_error(
    "rl_prod_universe_version must be 'v1' or 'v2', got: " ~ universe_version
  ) }}
{% endif %}
