# Using Notebooks with DBT Projects

## Why Use Notebooks with DBT?

Notebooks are excellent companions to DBT projects for:
1. **Data Exploration** - Understand your raw data before writing transformations
2. **Testing Queries** - Prototype complex SQL before moving to DBT models
3. **Analysis & Visualization** - Create charts and insights from your transformed data
4. **Documentation** - Show examples of how to use your data models

## Setting Up Notebooks for Your Solana Project

### 1. Directory Structure
```
dbt_bq/
├── models/
├── notebooks/              # Create this directory
│   ├── exploration/       # For data discovery
│   ├── analysis/          # For business analysis
│   └── testing/           # For testing transformations
```

### 2. Connecting to BigQuery from Notebooks

#### Option A: Using Python (Jupyter/Google Colab)
```python
# notebooks/exploration/connect_to_bigquery.ipynb

# Install required packages
!pip install google-cloud-bigquery pandas matplotlib seaborn dbt-bigquery

import pandas as pd
from google.cloud import bigquery
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize BigQuery client
client = bigquery.Client(project='positive-tuner-255507')

# Query your source data
query = """
SELECT 
    DATE(first_acquired_date) as acquisition_date,
    COUNT(DISTINCT mint) as unique_tokens,
    COUNT(*) as total_wallets
FROM `positive-tuner-255507.wallets_extraction.solana_wallet_history`
WHERE first_acquired_date >= '2024-01-01'
GROUP BY 1
ORDER BY 1
"""

df = client.query(query).to_dataframe()
df.head()
```

#### Option B: Using SQL Magic in Jupyter
```python
# Load SQL magic
%load_ext google.cloud.bigquery

# Set your project
%config Application.log_level='INFO'
project_id = 'positive-tuner-255507'

# Run SQL directly
%%bigquery wallet_summary --project $project_id
SELECT 
    DATE(first_acquired_date) as acquisition_date,
    COUNT(DISTINCT mint) as unique_tokens
FROM `positive-tuner-255507.wallets_extraction.solana_wallet_history`
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10
```

### 3. Notebook Workflows for Your Use Case

#### Exploration Notebook - Understanding Raw Data
```python
# notebooks/exploration/solana_data_exploration.ipynb

# 1. Check data quality
quality_check = """
SELECT 
    'wallet_history' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT mint) as unique_mints,
    COUNT(DISTINCT first_acquired_date) as unique_dates,
    MIN(first_acquired_date) as earliest_date,
    MAX(first_acquired_date) as latest_date
FROM `positive-tuner-255507.wallets_extraction.solana_wallet_history`
UNION ALL
SELECT 
    'price_history' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT address) as unique_addresses,
    COUNT(DISTINCT DATE(datetime)) as unique_dates,
    MIN(datetime) as earliest_date,
    MAX(datetime) as latest_date
FROM `positive-tuner-255507.wallets_extraction.solana_price_history`
"""

quality_df = client.query(quality_check).to_dataframe()
print(quality_df)

# 2. Find data issues
issues_query = """
-- Check for nulls and duplicates
WITH null_checks AS (
    SELECT 
        'wallet_nulls' as check_type,
        SUM(CASE WHEN mint IS NULL THEN 1 ELSE 0 END) as null_count,
        SUM(CASE WHEN first_acquired_date IS NULL THEN 1 ELSE 0 END) as date_null_count
    FROM `positive-tuner-255507.wallets_extraction.solana_wallet_history`
),
duplicate_checks AS (
    SELECT 
        'wallet_duplicates' as check_type,
        COUNT(*) - COUNT(DISTINCT CONCAT(mint, first_acquired_date)) as duplicate_count
    FROM `positive-tuner-255507.wallets_extraction.solana_wallet_history`
)
SELECT * FROM null_checks
UNION ALL
SELECT check_type, duplicate_count, 0 FROM duplicate_checks
"""

issues_df = client.query(issues_query).to_dataframe()
print(issues_df)
```

#### Testing Notebook - Validate DBT Transformations
```python
# notebooks/testing/validate_source_data_model.ipynb

# Compare source data with DBT model output
validation_query = """
-- Compare row counts
WITH source_count AS (
    SELECT COUNT(*) as source_rows
    FROM `positive-tuner-255507.wallets_extraction.solana_wallet_history` w
    JOIN `positive-tuner-255507.wallets_extraction.solana_price_history` p
        ON DATE(w.first_acquired_date) = DATE(p.datetime)
    WHERE w.mint IS NOT NULL 
    AND p.address IS NOT NULL
),
model_count AS (
    SELECT COUNT(*) as model_rows
    FROM `positive-tuner-255507.dbt_dev.source_data`  -- Your DBT output
)
SELECT 
    source_rows,
    model_rows,
    ABS(source_rows - model_rows) as difference,
    ROUND((model_rows * 100.0 / source_rows) - 100, 2) as pct_difference
FROM source_count, model_count
"""

validation_df = client.query(validation_query).to_dataframe()
print("Row count validation:")
print(validation_df)

# Sample data comparison
sample_comparison = """
-- Get sample of joined data
SELECT 
    'source' as data_source,
    DATE(w.first_acquired_date) as date,
    w.mint,
    p.address
FROM `positive-tuner-255507.wallets_extraction.solana_wallet_history` w
JOIN `positive-tuner-255507.wallets_extraction.solana_price_history` p
    ON DATE(w.first_acquired_date) = DATE(p.datetime)
WHERE w.mint IS NOT NULL AND p.address IS NOT NULL
LIMIT 5

UNION ALL

SELECT 
    'model' as data_source,
    first_acquired_date as date,
    NULL as mint,  -- Add your actual columns
    NULL as address
FROM `positive-tuner-255507.dbt_dev.source_data`
LIMIT 5
"""
```

#### Analysis Notebook - Business Insights
```python
# notebooks/analysis/wallet_acquisition_trends.ipynb

# 1. Daily wallet acquisition trends
daily_trends = """
SELECT 
    DATE(first_acquired_date) as acquisition_date,
    COUNT(DISTINCT mint) as unique_tokens,
    COUNT(*) as wallet_count,
    -- Calculate 7-day moving average
    AVG(COUNT(*)) OVER (
        ORDER BY DATE(first_acquired_date) 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as wallet_count_7d_ma
FROM `positive-tuner-255507.wallets_extraction.solana_wallet_history`
WHERE first_acquired_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY 1
ORDER BY 1
"""

trends_df = client.query(daily_trends).to_dataframe()

# Visualization
plt.figure(figsize=(15, 6))
plt.plot(trends_df['acquisition_date'], trends_df['wallet_count'], 
         label='Daily Wallets', alpha=0.5)
plt.plot(trends_df['acquisition_date'], trends_df['wallet_count_7d_ma'], 
         label='7-Day MA', linewidth=2)
plt.title('Solana Wallet Acquisition Trends')
plt.xlabel('Date')
plt.ylabel('Number of Wallets')
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 2. Token popularity analysis
token_analysis = """
WITH token_stats AS (
    SELECT 
        mint as token_address,
        COUNT(*) as wallet_count,
        MIN(first_acquired_date) as first_seen,
        MAX(first_acquired_date) as last_seen,
        DATE_DIFF(MAX(first_acquired_date), MIN(first_acquired_date), DAY) as active_days
    FROM `positive-tuner-255507.wallets_extraction.solana_wallet_history`
    GROUP BY 1
)
SELECT 
    token_address,
    wallet_count,
    active_days,
    ROUND(wallet_count * 1.0 / NULLIF(active_days, 0), 2) as avg_daily_wallets
FROM token_stats
WHERE wallet_count > 100  -- Filter for significant tokens
ORDER BY wallet_count DESC
LIMIT 20
"""

token_df = client.query(token_analysis).to_dataframe()

# Create bar chart
plt.figure(figsize=(12, 6))
plt.bar(range(len(token_df)), token_df['wallet_count'])
plt.title('Top 20 Tokens by Wallet Count')
plt.xlabel('Token Rank')
plt.ylabel('Number of Wallets')
plt.tight_layout()
plt.show()
```

### 4. Best Practices for Notebooks with DBT

1. **Version Control**
   ```bash
   # .gitignore
   notebooks/**/*.ipynb_checkpoints
   notebooks/**/outputs/
   ```

2. **Environment Variables**
   ```python
   # notebooks/config.py
   import os
   from dotenv import load_dotenv
   
   load_dotenv()
   
   PROJECT_ID = os.getenv('DBT_PROJECT_ID', 'positive-tuner-255507')
   DATASET_RAW = os.getenv('DBT_RAW_DATASET', 'wallets_extraction')
   DATASET_DEV = os.getenv('DBT_DEV_DATASET', 'dbt_dev')
   ```

3. **Reusable Functions**
   ```python
   # notebooks/utils.py
   from google.cloud import bigquery
   import pandas as pd
   
   def get_bq_client(project_id):
       return bigquery.Client(project=project_id)
   
   def run_query(query, project_id):
       client = get_bq_client(project_id)
       return client.query(query).to_dataframe()
   
   def test_dbt_model(model_name, expected_rows=None):
       """Test if DBT model exists and has expected properties"""
       query = f"""
       SELECT COUNT(*) as row_count
       FROM `{PROJECT_ID}.{DATASET_DEV}.{model_name}`
       """
       result = run_query(query, PROJECT_ID)
       
       if expected_rows:
           assert result['row_count'][0] == expected_rows
       
       return result
   ```

### 5. Integrating Notebooks into DBT Workflow

1. **Pre-DBT Analysis** (Before writing models)
   - Explore data distributions
   - Identify data quality issues
   - Prototype transformations

2. **During Development** (While writing models)
   - Test SQL snippets
   - Validate join conditions
   - Check intermediate results

3. **Post-DBT Validation** (After models run)
   - Compare source vs transformed data
   - Create data quality reports
   - Generate business insights

### 6. Example: Complete Workflow Notebook

```python
# notebooks/workflow/complete_analysis_workflow.ipynb

# Step 1: Connect and setup
from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt

client = bigquery.Client(project='positive-tuner-255507')

# Step 2: Run DBT models (from notebook)
!dbt run --models source_data

# Step 3: Analyze the output
analysis_query = """
WITH model_output AS (
    SELECT *
    FROM `positive-tuner-255507.dbt_dev.source_data`
),
daily_stats AS (
    SELECT 
        first_acquired_date,
        COUNT(*) as daily_count,
        COUNT(DISTINCT mint) as unique_tokens
    FROM model_output
    GROUP BY 1
)
SELECT 
    first_acquired_date,
    daily_count,
    unique_tokens,
    SUM(daily_count) OVER (ORDER BY first_acquired_date) as cumulative_count
FROM daily_stats
ORDER BY 1
"""

results_df = client.query(analysis_query).to_dataframe()

# Step 4: Visualize
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

# Daily counts
ax1.plot(results_df['first_acquired_date'], results_df['daily_count'])
ax1.set_title('Daily Wallet-Token Pairs')
ax1.set_ylabel('Count')

# Cumulative
ax2.plot(results_df['first_acquired_date'], results_df['cumulative_count'])
ax2.set_title('Cumulative Growth')
ax2.set_ylabel('Total Count')
ax2.set_xlabel('Date')

plt.tight_layout()
plt.show()

# Step 5: Export insights
results_df.to_csv('notebooks/outputs/daily_analysis.csv', index=False)
print(f"Analysis complete. Processed {len(results_df)} days of data.")
```

This guide provides a comprehensive approach to using notebooks alongside your DBT project for the Solana wallet/price data use case.