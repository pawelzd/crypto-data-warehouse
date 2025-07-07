# DBT (Data Build Tool) - Understanding Key Features and Project Structure

## Most Useful DBT Features

### 1. **SQL-Based Transformations**
- Write transformations in SQL with Jinja templating
- No need to learn complex programming languages
- Version control your SQL transformations

### 2. **Modularity with Models**
- Break complex transformations into smaller, reusable pieces
- Reference other models using `{{ ref('model_name') }}`
- Automatic dependency management

### 3. **Testing Framework**
- Built-in tests: unique, not_null, accepted_values, relationships
- Custom data tests for business logic validation
- Test your transformations automatically

### 4. **Documentation**
- Auto-generate documentation from your models
- Add descriptions to columns and models
- Create a searchable data catalog

### 5. **Incremental Models**
- Process only new/changed data
- Significantly faster for large datasets
- Reduce compute costs

### 6. **Macros and Packages**
- Reusable SQL snippets with Jinja macros
- Import community packages for common patterns
- DRY (Don't Repeat Yourself) principle

### 7. **Seeds**
- Version control CSV files as tables
- Perfect for lookup tables and static data
- Automatically load into your warehouse

### 8. **Snapshots**
- Track slowly changing dimensions (SCD Type 2)
- Historical tracking of data changes
- Automated valid_from/valid_to columns

## DBT Project Folder Structure

```
dbt_project/
│
├── dbt_project.yml          # Main configuration file
├── profiles.yml             # Connection configurations (usually in ~/.dbt/)
│
├── models/                  # All your SQL transformations
│   ├── staging/            # Raw data cleaning and standardization
│   │   ├── stg_orders.sql
│   │   └── schema.yml      # Tests and documentation
│   │
│   ├── intermediate/       # Business logic and joins
│   │   ├── int_order_items.sql
│   │   └── schema.yml
│   │
│   └── marts/              # Final business-ready tables
│       ├── finance/
│       │   ├── fct_revenue.sql
│       │   └── schema.yml
│       └── marketing/
│           ├── dim_customers.sql
│           └── schema.yml
│
├── data/                   # CSV seed files
│   └── country_codes.csv
│
├── macros/                 # Reusable SQL functions
│   └── generate_alias_name.sql
│
├── tests/                  # Custom test queries
│   └── assert_positive_values.sql
│
├── snapshots/              # SCD Type 2 historical tracking
│   └── customers_snapshot.sql
│
└── analysis/               # Ad-hoc queries (not built in prod)
    └── customer_analysis.sql
```

## Field Transformation Examples

### 1. Staging Layer (models/staging/)
Clean and standardize raw data:

```sql
-- models/staging/stg_orders.sql
{{ config(
    materialized='view'
) }}

SELECT
    -- Rename for consistency
    order_id AS order_id,
    customer_id AS customer_id,
    
    -- Type casting
    CAST(order_date AS DATE) AS order_date,
    
    -- Field transformations
    LOWER(TRIM(status)) AS order_status,
    ROUND(total_amount, 2) AS order_amount,
    
    -- Add metadata
    CURRENT_TIMESTAMP() AS _loaded_at

FROM {{ source('raw', 'orders') }}
WHERE order_id IS NOT NULL
```

### 2. Intermediate Layer (models/intermediate/)
Apply business logic:

```sql
-- models/intermediate/int_customer_orders.sql
{{ config(
    materialized='table'
) }}

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        o.order_id,
        o.order_date,
        o.order_amount,
        
        -- Calculate days since last order
        DATE_DIFF(
            CURRENT_DATE(), 
            MAX(o.order_date) OVER (PARTITION BY c.customer_id), 
            DAY
        ) AS days_since_last_order,
        
        -- Running total
        SUM(o.order_amount) OVER (
            PARTITION BY c.customer_id 
            ORDER BY o.order_date
        ) AS customer_lifetime_value

    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o
        ON c.customer_id = o.customer_id
)

SELECT * FROM customer_orders
```

### 3. Marts Layer (models/marts/)
Create business-ready datasets:

```sql
-- models/marts/finance/fct_monthly_revenue.sql
{{ config(
    materialized='incremental',
    unique_key='month_key',
    on_schema_change='fail'
) }}

SELECT
    -- Create composite key
    TO_CHAR(order_date, 'YYYY-MM') AS month_key,
    
    -- Aggregations
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(order_amount) AS gross_revenue,
    
    -- Complex calculations
    SUM(order_amount) - SUM(discount_amount) AS net_revenue,
    AVG(order_amount) AS avg_order_value,
    
    -- Percentages and ratios
    SUM(CASE WHEN order_status = 'returned' THEN order_amount ELSE 0 END) / 
        NULLIF(SUM(order_amount), 0) * 100 AS return_rate_pct

FROM {{ ref('int_customer_orders') }}

{% if is_incremental() %}
    WHERE order_date >= (SELECT MAX(TO_DATE(month_key, 'YYYY-MM')) FROM {{ this }})
{% endif %}

GROUP BY 1
```

## Best Practices for Field Transformations

1. **Naming Conventions**
   - Use consistent prefixes: `stg_`, `int_`, `fct_`, `dim_`
   - Snake_case for all objects
   - Clear, descriptive names

2. **Transformation Locations**
   - **Staging**: Type casting, renaming, basic cleaning
   - **Intermediate**: Business logic, complex joins, calculations
   - **Marts**: Final aggregations, metrics, KPIs

3. **Documentation**
   ```yaml
   # models/staging/schema.yml
   version: 2
   
   models:
     - name: stg_orders
       description: Cleaned orders data from raw source
       columns:
         - name: order_id
           description: Unique order identifier
           tests:
             - unique
             - not_null
         - name: order_amount
           description: Total order value in USD
           tests:
             - not_null
             - dbt_utils.accepted_range:
                 min_value: 0
                 max_value: 10000
   ```

4. **Testing Strategy**
   - Test early and often
   - Use generic tests for data quality
   - Write custom tests for business rules

5. **Performance Tips**
   - Use incremental models for large datasets
   - Partition tables by date when possible
   - Create appropriate materializations (view vs table)

## Getting Started Commands

```bash
# Initialize a new dbt project
dbt init my_project

# Run all models
dbt run

# Run specific models
dbt run --models staging.stg_orders+  # Run model and all downstream

# Test your models
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Run only changed models
dbt run --models state:modified+
```

This structure provides a solid foundation for organizing your DBT transformations while maintaining clarity and scalability.