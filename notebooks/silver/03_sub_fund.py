# DBT_NAME: sub_fund
# DBT_DESCRIPTION: Sub-fund information derived from source data
# DBT_CONFIG:
#   materialized: table
#   partition_by: ['year']

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, cast, lit
from src.utils import get_spark_session, write_delta_table, add_metadata_columns
from src.config import table_config

def process_sub_fund_data():
    """Process sub-fund data and create silver layer table."""
    # Initialize Spark session
    spark = get_spark_session()
    
    # Read source data from bronze layer
    source_df = spark.read.table(f"{table_config.bronze_schema}.{table_config.source_data_table}")
    
    # Data quality checks
    source_df = source_df.filter(col("sub_fund_id").isNotNull())
    source_df = source_df.filter(col("montereyschemeid").isNotNull())
    source_df = source_df.filter(col("report_year").isNotNull())
    
    # Transform data
    final_df = (source_df
                .select(
                    col("sub_fund_id"),
                    col("montereyschemeid").alias("fund_id"),
                    col("report_year").alias("year")
                )
                .distinct()
                .withColumn(
                    "sub_fund_keyid",
                    concat(
                        cast(col("sub_fund_id"), "string"),
                        lit("-"),
                        cast(col("year"), "string")
                    )
                ))
    
    # Add metadata
    final_df = add_metadata_columns(final_df)
    
    # Write to Delta table
    write_delta_table(
        final_df,
        table_config.sub_fund_table,
        partition_by=["year"]
    )
    
    return final_df

if __name__ == "__main__":
    process_sub_fund_data() 