# DBT_NAME: source_data
# DBT_DESCRIPTION: Bronze layer table containing raw source data with year mapping
# DBT_CONFIG:
#   materialized: table
#   partition_by: ['report_year']

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from src.utils import get_spark_session, write_delta_table, add_metadata_columns
from src.config import table_config, processing_config

def process_source_data():
    """Process source data and create bronze layer table."""
    # Initialize Spark session
    spark = get_spark_session()
    
    # Read source data
    monterey_df = spark.read.table(table_config.monterey_table)
    date_mapping_df = spark.read.table(table_config.date_mapping_table)
    
    # Data quality checks
    monterey_df = monterey_df.filter(col("montereyschemeid").isNotNull())
    monterey_df = monterey_df.filter(col("FundName").isNotNull())
    monterey_df = monterey_df.filter(col("sourcedate").isNotNull())
    
    date_mapping_df = date_mapping_df.filter(col("date").isNotNull())
    date_mapping_df = date_mapping_df.filter(col("year").isNotNull())
    
    # Transform data
    transformed_df = (monterey_df
                     .join(date_mapping_df,
                           monterey_df.sourcedate == date_mapping_df.date,
                           "left")
                     .withColumn("report_year", col("year"))
                     .filter(col("SourceDate") == processing_config.source_date))
    
    # Add metadata
    final_df = add_metadata_columns(transformed_df)
    
    # Write to Delta table
    write_delta_table(
        final_df,
        table_config.source_data_table,
        partition_by=["report_year"]
    )
    
    return final_df

if __name__ == "__main__":
    process_source_data() 