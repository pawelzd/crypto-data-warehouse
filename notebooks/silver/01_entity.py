# DBT_NAME: entity
# DBT_DESCRIPTION: Combined entity information from admin, manco, and ta objects
# DBT_CONFIG:
#   materialized: table
#   partition_by: ['year']

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.utils import get_spark_session, write_delta_table, add_metadata_columns
from src.config import table_config

def process_entity_data():
    """Process entity data and create silver layer table."""
    # Initialize Spark session
    spark = get_spark_session()
    
    # Read source tables
    admin_df = spark.read.table("ent_admin_object")
    manco_df = spark.read.table("ent_manco_object")
    ta_df = spark.read.table("ent_ta_object")
    
    # Process each entity type with data quality checks
    def process_entity_df(df):
        return (df
                .filter(col("entity_id").isNotNull())
                .filter(col("entity_name").isNotNull())
                .filter(col("year").isNotNull())
                .select("entity_id", "entity_name", "entity_location", "year", "entity_key_id"))
    
    # Transform each entity type
    admin_processed = process_entity_df(admin_df)
    manco_processed = process_entity_df(manco_df)
    ta_processed = process_entity_df(ta_df)
    
    # Union all processed DataFrames
    final_df = (admin_processed
                .unionByName(manco_processed)
                .unionByName(ta_processed))
    
    # Add metadata
    final_df = add_metadata_columns(final_df)
    
    # Write to Delta table
    write_delta_table(
        final_df,
        table_config.entity_table,
        partition_by=["year"]
    )
    
    return final_df

if __name__ == "__main__":
    process_entity_data() 