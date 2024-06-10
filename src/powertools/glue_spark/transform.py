from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, max as spark_max, lead, lag
from pyspark.sql.window import Window
from pyspark.sql import functions as f


class GlueSparkTransformer:

    @staticmethod
    def rename_columns(df: DataFrame, columns_mapping: dict) -> DataFrame:
        for old_name, new_name in columns_mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

    @staticmethod
    def replace_values(df: DataFrame, values_mapping: dict):
        df = df.replace(values_mapping)
        return df
    
    @staticmethod
    def add_column(df: DataFrame, name: str, value):
        df = df.withColumn(name, value)
        return df
    @staticmethod
    def drop_columns(df: DataFrame, columns):
        for column in columns:
            df = df.drop(column)
    
    @staticmethod
    def keep_last(df, record_keys, order_by):
        key_window = Window.partitionBy(record_keys).orderBy(order_by)
        df = df.withColumn("rank", f.row_number().over(key_window))
        df = df.filter(df["rank"] == 1)
        df = df.drop(f.col("rank"))
        return df

    @staticmethod
    def apply_type_2_scd(
        current_df: DataFrame,
        change_log: DataFrame,
        primary_keys: list,
        timestamp_keys: list | str,
        status_col: str = 'status',
        start_date_col: str = 'effective_from',
        end_date_col: str = 'effective_to'
    ) -> DataFrame:
    
        # Ensure timestamp_keys is a list
        if isinstance(timestamp_keys, str):
            timestamp_keys = [timestamp_keys]

        # Sort change_log by primary keys and timestamp_keys to ensure sequential application of changes
        change_log = change_log.orderBy(primary_keys + timestamp_keys)
        
        # Identify the window for primary keys to handle sequential updates
        change_log_window_spec = Window.partitionBy(primary_keys).orderBy(timestamp_keys)
        
        # Extract the latest active records from the current dataframe
        active_records = current_df.filter(col(status_col) == 'active')
        
        # Create new records from the change log
        new_records = change_log.withColumn(start_date_col, col(timestamp_keys[0])) \
                                .withColumn(end_date_col, lead(timestamp_keys[0]).over(change_log_window_spec)) \
                                .withColumn(status_col, lit('active')) # really?
        
        # Join active records with change log to find the ones that need to be updated
        join_cond = [active_records[k] == change_log[k] for k in primary_keys]
        updated_current_df = active_records.join(change_log, join_cond, 'inner') \
                                        .select(active_records["*"]) \
                                        .withColumn(end_date_col, col(change_log[timestamp_keys[0]]))
        
        # Union updated current records with new records
        result_df = updated_current_df.union(new_records)
        
        # Mark the end date and status of old records as inactive based on the changes
        result_df = result_df.withColumn(status_col, 
                                        when(col(end_date_col).isNull(), col(status_col))
                                        .otherwise(lit('inactive')))
        
        return result_df
        
