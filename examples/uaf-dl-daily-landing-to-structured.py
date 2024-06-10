from src.powertools.glue_spark.lps_glue import LPSGlue

from pyspark.sql import functions as f
from pyspark.sql.types import DateType, TimestampType


"""

1.  Log starting message with job name and job ID
2.  Log environment variables
3.  Create a Spark database with the provided catalog database name

4.  Connect to Redshift database
5.  Execute SQL query to fetch job details from a control table (elt.elt_table_daily_ctl)

6.  Fetch all rows from the query result
7.  Log the number of jobs fetched

8.  For each job in the fetched rows:
    9.  If no data in the landing zone -> Log a warning message, skip to step 16
    10. Copy data from landing zone to staging zone in S3 bucket
    11. Move the landing zone data in backup zone
    12. Read parquet files from staging zone
    13. Add system columns to the DataFrame (timestamps, job ID, etc.)
    14. Write DataFrame to processed zone in S3 bucket in Hudi format
    15. Delete staging zone files

    16. If processed zone data doesn't exist -> skip to step 27
    17. Read Hudi table from processed zone
    18. Filter DataFrame based on provided date range
    19. If no data to be processed -> skip to step 27
    20. Add system columns to the DataFrame
    21. Fill null for PK columns
    22. If structured zone path exists (NOT initial load) -> skip to step 24
    23. Filter "DELETE" records -> skip to step 25
    24. Apply SCD2 logic
    25. Add business date column
    26. Write DataFrame to structured zone in S3 bucket in Hudi format

    27. Update control table (elt.elt_table_daily_ctl) to mark job as ready

"""

host = None
port = None
database = None
user = None
password =  None



with LPSGlue() as lpsglue:
    lpsglue.args = lpsglue.args
    lpsglue.spark.sql(f"CREATE DATABASE IF NOT EXISTS {lpsglue.args.DB_NAME}")
    redshift_conn = lpsglue.aws.redshift.connect(host=host, port=port, database=database, user=user, password=password)
    records = lpsglue.aws.redshift.execute_query(f'''
        SELECT elt_batch_name, elt_table_name, elt_business_date, elt_start_time, elt_from_date, elt_to_date, target_owner, daily as target_table_name, primary_key primary_keys, elt_source_schema
        FROM elt.elt_table_daily_ctl ctl LEFT JOIN config.table_list tl
        ON ctl.elt_batch_name = tl.owner
        AND ctl.elt_table_name = tl.table_name
        WHERE glue_job_id = '{lpsglue.args.GLUE_JOB_ID}'
          AND elt_status = 'R'
    ''')
    for record in records:
        # Landing to Process
        prefix = f'/batch/oracle12c/hk/{record["elt_batch_name"]}/{record["elt_table_name"]}'
        landing_files = lpsglue.aws.s3.list_objects(
            bucket_name=lpsglue.args.LANDING_BUCKET_NAME, 
            prefix=prefix
        )
        if not landing_files:
            lpsglue.log.warning("No records")
        else:
            lpsglue.aws.s3.copy_objects(
                source_bucket=lpsglue.args.LANDING_BUCKET_NAME,
                source_key=landing_files,
                dest_bucket=lpsglue.args.STAGING_BUCKET_NAME,
                dest_key=landing_files
            )
            lpsglue.aws.s3.move_objects(
                source_bucket=lpsglue.args.LANDING_BUCKET_NAME,
                source_key=landing_files,
                dest_bucket=lpsglue.args.BACKUP_BUCKET_NAME,
                dest_key=landing_files
            )

        staging_files = lpsglue.aws.s3.list_objects(
            bucket_name=lpsglue.args.STAGING_BUCKET_NAME, 
            prefix=prefix
        )
        if not staging_files:
            lpsglue.log.warning("No records")
        else:
            df = lpsglue.read.parquet(f's3://{lpsglue.args.STAGING_BUCKET_NAME}/{prefix}')
            df = lpsglue.tran.add_column(df, 'DMS_TIMESTAMP', f.from_utc_timestamp(f.col("DMS_TIMESTAMP").cast(TimestampType()), "Asia/Hong_Kong"))
            df = lpsglue.tran.add_column(df, 'GLUE_JOB_ID', f.lit(lpsglue.args.GLUE_JOB_ID))
            df = lpsglue.tran.add_column(df, 'GLUE_ELT_UPDATE_DATETIME', f.from_utc_timestamp(f.current_timestamp(), "Asia/Hong_Kong"))
            lpsglue.write.hudi(
                df=df,
                path=f"s3://{lpsglue.args.PROCESSED_BUCKET_NAME}/processed/{prefix}",
                record_key='DMS_TIMESTAMP',
                partition_key=None,
                table_name=f"processed_{record['elt_batch_name']}_{record['elt_table_name']}".lower(),
                dedup=False
            )

            lpsglue.aws.s3.delete_objects(bucket_name=lpsglue.args.STAGING_BUCKET_NAME, prefix=prefix)

        # Process to Structure
        processed_files = lpsglue.aws.s3.list_objects(
            bucket_name=lpsglue.args.STAGING_BUCKET_NAME, 
            prefix=prefix
        )

        if not processed_files:
            lpsglue.log.warning("No records in process zone")
        else:
            df = lpsglue.read.hudi(f's3://{lpsglue.args.PROCESSED_BUCKET_NAME}/processed/{prefix}')
            df = df.filter(f"DMS_TIMESTAMP between to_timestamp('{record['elt_from_date']}') and to_timestamp('{record['elt_to_date']}')")
            if df.count() == 0:
                lpsglue.log.warning("No records to process")
            else:
                df = lpsglue.tran.keep_last(df, record['primary_keys'], [f.desc("DMS_TIMESTAMP"),f.desc("_hoodie_commit_seqno")])
                df = lpsglue.tran.add_column(df, 'ELT_START_DATE', f.col('DMS_TIMESTAMP'))
                df = lpsglue.tran.add_column(df, 'ELT_END_DATE', f.lit(None).cast(TimestampType()))
                df = lpsglue.tran.add_column(df, 'ELT_LOAD_DATE', f.lit(record['elt_start_time']).cast(DateType()))
                df = lpsglue.tran.add_column(df, 'ELT_STATUS', f.lit('active'))
                df = lpsglue.tran.add_column(df, 'ELT_SOURCE_SYSTEM', f.lit(record['elt_source_schema']))
                df = lpsglue.tran.add_column(df, 'GLUE_JOB_ID', f.lit(lpsglue.args.GLUE_JOB_ID))
                df = lpsglue.tran.add_column(df, 'GLUE_ELT_UPDATE_DATETIME', f.from_utc_timestamp(f.current_timestamp(), "Asia/Hong_Kong"))

                
                initial_load = True
                structured_zone_path = f"s3://{lpsglue.args.STRCUTRED_BUCKET_NAME}/{prefix}"
                try:
                    lpsglue.read.hudi(structured_zone_path)
                    initial_load = False
                except:
                    pass

                if initial_load:
                    df = df.filter("Op != 'D'")
                    df = lpsglue.tran.drop_columns(df, ['Op', 'DMS_TIMESTAMP'])
                
                else:

                    current_df = lpsglue.read.hudi(structured_zone_path)
                    current_df = current_df.filter("elt_status = 'active'")
                    df = lpsglue.tran.apply_type_2_scd(
                        current_df=current_df, 
                        change_log=df, 
                        primary_keys=records['primary_keys'], 
                        timestamp_keys='elt_start_date',
                        status_col='elt_status',
                        start_date_col='elt_start_date',
                        end_date_col='elt_end_date'
                    )
                    
                df = lpsglue.tran.add_column(df, 'ELT_BUSINESS_DATE', f.lit(record["elt_business_date"]).cast(DateType()))
                lpsglue.write.hudi(
                    df=df,
                    path=structured_zone_path,
                    record_key=",".join(record['primary_keys'] + ["ELT_START_DATE"]),
                    partition_key=None,
                    table_name=f"structured_{record['elt_batch_name']}_{record['elt_table_name']}".lower(),
                    dedup=False if initial_load else True
                )
        lpsglue.aws.redshift.execute_query(f'''
            UPDATE ELT.elt_table_daily_ctl
            SET 
                elt_step_index = 2,
                elt_status = 'R', 
                elt_status_desc = 'Ready', 
                glue_elt_update_datetime = TIMEZONE('Asia/Hong_Kong', current_timestamp)
            WHERE elt_step_index = 1
            AND elt_batch_name = '{record["elt_batch_name"]}'
            AND elt_table_name = '{record["elt_table_name"]}'
            AND elt_business_date = '{record["elt_business_date"]}'
        ''')