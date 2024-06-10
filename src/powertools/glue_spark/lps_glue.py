import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
## @params: [JOB_NAME]

from .log import GlueLogger
from .read import GlueSparkReader
from .write import GlueSparkWriter
from .transform import GlueSparkTransformer
from .aws import AWS


class LPSGlue:
    def __init__(self, spark_shell:bool = True):


        self.log = GlueLogger
        self.aws = AWS()

        if spark_shell:
            args = [arg[2:] for arg in sys.argv if (arg.startswith('--') and arg not in Job.job_bookmark_range_options())]
            self.args = getResolvedOptions(sys.argv, args)
            self.sc = SparkSession.builder.getOrCreate()
            self.glueContext = GlueContext(self.sc)
            self.spark = self.glueContext.spark_session
            self.job = Job(self.glueContext)
            self.job.init(self.args['JOB_NAME'], self.args)
            
            self.fix_glue_legacy_timestamp()
            self.read = GlueSparkReader(self.spark)
            self.tran = GlueSparkTransformer
            self.write = GlueSparkWriter(self.spark)
        
        else:
            # Todo: python shell initialization
            pass
        
    
    def fix_glue_legacy_timestamp(self) -> None:
        self.spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        self.spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        self.spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        self.spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.sc:
            self.sc.stop()
        if self.job:
            self.job.commit()