from pyspark.sql import DataFrame


class GlueSparkWriter:
    def __init__(self, spark):
        self.spark = spark

    def csv(self, df: DataFrame, path: str, **options):
        df.write.csv(path, **options)

    def parquet(self, df: DataFrame, path: str, **options):
        df.write.parquet(path, **options)

    def hudi(self, df: DataFrame, path: str, **options):
        # Import hudi java
        df.write.format("hudi").options(**options).save(path)

    # def write_delta(self, df: DataFrame, path: str, **options):
    #     df.write.format("delta").options(**options).save(path)

    # def write_jdbc(self, df: DataFrame, url: str, table: str, properties):
    #     df.write.jdbc(url=url, table=table, properties=properties)