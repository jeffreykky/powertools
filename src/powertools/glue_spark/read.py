from pyspark.sql.dataframe import DataFrame



class GlueSparkReader:
    def __init__(self, spark):
        self.spark = spark

    def csv(self, path, **options) -> DataFrame:
        return self.spark.read.csv(path, **options)

    def parquet(self, path, **options) -> DataFrame:
        return self.spark.read.parquet(path, **options)

    # def read_hudi(self, path, **options):
    #     return self.spark.read.format("hudi").load(path, **options)

    # def read_delta(self, path, **options):
    #     return self.spark.read.format("delta").load(path, **options)

    # def read_jdbc(self, url, table, properties):
    #     return self.spark.read.jdbc(url=url, table=table, properties=properties)