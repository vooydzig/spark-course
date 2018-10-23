from pyspark.sql import SparkSession

DATA_DIR = '/tmp/data/'


def setup(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
