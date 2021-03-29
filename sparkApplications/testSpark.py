import pyspark
import findspark
from pyspark.sql import SparkSession


def testSparkConection():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    # Test spark
    df = spark.createDataFrame([{"hello": "world"} for x in range(1000)])
    df.show(5)


# make pyspark importable as a regular library
findspark.init()
testSparkConection()
# create Spark session