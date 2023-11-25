import sys
import os

sys.path.insert(0, os.getcwd())

from schemas.imdb_schema import episode_schema
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark_session = (SparkSession.builder
                             .master('local')
                             .appName('test app')
                             .config(conf=SparkConf())
                             .getOrCreate())

tsv_path = r"D:\study\circus\HDFS\project\github\big-data-project\data\title.episode.tsv"

df = spark_session.read.option("delimiter", "\t").schema(episode_schema).csv(tsv_path)

df.printSchema()

df.show()
