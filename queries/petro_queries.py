import sys
import os

sys.path.insert(0, os.getcwd())

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, rand, split, explode, regexp_replace, corr

from schemas.dataframes import get_episode_df, get_basics_df, get_akas_df, get_crew_df, get_principals_df, get_ratings_df, get_name_df


spark_session = (SparkSession.builder
                             .master('local')
                             .appName('test app')
                             .config(conf=SparkConf())
                             .getOrCreate())


title_episode = get_episode_df(spark_session)

title_basic = get_basics_df(spark_session)

title_akas = get_akas_df(spark_session)

rating = get_ratings_df(spark_session)

merged_data = title_episode.join(title_basic, title_episode['parentTconst'] == title_basic['tconst'], 'inner') \
                           .orderBy(rand()) \
                           .select("primaryTitle", "originalTitle", col("startYear").alias("Year"), "seasonNumber", "episodeNumber" , "genres") \
                           .na.drop() \
                           .limit(10)

merged_data.show()
