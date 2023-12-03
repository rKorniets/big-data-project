import sys
import os
import pyspark.sql.types as t
sys.path.insert(0, os.path.dirname(os.getcwd()))

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, rand, split, explode, regexp_replace, corr, desc, countDistinct, row_number, mean, count, array_contains, size

from schemas.dataframes import get_episode_df, get_basics_df, get_akas_df, get_crew_df, get_principals_df, get_ratings_df, get_name_df, project_dir

spark_session = (SparkSession.builder
                             .master('local')
                             .appName('test app')
                             .config(conf=SparkConf())
                             .getOrCreate())

title_episode = get_episode_df(spark_session)

title_basic = get_basics_df(spark_session)

title_akas = get_akas_df(spark_session)

rating = get_ratings_df(spark_session)

crew = get_crew_df(spark_session)

name = get_name_df(spark_session)

principals = get_principals_df(spark_session)


directors_mean_rating = (
    crew.filter(size(crew.directors) == 1)
    .join(rating, crew.tconst == rating.tconst)
    .filter(rating.numVotes > 25000)
    .groupBy(crew.directors)
    .agg(mean(rating.averageRating).alias('mean_rating'))
    .join(name, crew.directors[0] == name.nconst)
    .select(name.primaryName, 'mean_rating')
    .orderBy(col("mean_rating").desc())
    .limit(10)
)
directors_mean_rating.show()