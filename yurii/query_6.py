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

bollywood_mean_rating = (
    title_akas
    .filter(title_akas.region == "IN")
    .join(rating, title_akas.titleId == rating.tconst)
    .groupBy(title_akas.region)
    .agg(mean(rating.averageRating).alias('mean_rating'))
    .limit(10)
)
bollywood_rating = bollywood_mean_rating.first().mean_rating

dwayne_best_films =(
          name.filter((name.primaryName == 'Dwayne Johnson') & (name.birthYear == 1972))
              .join(principals, principals.nconst == name.nconst)
              .join(title_basic, title_basic.tconst == principals.tconst)
              .filter((title_basic.titleType == 'movie') | (title_basic.titleType == 'tvMovie'))
              .join(rating, title_basic.tconst == rating.tconst)
              .filter((rating.averageRating > bollywood_rating))
              .select(name.primaryName, title_basic.originalTitle, rating.averageRating)
              .limit(50)
) 

dwayne_best_films.show()