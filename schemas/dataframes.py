import sys
import pathlib
import pyspark.sql.types as t
from schemas.imdb_schema import akas_schema, basics_schema, crew_schema, episode_schema, principals_schema, ratings_schema, name_schema

def get_project_dir():
    cur_dir = pathlib.Path().absolute()
    while cur_dir.name != 'big-data-project':
        cur_dir = cur_dir.parent
    return cur_dir

project_dir = get_project_dir()
print(project_dir)

# create dataframes for schemas
def get_akas_df(spark):
    df = spark.read.csv(str(project_dir) + '/data/title.akas.tsv', sep=r'\t', schema=akas_schema, header=True)
    df = df.withColumn('types', t.ArrayType(t.StringType(), True))
    df = df.withColumn('attributes', t.ArrayType(t.StringType(), True))
    return df

def get_basics_df(spark):
    df = spark.read.csv(str(project_dir) + '/data/title.basics.tsv', sep=r'\t', schema=basics_schema, header=True)
    df = df.withColumn('genres', t.ArrayType(t.StringType(), True))
    return df

def get_crew_df(spark):
    df = spark.read.csv(str(project_dir) + '/data/title.crew.tsv', sep=r'\t', schema=crew_schema, header=True)
    df = df.withColumn('directors', t.ArrayType(t.StringType(), True))
    df = df.withColumn('writers', t.ArrayType(t.StringType(), True))
    return df

def get_episode_df(spark):
    return spark.read.csv(str(project_dir) + '/data/title.episode.tsv', sep=r'\t', schema=episode_schema, header=True)

def get_principals_df(spark):
    return spark.read.csv(str(project_dir) + '/data/title.principals.tsv', sep=r'\t', schema=principals_schema, header=True)

def get_ratings_df(spark):
    return spark.read.csv(str(project_dir) + '/data/title.ratings.tsv', sep=r'\t', schema=ratings_schema, header=True)

def get_name_df(spark):
    df = spark.read.csv(str(project_dir) + '/data/name.basics.tsv', sep=r'\t', schema=name_schema, header=True)
    df = df.withColumn('knownForTitles', t.ArrayType(t.StringType(), True))
    df = df.withColumn('primaryProfession', t.ArrayType(t.StringType(), True))
    return df
