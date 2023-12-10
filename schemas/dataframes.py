import sys
import pathlib
import pyspark.sql.types as t
import pyspark.sql.functions as f
from schemas.imdb_schema import akas_schema, basics_schema, crew_schema, episode_schema, principals_schema, ratings_schema, name_schema


def get_project_dir():
    recursion_limit = 32
    cur_dir = pathlib.Path().absolute()
    i = 0
    while cur_dir.name != 'big-data-project' and i < recursion_limit:
        cur_dir = cur_dir.parent
        i += 1
    if i == recursion_limit:
        raise Exception('Recursion limit reached, project directory not found')
    return cur_dir

project_dir = get_project_dir()
print(project_dir)

# create dataframes for schemas
def get_akas_df(spark):
    df = spark.read.csv(str(project_dir) + '/data/title.akas.tsv', sep=r'\t', schema=akas_schema, header=True)
    df = df.withColumn('types', f.split(df.types, ','))
    df = df.withColumn('attributes', f.split(df.attributes, ','))
    df = df.withColumn('isOriginalTitle', f.when(df.isOriginalTitle == 1, True).otherwise(False))
    return df

def get_basics_df(spark):
    df = spark.read.csv(str(project_dir) + '/data/title.basics.tsv', sep=r'\t', schema=basics_schema, header=True)
    df = df.withColumn('genres', f.split(df.genres, ','))
    return df

def get_crew_df(spark):
    df = spark.read.csv(str(project_dir) + '/data/title.crew.tsv', sep=r'\t', schema=crew_schema, header=True)
    df = df.withColumn('directors', f.split(df.directors, ','))
    df = df.withColumn('writers', f.split(df.writers, ','))
    return df

def get_episode_df(spark):
    return spark.read.csv(str(project_dir) + '/data/title.episode.tsv', sep=r'\t', schema=episode_schema, header=True)

def get_principals_df(spark):
    return spark.read.csv(str(project_dir) + '/data/title.principals.tsv', sep=r'\t', schema=principals_schema, header=True)

def get_ratings_df(spark):
    return spark.read.csv(str(project_dir) + '/data/title.ratings.tsv', sep=r'\t', schema=ratings_schema, header=True)

def get_name_df(spark):
    df = spark.read.csv(str(project_dir) + '/data/name.basics.tsv', sep=r'\t', schema=name_schema, header=True)
    df = df.withColumn('knownForTitles', f.split(df.knownForTitles, ','))
    df = df.withColumn('primaryProfession', f.split(df.primaryProfession, ','))
    return df
