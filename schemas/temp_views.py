from schemas.dataframes import get_akas_df, get_basics_df, get_crew_df, get_episode_df, get_principals_df, get_ratings_df, get_name_df
#create temp views for dataframes

def create_akas_view(spark):
    akas_df = get_akas_df(spark)
    akas_df.createOrReplaceTempView('akas')


def create_basics_view(spark):
    basics_df = get_basics_df(spark)
    basics_df.createOrReplaceTempView('basics')


def create_crew_view(spark):
    crew_df = get_crew_df(spark)
    crew_df.createOrReplaceTempView('crew')


def create_episode_view(spark):
    episode_df = get_episode_df(spark)
    episode_df.createOrReplaceTempView('episode')

def create_principals_view(spark):
    principals_df = get_principals_df(spark)
    principals_df.createOrReplaceTempView('principals')


def create_ratings_view(spark):
    ratings_df = get_ratings_df(spark)
    ratings_df.createOrReplaceTempView('ratings')


def create_name_view(spark):
    name_df = get_name_df(spark)
    name_df.createOrReplaceTempView('name')

def create_all_views(spark):
    create_akas_view(spark)
    create_basics_view(spark)
    create_crew_view(spark)
    create_episode_view(spark)
    create_principals_view(spark)
    create_ratings_view(spark)
    create_name_view(spark)
