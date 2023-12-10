import pyspark.sql.types as t


# -------title.akas schema-------
"""
    titleId (string) - a tconst, an alphanumeric unique identifier of the title
    ordering (integer) – a number to uniquely identify rows for a given titleId
    title (string) – the localized title
    region (string) - the region for this version of the title
    language (string) - the language of the title
    types (array) - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning
    attributes (array) - Additional terms to describe this alternative title, not enumerated
    isOriginalTitle (boolean) – 0: not original title; 1: original title
"""

akas_schema = t.StructType([
    t.StructField('titleId', t.StringType(), False),
    t.StructField('ordering', t.IntegerType(), True),
    t.StructField('title', t.StringType(), True),
    t.StructField('region', t.StringType(), True),
    t.StructField('language', t.StringType(), True),
    t.StructField('types', t.StringType(), True),
    t.StructField('attributes', t.StringType(), True),
    t.StructField('isOriginalTitle', t.IntegerType(), True)
    ])


# -------title.basics schema-------
"""
    tconst (string) - alphanumeric unique identifier of the title
    titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
    primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
    originalTitle (string) - original title, in the original language
    isAdult (boolean) - 0: non-adult title; 1: adult title
    startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year
    endYear (YYYY) – TV Series end year. ‘\\N’ for all other title types
    runtimeMinutes – primary runtime of the title, in minutes
    genres (string array) – includes up to three genres associated with the title

"""

basics_schema = t.StructType([
    t.StructField('tconst', t.StringType(), False),
    t.StructField('titleType', t.StringType(), True),
    t.StructField('primaryTitle', t.StringType(), True),
    t.StructField('originalTitle', t.StringType(), True),
    t.StructField('isAdult', t.BooleanType(), True),
    t.StructField('startYear', t.IntegerType(), True),
    t.StructField('endYear', t.IntegerType(), True),
    t.StructField('runtimeMinutes', t.IntegerType(), True),
    t.StructField('genres', t.StringType(), True)
    ])


# -------title.crew schema-------
"""
    tconst (string) - alphanumeric unique identifier of the title
    directors (array of nconsts) - director(s) of the given title
    writers (array of nconsts) – writer(s) of the given title
"""

crew_schema = t.StructType([
    t.StructField('tconst', t.StringType(), False),
    t.StructField('directors', t.StringType(), True),
    t.StructField('writers', t.StringType(), True)
    ])


# -------title.episode schema-------
"""
    tconst (string) - alphanumeric identifier of episode
    parentTconst (string) - alphanumeric identifier of the parent TV Series
    seasonNumber (integer) – season number the episode belongs to
    episodeNumber (integer) – episode number of the tconst in the TV series

"""

episode_schema = t.StructType([
    t.StructField('tconst', t.StringType(), False),
    t.StructField('parentTconst', t.StringType(), True),
    t.StructField('seasonNumber', t.IntegerType(), True),
    t.StructField('episodeNumber', t.IntegerType(), True)
    ])


# -------title.principals schema-------
"""
    tconst (string) - alphanumeric unique identifier of the title
    ordering (integer) – a number to uniquely identify rows for a given titleId
    nconst (string) - alphanumeric unique identifier of the name/person
    category (string) - the category of job that person was in
    job (string) - the specific job title if applicable, else '\\N'
    characters (string) - the name of the character played if applicable, else '\\N'
"""

principals_schema = t.StructType([
    t.StructField('tconst', t.StringType(), False),
    t.StructField('ordering', t.IntegerType(), True),
    t.StructField('nconst', t.StringType(), True),
    t.StructField('category', t.StringType(), True),
    t.StructField('job', t.StringType(), True),
    t.StructField('characters', t.StringType(), True)
    ])


# -------title.ratings schema-------
"""
    tconst (string) - alphanumeric unique identifier of the title
    averageRating – weighted average of all the individual user ratings
    numVotes - number of votes the title has received
"""

ratings_schema = t.StructType([
    t.StructField('tconst', t.StringType(), False),
    t.StructField('averageRating', t.FloatType(), True),
    t.StructField('numVotes', t.IntegerType(), True)
    ])


# -------name.basics schema-------
"""
    nconst (string) - alphanumeric unique identifier of the name/person
    primaryName (string)– name by which the person is most often credited
    birthYear – in YYYY format
    deathYear – in YYYY format if applicable, else '\\N'
    primaryProfession (array of strings)– the top-3 professions of the person
    knownForTitles (array of tconsts) – titles the person is known for
"""

name_schema = t.StructType([
    t.StructField('nconst', t.StringType(), False),
    t.StructField('primaryName', t.StringType(), True),
    t.StructField('birthYear', t.IntegerType(), True),
    t.StructField('deathYear', t.IntegerType(), True),
    t.StructField('primaryProfession', t.StringType(), True),
    t.StructField('knownForTitles', t.StringType(), True)
    ])
