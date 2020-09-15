import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType


def process_song_data(spark, input_data, output_data):
    """
    This function processes song data, applies necessary filters and outputs necessary tables to specified path.

    Args:
        spark: spark session
        input_data: path to input data
        output_data: path to output data

    Returns:
        None
    """
    print("Started processing song data from input path " + input_data)
    df_song_data = get_song_data_df(input_data, spark)

    # prepare view to use SparkSQL with
    df_song_data.createOrReplaceTempView("staging_songs")

    # extract columns to create songs table
    songs = spark.sql("""SELECT DISTINCT song_id, title AS song_title, artist_id, year, duration 
                            FROM staging_songs
    """)

    # drop songs where year == 0 as it is not a valid year
    songs = songs.filter(songs.year != 0)

    # write songs table to parquet files partitioned by year and artist
    songs.write.partitionBy("year", "artist_id").parquet(
        path=output_data + "/songs/",
        mode="overwrite")

    # extract columns to create artists table
    artists = spark.sql("""
                            SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
                            FROM staging_songs
    """)

    # write artists table to parquet files
    artists.write.parquet(
        path=output_data + "/artists/",
        mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function processes log data, applies necessary filters and outputs necessary tables to specified path.

    Args:
        spark: spark session
        input_data: path to input data
        output_data: path to output data

    Returns:
        None
    """
    print("Started processing log data from input path " + input_data)
    df_log_data = get_log_data_df(input_data, spark)

    # filter by actions for song plays
    df_log_data = df_log_data.filter(df_log_data.page == "NextSong")

    # enrich timestamp and datetime columns
    df_log_data = enrich_log_data(df_log_data)

    # prepare view to use SparkSQL with
    df_log_data.createOrReplaceTempView("staging_logs")

    # extract columns for users table
    users = spark.sql("""
                        SELECT  staging_logs_left.userId, 
                                staging_logs_left.level, 
                                staging_logs_left.firstName, 
                                staging_logs_left.lastName, 
                                staging_logs_left.gender
                        FROM staging_logs staging_logs_left
                        INNER JOIN (SELECT userId, 
                                            MAX(timestamp) AS maxtimestamp
                                            FROM staging_logs 
                                            GROUP BY userId, page) staging_logs_right 
                        ON staging_logs_left.userId = staging_logs_right.userId 
                        AND staging_logs_left.timestamp = staging_logs_right.maxtimestamp
    """)
    users = users.dropDuplicates(['userId', 'level'])

    # write users table to parquet files
    users.write.parquet(
        path=output_data + "/users/",
        mode="overwrite")

    # extract columns to create time table
    times = spark.sql("""
                        SELECT DISTINCT start_time,
                                        hour(start_time) AS hour, 
                                        day(start_time) AS day, 
                                        weekofyear(start_time) AS week, 
                                        month(start_time) AS month, 
                                        year (start_time) AS year, 
                                        weekday(start_time) AS weekday 
                        FROM staging_logs
    """)

    # write time table to parquet files partitioned by year and month
    times.write.partitionBy("year", "month").parquet(
        path=output_data + "/time/",
        mode="overwrite")

    # read in song data to use for songplays table
    df_song_data = get_song_data_df(input_data, spark)

    # prepare view to use SparkSQL with
    df_song_data.createOrReplaceTempView("staging_songs")

    # extract columns from joined song and log datasets to create songplays table
    songplays = spark.sql("""
                            SELECT  l.start_time, 
                                    l.userId, 
                                    l.level, 
                                    r.song_id, 
                                    r.artist_id, 
                                    l.sessionId, 
                                    l.location, 
                                    l.userAgent,
                                    year(l.start_time) AS year, 
                                    month(l.start_time) AS month
                            FROM staging_logs AS l 
                            INNER JOIN staging_songs AS r 
                            ON l.song = r.title
    """).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays.write.partitionBy("year", "month").parquet(
        path=output_data + "/songplays/",
        mode="overwrite")


def get_song_data_df(input_data, spark):
    """
    This function uses spark session to read in the data and returns dataframe with proper types.

    Args:
        input_data: path to input data
        spark: spark session
    Returns:
        df_song_data: dataframe of song data with proper types
    """
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*"

    # define song schema
    df_song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])
    # read song data file
    df_song_data = spark.read.json(song_data,
                                   schema=df_song_schema,
                                   mode='DROPMALFORMED')
    return df_song_data


def get_log_data_df(input_data, spark):
    """
    This function uses spark session to read in the data and returns dataframe with proper types.

    Args:
        input_data: path to input data
        spark: spark session
    Returns:
        df_song_data: dataframe of song data with proper types
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*"

    # define log schema
    df_log_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", IntegerType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", IntegerType()),
        StructField("song", StringType()),
        StructField("status", IntegerType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType()),
    ])
    # read log data file
    df_log_data = spark.read.json(log_data,
                                  schema=df_log_schema,
                                  mode='DROPMALFORMED')
    return df_log_data


def enrich_log_data(df_log_data):
    """
    This function uses original log data to enrich it with converted timestamp column and another datetime presentation.

    Args:
        df_log_data: log data dataframe

    Returns:
        df_log_data: log data dataframe
    """
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t: datetime.fromtimestamp(t / 1000.0))
    df_log_data = df_log_data.withColumn('timestamp', get_timestamp(df_log_data.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(x))
    df_log_data = df_log_data.withColumn('start_time', get_datetime(df_log_data.timestamp))
    return df_log_data


def create_spark_session():
    """
    This function initializes spark session with correct configuration.

    Returns:
        spark: Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def main():
    """
    This function orchestrates ETL process.
    It reads in the configs and delegates processing to relevant methods for song data and log data.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_ACCESS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()

    input_data = config['INPUT']['INPUT_ROOT_PATH']
    output_data = config['OUTPUT']['OUTPUT_ROOT_PATH']

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
