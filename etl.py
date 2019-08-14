from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import functions as F


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def to_date(ts):
    return datetime.fromtimestamp(ts / 1000)


@udf
def extract_hour(ts):
    return to_date(ts).hour


@udf
def extract_day(ts):
    return to_date(ts).day


@udf
def extract_week(ts):
    year, week_num, day_of_week = to_date(ts).isocalendar()
    return week_num


@udf
def extract_month(ts):
    return to_date(ts).month


@udf
def extract_year(ts):
    return to_date(ts).year


@udf
def extract_week_day(ts):
    year, week_num, day_of_week = to_date(ts).isocalendar()
    return day_of_week


def process_song_data(spark, input_data, output_data):
    df = spark.read.json(input_data)

    cleaned_songs = df.where(df.song_id.isNotNull()).dropDuplicates(['song_id'])

    songs_df = cleaned_songs \
        .withColumn('title', F.trim(cleaned_songs.title)) \
        .select(['song_id', 'title', 'artist_id', 'year', 'duration']) \
        .sort(cleaned_songs.title)

    artists_df = cleaned_songs.groupBy('artist_id').agg(
        F.first(df.artist_name).alias('name'),
        F.first(df.artist_location).alias('location'),
        F.first(df.artist_latitude).alias('latitude'),
        F.first(df.artist_longitude).alias('longitude')
    )

    artists_df = artists_df.withColumn('name', F.trim(artists_df.name))

    songs_df.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + '/songs.parquet')
    artists_df.write.mode('overwrite').parquet(output_data + '/artists.parquet')

    return songs_df, artists_df


def process_log_data(spark, input_data, output_data, songs_df, artists_df):
    df = spark.read.json(input_data)
    cleaned_log_data = df \
        .where(df.userId.isNotNull()) \
        .where(df.userId != '') \
        .where(df.page == 'NextSong') \
        .where(df._corrupt_record.isNull()) \
        .withColumn('song', F.trim(df.song)) \
        .withColumn('artist', F.trim(df.artist))

    time_df = cleaned_log_data.groupBy(cleaned_log_data.ts).count() \
        .drop('count') \
        .withColumn('hour', extract_hour(cleaned_log_data.ts)) \
        .withColumn('day', extract_day(cleaned_log_data.ts)) \
        .withColumn('week', extract_week(cleaned_log_data.ts)) \
        .withColumn('month', extract_month(cleaned_log_data.ts)) \
        .withColumn('year', extract_year(cleaned_log_data.ts)) \
        .withColumn('weekday', extract_week_day(cleaned_log_data.ts)) \
        .selectExpr('ts as start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    time_df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + '/time.parquet')

    joined_songs_df = songs_df.selectExpr('song_id', 'title as songs_df_title')
    joined_artists_df = artists_df.selectExpr('artist_id', 'name as artists_df_name')

    songplays_df = cleaned_log_data \
        .join(joined_songs_df, cleaned_log_data.song == joined_songs_df.songs_df_title) \
        .join(joined_artists_df, cleaned_log_data.artist == joined_artists_df.artists_df_name) \
        .join(time_df, cleaned_log_data.ts == time_df.start_time) \
        .withColumn('songplay_id', F.monotonically_increasing_id())

    songplays_df.selectExpr('songplay_id', 'ts as start_time', 'userId as user_id', 'level', 'song_id',
                            'artist_id', 'sessionId as session_id', 'location', 'userAgent as user_agent') \
        .write \
        .mode('overwrite') \
        .parquet(output_data + '/songplays.parquet')


def run(song_input_path, log_input_path, output_path):
    spark = create_spark_session()
    songs_df, artists_df = process_song_data(spark, song_input_path, output_path)
    process_log_data(spark, log_input_path, output_path, songs_df, artists_df)
