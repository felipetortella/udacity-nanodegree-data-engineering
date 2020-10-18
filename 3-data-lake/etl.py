import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, TimestampType, FloatType as Flt
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']


@udf(TimestampType())
def get_timestamp(ms):
    """
        Get timestamp from milliseconds
    """
    return datetime.datetime.fromtimestamp(ms / 1000)


def create_spark_session():
    """
        Create a Spark Session or return an existing one.
        This method use the AWS hadoop library version 2.7.0 to connect to Spark.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def read_json_file(spark, song_data):
    """
        Read all json files based on the song_schema and return a dataframe
    """
    song_schema = R([
        Fld("num_songs", Int(), nullable=True),
        Fld("artist_id", Str(), nullable=False),
        Fld("artist_latitude", Dbl(), nullable=True),
        Fld("artist_longitude", Dbl(), nullable=True),
        Fld("artist_location", Str(), nullable=True),
        Fld("artist_name", Str(), nullable=True),
        Fld("song_id", Str(), nullable=False),
        Fld("title", Str(), nullable=True),
        Fld("duration", Dbl(), nullable=True),
        Fld("year", Int(), nullable=True),
    ])

    # read song data file
    return spark.read.json(song_data, schema=song_schema)


def extract_songs(df, output_data):
    """
        Extract the songs from dataframe with selected columns and write to another s3 in parquet format
    """
    # extract columns to create songs table
    songs_columns = ["title", "artist_id", "year", "duration"]

    # write songs table to parquet files partitioned by year and artist
    songs_table = df.select(songs_columns).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    # songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs/')
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/', mode='overwrite')


def extract_artists(df, output_data):
    """
        Extract the artists from dataframe with selected columns and write to another s3 in parquet format
    """
    # extract columns to create artists table
    artists_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]

    # write artists table to parquet files
    artists_table = df.selectExpr(artists_columns).dropDuplicates()
    artists_table.write.parquet(output_data + 'artists/', mode='overwrite')


def process_song_data(spark, input_data, output_data):
    """
        Description: This method load the data (song) from S3 and them process it.
        The process extract the songs and artist tables.
        from S3 and them load it again to S3 in this new format using parquet files.

        Parameters:
            spark       : Spark Session
            input_data  : S3 bucket were original data is stored
            output_data : S3 bucket were parquet format data will be stored
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    df = read_json_file(spark, song_data)

    extract_songs(df, output_data)
    extract_artists(df, output_data)


def process_log_data(spark, input_data, output_data):
    """
        Description: This method load the data (log) from S3 and them process it.
        The process extract the songs and artist tables.
        from S3 and them load it again to S3 in this new format using parquet files.
        Data from previous method (process_song_data) is used in this method.

        Parameters:
            spark       : Spark Session
            input_data  : S3 bucket were original data is stored
            output_data : S3 bucket were parquet format data will be stored
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    staging_events_schema = R([
        Fld("artist", Str(), nullable=True),
        Fld("auth", Str(), nullable=True),
        Fld("firstName", Str(), nullable=True),
        Fld("gender", Str(), nullable=True),
        Fld("itemInSession", Int(), nullable=True),
        Fld("lastName", Str(), nullable=True),
        Fld("length", Flt(), nullable=True),
        Fld("level", Str(), nullable=True),
        Fld("location", Str(), nullable=True),
        Fld("method", Str(), nullable=True),
        Fld("page", Str(), nullable=True),
        Fld("registration", Str(), nullable=True),
        Fld("sessionId", Int(), nullable=True),
        Fld("song", Str(), nullable=True),
        Fld("status", Int(), nullable=True),
        Fld("ts", Dbl(), nullable=True),
        Fld("userAgent", Str(), nullable=True),
        Fld("userId", Str(), nullable=True),
    ])

    # read log data file
    df = spark.read.json(log_data, schema=staging_events_schema)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_columns = ["userId as user_id",
                     "firstName as first_name",
                     "lastName as last_name",
                     "gender",
                     "level"]
    users_table = df.selectExpr(users_columns).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/', mode='overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn("new_ts", get_timestamp("ts"))

    # extract columns to create time table
    time_columns = ["new_ts as start_time",
                    "hour(new_ts) as hour",
                    "day(new_ts) as day",
                    "weekofyear(new_ts) as week",
                    "month(new_ts) as month",
                    "year(new_ts) as year",
                    "weekday(new_ts) as weekday"]
    time_table = df.selectExpr(time_columns).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/', mode='overwrite')

    # read in song data to use for songplays table
    df_songs = spark.read.parquet(output_data + 'songs/*/*/*')
    df_artists = spark.read.parquet(output_data + 'artists/*')

    songplays_columns = [
        "new_ts as start_time",
        "userId as user_id",
        "level",
        "song_id",
        "artist_id",
        "sessionId as session_id",
        "location",
        "userAgent as user_agent",
        "year",
        "month"
    ]

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = (
        df.join(df_songs, df.song == df_songs.title)
        .drop("artist_id")
        .join(df_artists, df.artist == df_artists.artist_name)
        .join(time_table, df.new_ts == time_table.start_time)
        .filter("song_id is not null and artist_id is not null")
        .selectExpr(songplays_columns)
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/', partitionBy=["year", "month"], mode="overwrite")


def main():
    """
        This etl.py will ETL the data from S3, transform into dimensional
        tables and load it to S3 again in new format (parquet).
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    # input_data = "s3a://nnd-datalake-in/" # for testing with sample data
    output_data = "s3a://nnd-datalake-out/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
