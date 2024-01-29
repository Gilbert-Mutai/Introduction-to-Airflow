import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set the Spark local IP address
os.environ['SPARK_LOCAL_IP'] = '192.168.1.104'

# Create Spark session
spark = SparkSession.builder \
    .appName("Python Spark First ETL Project") \
    .config('spark.jars', '/usr/share/java/postgresql-42.7.1.jar') \
    .getOrCreate()

def extract_movies_to_df():
    # Read database connection details from environment variables
    url = os.getenv("DB_URL")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    # Read movies DataFrame from the database
    movies_df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "movies") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    return movies_df

def extract_users_to_df():
    # Read database connection details from environment variables
    url = os.getenv("DB_URL")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    # Read users DataFrame from the database
    users_df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "users") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    return users_df

def transform_avg_ratings(movies_df, users_df):
    # Transforming tables to calculate average ratings
    avg_rating = users_df.groupBy("movie_id").mean("rating")
    df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)
    df = df.drop("movie_id")
    return df

def load_df_to_db(df):
    # Read database connection details from environment variables
    url = os.getenv("DB_URL")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    # Write DataFrame to the database
    mode = "overwrite"
    properties = {"user": user, "password": password, "driver": "org.postgresql.Driver"}

    df.write.jdbc(url=url, table="avg_ratings", mode=mode, properties=properties)

if __name__ == "__main__":
    # Extract movies and users DataFrames
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()

    # Transform tables to calculate average ratings
    ratings_df = transform_avg_ratings(movies_df, users_df)

    # Load the transformed DataFrame to the database
    load_df_to_db(ratings_df)
