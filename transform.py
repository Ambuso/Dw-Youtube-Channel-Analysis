import os
import psycopg2
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("YouTube Data Transformation") \
    .config("spark.jars", "/home/main/downloads/postgresql-42.7.3.jar") \
    .getOrCreate()

# Load JSON data
df_raw = spark.read.option("multiline", "true").json("/home/main/youtube/ambuso.json")
df_raw.createOrReplaceTempView("youtube_videos_raw")

# Transform raw data
sql_query = """
    SELECT
        videoId,
        title,
        publishedAt,
        CAST(viewCount AS INT) AS viewCount,
        CAST(likeCount AS INT) AS likeCount,
        CAST(commentCount AS INT) AS commentCount,
        extractionTimestamp
    FROM youtube_videos_raw
    WHERE viewCount IS NOT NULL
    ORDER BY viewCount DESC
"""
df_transformed = spark.sql(sql_query)
df_transformed.createOrReplaceTempView("youtube_transformed")

# Enrich data
enriched_query = """
    SELECT
        videoId,
        title,
        publishedAt,
        viewCount,
        likeCount,
        commentCount,
        extractionTimestamp,
        YEAR(TO_TIMESTAMP(publishedAt)) AS year,
        MONTH(TO_TIMESTAMP(publishedAt)) AS month,
        DAY(TO_TIMESTAMP(publishedAt)) AS day_of_month,
        DATE_FORMAT(TO_TIMESTAMP(publishedAt), 'EEEE') AS day_of_week,
        HOUR(TO_TIMESTAMP(publishedAt)) AS hour,
        WEEKOFYEAR(TO_TIMESTAMP(publishedAt)) AS week,
        CASE
            WHEN viewCount > 100000 THEN 'viral'
            WHEN viewCount > 5000 THEN 'high'
            ELSE 'normal'
        END AS performance_class
    FROM youtube_transformed
"""
df_enriched = spark.sql(enriched_query)
df_enriched.show()

# Create schema and table if needed
try:
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        sslmode=POSTGRES_SSLMODE
    )
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS dataengineering;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dataengineering.youtube_videos_enriched (
            videoId TEXT,
            title TEXT,
            publishedAt TIMESTAMP,
            viewCount INT,
            likeCount INT,
            commentCount INT,
            extractionTimestamp TIMESTAMP,
            year INT,
            month INT,
            day_of_month INT,
            day_of_week TEXT,
            hour INT,
            week INT,
            performance_class TEXT
        );
    """)
    cursor.close()
    conn.close()
    print("Schema and table checked or created.")
except Exception as e:
    print("Failed to create schema or table:", e)

# Write to PostgreSQL
jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
connection_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

df_enriched.write \
    .jdbc(
        url=jdbc_url,
        table="dataengineering.youtube_videos_enriched",
        mode="append",
        properties=connection_properties
    )

print("Enriched data successfully appended to PostgreSQL.")
