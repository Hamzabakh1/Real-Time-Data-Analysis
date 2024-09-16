from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Initialize Spark Session pdu des
spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()

# Définition du Schéma des Données
schema = StructType([
    StructField("text", StringType(), True),
    StructField("created_at", StringType(), True)
])

# Read data from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter") \
    .load()

# Convert the value column to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON data
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Setup NLTK for sentiment analysis
nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

def get_sentiment(text):
    sentiment = sia.polarity_scores(text)
    return sentiment

# Add sentiment analysis to the Spark DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, StringType, FloatType

sentiment_udf = udf(lambda text: get_sentiment(text), MapType(StringType(), FloatType()))

df = df.withColumn("sentiment", sentiment_udf(df["text"]))

# Write the data with sentiment to console (for testing purposes)
query = df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
