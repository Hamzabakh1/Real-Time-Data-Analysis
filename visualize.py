import plotly.express as px
import pandas as pd
from kafka import KafkaConsumer
import json

# Kafka consumer to read data from the topic
consumer = KafkaConsumer(
    'twitter',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Collect tweets and sentiments
tweets = []

for message in consumer:
    tweet = message.value
    tweets.append(tweet)

    # Convert list of tweets to DataFrame every 100 tweets
    if len(tweets) % 100 == 0:
        df = pd.DataFrame(tweets)
        fig = px.histogram(df, x='sentiment')
        fig.show()
        tweets = []  # Reset the list after visualization
