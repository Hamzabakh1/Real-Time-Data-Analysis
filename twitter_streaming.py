import json
import tweepy
from kafka import KafkaProducer

# Twitter API credentials
consumer_key = 'C5eIXLNbmK6kpcmu6X8kCTyaV'
consumer_secret = 'lCdAQOGdpxSNhiROs2f8whAzVoFaM1wSdR5QAWeUS8ZZQw72r7'
access_token = '1793439898684293120-fB71wptljQR5nnEqTV0YjW7vnLZjvg'
access_token_secret = '4V3QXidoXCT669lxtKiA9C5Nt19CaTer71z99FFLwjMjm'
bearer_token = ('AAAAAAAAAAAAAAAAAAAAAGO5twEAAAAAU09baLVfcmz3S%2B09gYMhn'
                'lI0sB8%3DcF50GfU3Q1bHfEveMIMaMGWFkQbgNKZ2iNSMl8RZBm8GzylBUi')

# Set up Kafka producer
try:
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print("Kafka producer created successfully.")
except Exception as e:
    print(f"Failed to create Kafka producer: {e}")


# Define a Tweepy StreamListener to stream tweets to Kafka
class MyStreamListener(tweepy.StreamingClient):
    def on_data(self, raw_data):
        try:
            producer.send('twitter', json.loads(raw_data))
            print("Message sent to Kafka.")
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")
        return True

    def on_error(self, status_code):
        print(f"Error: {status_code}")
        return False


# Set up Tweepy client
try:
    client = tweepy.Client(bearer_token=bearer_token, consumer_key=consumer_key, consumer_secret=consumer_secret,
                           access_token=access_token, access_token_secret=access_token_secret)
    print("Twitter client created successfully.")
except Exception as e:
    print(f"Failed to create Twitter client: {e}")

# Add rules to filter tweets
try:
    stream_listener = MyStreamListener(bearer_token)
    print("Setting up rules.")
    rules = stream_listener.get_rules()
    if rules.data:
        rule_ids = [rule.id for rule in rules.data]
        stream_listener.delete_rules(rule_ids)

    stream_listener.add_rules(tweepy.StreamRule("python lang:en"))
    print("Rules set up successfully.")
except Exception as e:
    print(f"Failed to set up rules: {e}")

# Start streaming tweets
try:
    print("Starting stream listener.")
    stream_listener.filter()
except Exception as e:
    print(f"Failed to start stream listener: {e}")
