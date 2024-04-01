from neo4j import GraphDatabase, basic_auth
from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.DEBUG
)

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "1234567890"))

consumer = KafkaConsumer('twitter-sentiment', bootstrap_servers=['localhost:9092'])

try:
    for msg in consumer:
        try:
            sentiment_data = json.loads(msg.value.decode('utf-8'))
            sentiment = sentiment_data["sentiment"]
            tweet_text = sentiment_data["text"]

            with driver.session() as session:
                session.run("MATCH (t: Tweet {text: $text}) SET t.sentiment = $sentiment", text=tweet_text, sentiment=sentiment)

            logging.info(f"Updated sentiment for tweet text: {tweet_text}")
        except Exception as e:
            logging.error(f"Error processing message: {msg}. Error: {e}")
except Exception as e:
    logging.error(f"Error consuming messages from Kafka: {e}")
finally:
    consumer.close()
    driver.close()