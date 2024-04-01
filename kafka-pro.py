from kafka import KafkaProducer
from neo4j import GraphDatabase, basic_auth
import json
from textblob import TextBlob

uri = "bolt://localhost:7687"

driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "1234567890"))

bootstrap_servers = ['localhost:9092']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

with driver.session() as session:
    result = session.run("MATCH (t:Tweet) RETURN t.text, t.hashtags, t.usernames")
    count = 0
    for record in result:
        text = record["t.text"]
        hashtags = record["t.hashtags"]
        usernames = record["t.usernames"]
        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity
        message = {
            "text": text,
            "hashtags": hashtags,
            "usernames": usernames,
            "sentiment": sentiment
        }
        value_bytes = json.dumps(message, ensure_ascii=False).encode('utf-8')
        producer.send('twitter-text', value=value_bytes)
        if hashtags is not None:
            for hashtag in hashtags:
                producer.send('twitter-hashtags', value=hashtag.encode('utf-8'))
        if usernames is not None:
            for username in usernames:
                producer.send('twitter-usernames', value=username.encode('utf-8'))

        # Publish sentiment data to the 'twitter-sentiment' topic
        producer.send('twitter-sentiment', value=json.dumps({"sentiment": sentiment}).encode('utf-8'))

        result = session.run("MATCH (t:Tweet {text: $text}) SET t.sentiment = $sentiment", text=text, sentiment=sentiment)
        count += 1
        print(f"Sent message {count}: {message}")

producer.flush()
producer.close()
driver.close()