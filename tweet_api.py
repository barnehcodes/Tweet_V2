from flask import Flask, redirect, render_template, request, jsonify, send_from_directory, session

from kafka import KafkaProducer
from neo4j import GraphDatabase, basic_auth
import json
from functools import wraps
from textblob import TextBlob
import pymongo
import re

from recommendation_system import collaborative_filtering_recommendation, content_based_filtering_recommendation
from neo4j_utils import get_hashtags_tweets_dict

app = Flask(__name__)
app.secret_key = b'\xcc^\x91\xea\x17-\xd0W\x03\xa7\xf8J0\xac8\xc5'


uri = "bolt://localhost:7687"

driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "1234567890"))

bootstrap_servers = ['localhost:9092']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Database
client = pymongo.MongoClient('localhost', 27017)
db = client.twitter

# Decorators
def login_required(f):
  @wraps(f)
  def wrap(*args, **kwargs):
    if 'logged_in' in session:
      return f(*args, **kwargs)
    else:
      return redirect('/')
  
  return wrap

# Routes
from user import routes

@app.route('/')
def home():
  return render_template('home.html')

@app.route('/dashboard/')
@login_required
def index():
    return render_template('dashboard.html')

@app.route('/dashboard/new_tweet', methods=['POST'])
def new_tweet():
    # Parse the new tweet data from the request
    tweet_text = request.form.get('text')
    username = request.form.get('username')

    # Check if the 'text' parameter is null or missing
    if tweet_text is None:
        return jsonify({'error': 'Text parameter is missing or null'}), 400

    # Extract hashtags and mentions from the tweet text
    hashtags = re.findall(r'#(\w+)', tweet_text)
    mentions = re.findall(r'@(\w+)', tweet_text)

    # Insert the new tweet data into the graph database
    with driver.session() as session:
        session.run("""
            MERGE (t:Tweet {text: $text})
            SET t.usernames = COALESCE(t.usernames, []) + $username
            SET t.hashtags = COALESCE(t.hashtags, []) + $hashtags
            SET t.mentions = COALESCE(t.mentions, []) + $mentions
        """, text=tweet_text, username=username, hashtags=hashtags, mentions=mentions)
        result = session.run("MATCH (t:Tweet {text: $text}) RETURN t", text=tweet_text)
        tweet = result.single()[0]
        print(f"Updated tweet: {tweet}")

    # Your remaining code...

    # Stream the new tweet data to the Kafka producer
    message = {
        "text": tweet_text,
        "hashtags": hashtags,
        "usernames": [username],
        "sentiment": None
    }
    value_bytes = json.dumps(message, ensure_ascii=False).encode('utf-8')
    producer.send('twitter-text', value=value_bytes)

    for hashtag in hashtags:
        producer.send('twitter-hashtags', value=hashtag.encode('utf-8'))

    for mention in mentions:
        producer.send('twitter-usernames', value=mention.encode('utf-8'))

    # Perform sentiment analysis on the new tweet data
    blob = TextBlob(tweet_text)
    sentiment = blob.sentiment.polarity

    # Stream the sentiment data to the Kafka producer and update the sentiment data in the graph database
    producer.send('twitter-sentiment', value=json.dumps({"sentiment": sentiment}).encode('utf-8'))
    with driver.session() as session:
        session.run("""
            MATCH (t:Tweet {text: $text})
            SET t.sentiment = $sentiment
        """, text=tweet_text, sentiment=sentiment)

    # Get the recommended hashtags and users based on the new tweet data
    hashtags_tweets_dict = get_hashtags_tweets_dict()
    similar_tweets = content_based_filtering_recommendation(hashtags[0], hashtags_tweets_dict)
    recommended_users = collaborative_filtering_recommendation(username)

    # Return the recommended hashtags, users, and sentiment as a response
    response = {
        "similar_tweets": similar_tweets if similar_tweets else [],
        "recommended_users": recommended_users if recommended_users else [],
        "sentiment": sentiment
    }
    return jsonify(response)

# if __name__ == '__main__':
#     app.run(debug=True, port=5001)