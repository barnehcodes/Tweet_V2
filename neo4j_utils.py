from neo4j import GraphDatabase, basic_auth

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "1234567890"))

def get_users_tweets_dict():
    with driver.session() as session:
        result = session.run("MATCH (u:User)-[:POSTED]->(t:Tweet) RETURN u.name, collect(DISTINCT t.text) as tweets ORDER BY u.name")
        users_tweets = {}
        for record in result:
            user = record['u.name']
            tweets = record['tweets']
            users_tweets[user] = tweets
        # Consume the result
        result.consume()
        return users_tweets

def get_hashtags_tweets_dict():
    with driver.session() as session:
        result = session.run("""
            MATCH (t:Tweet)
            WHERE size(t.hashtags) > 0
            RETURN t.text, t.hashtags
        """)
        hashtags_tweets_dict = {}
        for record in result:
            tweet_text = record["t.text"]
            hashtags = record["t.hashtags"]
            for hashtag in hashtags:
                if hashtag not in hashtags_tweets_dict:
                    hashtags_tweets_dict[hashtag] = []
                hashtags_tweets_dict[hashtag].append(tweet_text)
    return hashtags_tweets_dict