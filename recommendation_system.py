from neo4j_utils import get_users_tweets_dict, get_hashtags_tweets_dict
from neo4j import GraphDatabase, basic_auth
import logging

# Initialize the driver
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "1234567890"))

# Set the logging level to WARNING
logging.basicConfig(level=logging.WARNING)

def collaborative_filtering_recommendation(user_id):
    with driver.session() as session:
        result = session.run("MATCH (u:User {name: $user_id})-[:POSTED]->(t:Tweet)-[:TAGGED]->(h:Hashtag)<-[:TAGGED]-(t2:Tweet)-[:MENTIONED]->(u2:User) WHERE u <> u2 WITH u2, COUNT(DISTINCT h) AS common_hashtags ORDER BY common_hashtags DESC RETURN u2.name, common_hashtags LIMIT 10", user_id=user_id)
        records = list(result)
    return [(record['u2.name'], record['common_hashtags']) for record in records]

def content_based_filtering_recommendation(hashtag, hashtags_tweets):
    with driver.session() as session:
        result = session.run("MATCH (h:Hashtag {name: $hashtag})<-[:TAGGED]-(t:Tweet) WHERE t.sentiment IS NOT NULL RETURN t.text, t.sentiment ORDER BY t.sentiment DESC LIMIT 10", hashtag=hashtag)
        records = list(result)
    return [(record['t.text'], record['t.sentiment']) for record in records]

if __name__ == '__main__':

    users_tweets = get_users_tweets_dict()
    hashtags_tweets = get_hashtags_tweets_dict()

    result = None #we consume the result to avoid the ResultConsumedError

    usrs_recommendations = collaborative_filtering_recommendation("catvix")
    print(usrs_recommendations)
    
    print("================================================================================")
    
    tweet_recommendations = content_based_filtering_recommendation("#fb", hashtags_tweets)
    print(tweet_recommendations)