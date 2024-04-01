import re
import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "1234567890"))

def insert_tweet(tx, row):
    """
    Inserts a tweet and its associated nodes and relationships into the graph.
    """
    # Create or merge Tweet node using tweet text as unique identifier
    tx.run("MERGE (t:Tweet {text: $text})", text=row['text'])

    # Create or merge User node
    user = row['user']
    if user and not pd.isnull(user):
        tx.run("MERGE (u:User {name: $name})", name=user)

        # FOLLOWED relationship between User and Tweet
        tx.run("""
            MATCH (u:User {name: $name})
            MATCH (t:Tweet {text: $text})
            MERGE (u)-[:POSTED]->(t)
        """, name=user, text=row['text'])

        # Add username to Tweet node
        tx.run("""
            MATCH (t:Tweet {text: $text})
            SET t.usernames = COALESCE(t.usernames, []) + $username
        """, text=row['text'], username=user)

    # Mentioned users and MENTIONED relationships
    mentions = row['mentions']
    if mentions and not pd.isnull(mentions):
        for mention in mentions.split(','):
            tx.run("MERGE (m:User {name: $name})", name=mention)

            # MENTIONED relationship between Tweet and Mentioned user
            tx.run("""
                MATCH (m:User {name: $name})
                MATCH (t:Tweet {text: $text})
                MERGE (t)-[:MENTIONED]->(m)
            """, name=mention, text=row['text'])

            # Add mentioned user to Tweet node
            tx.run("""
                MATCH (t:Tweet {text: $text})
                SET t.usernames = COALESCE(t.usernames, []) + $username
            """, text=row['text'], username=mention)

    # Hashtag nodes and TAGGED relationships
    for tag in re.findall(r'#\w+', row['text']):
        tx.run("MERGE (h:Hashtag {name: $name})", name=tag)

        # TAGGED relationship between Tweet and Hashtag
        tx.run("""
            MATCH (h:Hashtag {name: $name})
            MATCH (t:Tweet {text: $text})
            MERGE (t)-[:TAGGED]->(h)
        """, name=tag, text=row['text'])

        # Add hashtag to Tweet node
        tx.run("""
            MATCH (t:Tweet {text: $text})
            SET t.hashtags = COALESCE(t.hashtags, []) + $hashtag
        """, text=row['text'], hashtag=tag)

    # Link nodes and LINKED relationships
    links = row['links']
    if links and not pd.isnull(links):
        for link in links.split(','):
            tx.run("""
                MERGE (l:Link {url: $url})
                WITH l
                MATCH (t:Tweet {text: $text})
                MERGE (t)-[:LINKED]->(l)
            """, url=link, text=row['text'])

#================================================================

df = pd.read_csv("tweets_with_tags.csv")

#insertion
with driver.session() as session:
    for index, row in tqdm(df.iterrows(), total=len(df)):
        session.write_transaction(insert_tweet, row)

driver.close()