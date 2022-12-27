import tweepy
from confluent_kafka import Producer
from confluent_kafka import Producer,Consumer
import json
import pandas as pd
consumerKey = "Z0mSXqrutFlEUA1PqwCA7f98J"
consumerSecret = "2rEbiMaIVbZ5zphmTTGqeridgeLsT9560JhXUvTr9ZVQQqZIUJ"
accessToken = "1580974448789934082-eBzarmuIfXQ9cKXBV6p2EVFnJCewwW"
accessTokenSecret = "HY7OicSff88xGzh5aDeudnD2I4yzufu65vJqL7ka6M68K"
bearToken="AAAAAAAAAAAAAAAAAAAAACorigEAAAAApT9drqWQMtLOoRTYpl%2Ft7Veu550%3DNKmmcE7iHNtTJMHAJKexhEWGsBftg5Wh7m32iuMuDKQCgRuvvI"
producer =Producer({'bootstrap.servers':'localhost:9092'})

search_term = '#harassment -is:retweet lang:en'
topic_name = 'stream'

df=pd.DataFrame(columns=["text"])
List=[]
client = tweepy.Client(bearer_token=bearToken,consumer_key= consumerKey,consumer_secret= consumerSecret,access_token= accessToken,access_token_secret= accessTokenSecret)
    

# Replace the limit=1000 with the maximum number of Tweets you want
i=0
for tweet in tweepy.Paginator(client.search_recent_tweets, query=search_term,
                              tweet_fields=['context_annotations', 'created_at'], max_results=100).flatten(limit=10):
    producer.poll(1)
    producer.produce('stream', tweet.text)
    producer.flush()
    List.append(tweet.text)
df=pd.DataFrame(List,columns=["text"])
df.to_csv("harassements.csv")