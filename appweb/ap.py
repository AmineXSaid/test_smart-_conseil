from flask import Flask, request, render_template, session, redirect
import numpy as np
import pandas as pd
import re
from detoxify import Detoxify
from pymongo import MongoClient

app = Flask(__name__)


def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = re.sub(r'\n\n\S+', '', str(tweet))
    tweet = tweet.strip('[link]')
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))
    tweet = re.sub('([0-9]+)', '', str(tweet))
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet



df=pd.read_csv("Tweets sentiment analysis ( kafka - pyspark )/harassements.csv")
df.drop("Unnamed: 0",axis=1,inplace=True)
df=pd.DataFrame(df.apply(lambda x : cleanTweet(x),axis=1),columns=['Text'])
result=df.apply(lambda x : Detoxify('original').predict(str(x)),axis=1)
dfx=pd.DataFrame(pd.Series(result))
for i in dfx[0] : 
    df['toxicity']=i['toxicity']
    df['severe_toxicity']=i['severe_toxicity']
    df['obscene']=i['obscene']
    df['threat']=i['threat']
    df['insult']=i['insult']
    df['identity_attack']=i['identity_attack']



@app.route('/', methods=("POST", "GET"))
def html_table():

    return render_template('simple.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)



if __name__ == '__main__':
    app.run(host='0.0.0.0')

