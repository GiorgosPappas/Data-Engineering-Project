

#import libraries 
import snscrape.modules.twitter as sntwitter
import pandas as pd
import numpy 
import time 
from transformers import AutoTokenizer,AutoModelForSequenceClassification
from scipy.special import softmax
from torch import  Tensor
import config
#options
pd.set_option('display.max_columns', 20)


#code
def get_sentiments(tweet_text): 
    '''Clean , prepare  and sentiment analysis'''
    tweet_context = []
    for word in tweet_text.split(" "):
        if word.startswith('@') :
            word = '@user'
        elif word.startswith('http'):
            word = 'http'
        tweet_context.append(word)
    #clean tweet turns mentions and links to simple user and http for the model
    clean_tweet = " ".join(tweet_context)

#roBERTa nlp model load (from Meta AI)
    roberta = "cardiffnlp/twitter-roberta-base-sentiment"

    model = AutoModelForSequenceClassification.from_pretrained(roberta)

    tokenizer = AutoTokenizer.from_pretrained(roberta)

    labels = ['Negative' , 'Neutral', 'Positive']
   
    encoded_tweet = tokenizer(clean_tweet,return_tensors = 'pt')
# returns encoded tweet with pytorch, basically turns the tweet into numbers for the softmax 
    
    output = model(**encoded_tweet)

    scores = output[0][0].detach()
    #softmax calculates propabilities
    scores  = softmax(scores)

    for i in range(len(scores)):

        max_score = 0
        label = ''
        l = labels[i]
        s = scores[i]
        if max_score < s :
            max_score = s
            label = l 
    return (label,str(max_score))



def get_tweets_create_df(ti):
  try:
    tweets = []
    # we query for every big tech company see config file.
    for company in config.target_companies:
        query = f'{company} stocks'
        company_counter = 0 

        for tweet in sntwitter.TwitterSearchScraper(query = query).get_items():
#we get 5 tweets per company 
            if  company_counter > 4:
                break
            else: 

                    tweets.append([tweet.id, tweet.date, tweet.user.username, tweet.rawContent, tweet.replyCount,tweet.retweetCount ,tweet.likeCount, tweet.quoteCount,tweet.viewCount,company,tweet.lang ,get_sentiments(tweet.rawContent)])
                    company_counter += 1
#make the dataframe             
    df_tweeter = pd.DataFrame(tweets, columns=['id','date','tweeter_user','tweet','reply_count','retweet_count','like_count','quote_count','view_count','company_name','language','sentiment'])
    ti.xcom_push(key = "tweets", value = df_tweeter.to_json())
  
  except :
    ti.xcom_push(key= "tweets",value = "Snscrape bug with twitter API")
