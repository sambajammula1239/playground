
# coding: utf-8

# In[1]:


# import the module 
import tweepy 
import time
from datetime import datetime
from datetime import date
import pandas as pd
import yaml


# In[3]:


def authenticateTweetUser(tweetAccount):
    consumer_key = myKeys[tweetAccount]['apiKey']
    consumer_secret = myKeys[tweetAccount]['secretKey']
    access_token =  myKeys[tweetAccount]['AccessToken']
    access_token_secret =  myKeys[tweetAccount]['AccessTokenSecret']

    # authorization of consumer key and consumer secret 
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret) 

    # set access to user's access key and access secret 
    auth.set_access_token(access_token, access_token_secret) 

    # calling the api
    api = tweepy.API(auth)
    return api


# In[4]:


## Get Tweet Details
def getTweetDetails(api):
    tweets = tweepy.Cursor(api.search,
                  q=search_words,count=100000  ).items()
    SearchResultsList = [[tweet.user.screen_name, tweet.user.location,tweet.id,tweet.text] for tweet in tweets]
    return SearchResultsList


# In[7]:


def TweetAndReTweet(tweetAccount):
    global SearchResultsList
    api = authenticateTweetUser(tweetAccount)
    latestTweetId = api.user_timeline(count=1)[0].id
    SearchResultsList = getTweetDetails(api)
    LatestTweets=list(filter( lambda x : x[2] > latestTweetId , SearchResultsList))

    TweetUrl='https://twitter.com/{user_displayname}/status/{tweet_id}'
    for URL in list(map( lambda x : TweetUrl.format(user_displayname = x[0],tweet_id=x[2]) ,  LatestTweets)):
            TimeNow =  df.sample().values.tolist()[0][0] +  " #SaveAmaravati " + search_words + " " + str(datetime.now().strftime("%H%M%S"))
            URL2 = TimeNow + " " + URL
            print (URL2)
            try:
                api.update_status(URL2)
                print ("Posted Successfully!!")
            except tweepy.TweepError as e:
                print(e)
                if ( e.api_code == 186 ):
                    print("Reduce text Size")
                else:
                    raise
            time.sleep(10)


    for ID in list(map(lambda x : x[2] , LatestTweets)):
        try:
            api.retweet(ID)
            print("Retweeted : " , ID)
            time.sleep(10)
        except Exception as e:
            print(e)
            print("Failed Retweet : " , ID)
            #if ( e.api_code == 185 ):
            raise


# In[ ]:


## Pre Process
ProtestAge = date.today() - date(2019, 12, 17)
print(ProtestAge.days)
search_words = "#{Age}DaysOfAmaravatiProtests".format(Age=ProtestAge.days)
print(search_words)

## Load Twitter Data

from pandas_ods_reader import read_ods
path = r"C:\Users\samba\Amaravati-MasterTweets-Len.ods"
sheet_idx = 1
df = read_ods(path, sheet_idx)
sheet_name = "Final"
df = read_ods(path, sheet_name)

SearchResultsList=[]

with open(r"C:\Users\samba\TwitterApiKeys.yml", 'r') as stream:
    try:
        myKeys=yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
for i in range(1,10):
    for tweetAccount in myKeys:
        print("Started Tweeting for ",tweetAccount)
        try:
            TweetAndReTweet(tweetAccount)
        except tweepy.TweepError as e:
                    print(e)
        print("Completed Tweeting for ",tweetAccount)
        
    print("Sleeping for 60 Mins")
    time.sleep(1800)


# In[ ]:


## Stats Based on latest Search Results
df2 = pd.DataFrame(SearchResultsList)
df2.columns =['UserId','Location','TweetId',"Text"]
print("Total Tweets for ",search_words, df2
      .count())
df2[['Location','UserId']].groupby('UserId').count().sort_values(['Location'], ascending=False).head(20).rename(columns={'Location':'Total_Tweets'})

