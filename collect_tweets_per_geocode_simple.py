# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 11:32:02 2016

@author: Samer Aamar
"""

#%%

import configparser

CONF_INI_FILE = 'c:/temp/conf.ini'

#conf.ini should look like this (in c:/temp folder)
#[DEFAULT]
#consumer_key = <key>
#consumer_secret = <secret>
#access_key = <key>
#access_secret = <secret>
#
#; default is localhost:27017 for mongodb
#mongodb_host = localhost
#mongodb_port = 27017

config = configparser.ConfigParser()
config.read(CONF_INI_FILE)

default = config['USER1']
consumer_key = default['consumer_key']
consumer_secret = default['consumer_secret']
access_key = default['access_key']
access_secret = default['access_secret']

default = config['DEFAULT']
host = default['mongodb_host']
port = default['mongodb_port']

#%%

import tweepy, datetime, time
from pymongo import MongoClient
import time, pymongo

def get_tweets_for_user(api, username):
    page = 1
    deadend = False
    while True:
        tweets = api.user_timeline(username, page = page)

        for tweet in tweets:
            if (datetime.datetime.now() - tweet.created_at).days < 1:
                #Do processing here:

                print (tweet.text.encode("utf-8"))
            else:
                deadend = True
                return
        if not deadend:
            page+=1
            time.sleep(500)




def collect_tweets_timeline(since, until, collection):
    c = 0

        
    
    for status in tweepy.Cursor(api.search,
                           #locations= boundingboxes['Europe'],
                           #languages = ['en'],
                           q='geocode:"42.9207731,12.5352991,50mi" since:'+since+' until:'+until+' include:retweets',
                           #since="2016-08-23", 
                           #until="2016-08-24",
                           #count=100000,
                           #geocode="Italy",
                           #within="500mi",
                           result_type='recent',
                           include_entities=True,
                           monitor_rate_limit=True, 
                           wait_on_rate_limit=True
                           #,
                           #lang="en"
                           ).items(100):
        #print(status._json)
        collection.insert_one({'_id': status.id, 'json':status._json})
        c+=1
        if c>10:
            print (c)
    print (c)


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
#
api = tweepy.API(auth)

#client = MongoClient("mongodb://"+host+":"+port)
client = MongoClient(host, int(port))
db = client['twitter_db']

since = '2016-08-24'
until = '2016-08-25'
collection = db[since+'-'+until+'-tmp']

collect_tweets_timeline(since, until, collection)


#
#for tweet in tweepy.Cursor(api.user_timeline,id='USATODAY').items():
#    collection.insert(tweet._json)
#
#get_tweets_for_user(api, "username")


