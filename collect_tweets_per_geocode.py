# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 11:32:02 2016

@author: Samer Aamar
"""

#%%

import configparser

CONF_INI_FILE = 'c:/data/conf.ini'

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

def load_config(section='DEFAULT'):
    config = configparser.ConfigParser()
    config.read(CONF_INI_FILE)
    
    
    default = config['DEFAULT']
    host = default['mongodb_host']
    port = default['mongodb_port']    
    
    default = config[section]
    consumer_key = default['consumer_key']
    consumer_secret = default['consumer_secret']
    access_key = default['access_key']
    access_secret = default['access_secret']

    
    return consumer_key, consumer_secret, access_key, access_secret, host, port

#%%

import tweepy, datetime, time
from pymongo import MongoClient
from tweepy import TweepError

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




def collect_tweets_timeline(api, geocode, since, until, collection):
    c = 0
    
    # fnid the minimum we have and give it as the maximum
    minid = collection.find_one({'$query':{},'$orderby':{'_id': 1}}) 
    if minid != None:
        minid = minid['_id']
    
    maxid = collection.find_one({'$query':{},'$orderby':{'_id': -1}}) 
    if maxid != None:
        maxid = maxid['_id']
    
    print('Range id: ', minid, '-', maxid)

    #refer to this site for more details
    #https://dev.twitter.com/rest/reference/get/search/tweets

    for status in tweepy.Cursor(api.search,
                           #locations= boundingboxes['Europe'],
                           #languages = ['en'],
                           #q="lang:en since:"+since+" until:"+until+" include:retweets",
                           #q='since:'+since+' until:'+until+' include:retweets', # near:"Italy" within:500mi',
                           #q='geocode:"37.781157,-122.398720,1mi" since:'+since+' until:'+until+' include:retweets',
                           q='geocode:"'+geocode+'" since:'+since+' until:'+until+' include:retweets',
                           #q='near:"Italy" within:15mi '
                           #since="2016-08-23", 
                           #until="2016-08-24",
                           count=300,
                           #geocode="Italy",
                           #within="500mi",
                           max_id=minid,
                           result_type='recent',
                           include_entities=True,
                           monitor_rate_limit=False, 
                           wait_on_rate_limit=False).items(300):
        #print(status._json)
                           
        tweet = {'_id': status.id, 'json':status._json}
        #collection.update_one({'_id': status.id}, {'$set': tweet})
        try:
            collection.insert_one( tweet )
        except Exception as e:
            print(e)
        c+=1
        if c%100==0:
            print (c)
    print (c)


switch=0
USERS=[ 'USER1', 'USER2', 'USER3', 'USER4',  'USER6' ]
errors = [0]*len(USERS)
apis = [None]*len(USERS)

consumer_key, consumer_secret, access_key, access_secret, host, port =load_config(USERS[switch])

if apis[switch] == None:
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    #
    apis[switch] = tweepy.API(auth)

#client = MongoClient("mongodb://"+host+":"+port)
client = MongoClient(host, int(port))
db = client['twitter_db']

#geocode="42.6950869,13.2506592,300mi"  #Accumoli , Italy
geocode = "31.3435979,35.6433166,200mi" # Israel
        
LABEL = 'Israel' 
since = '2016-09-05'
until = '2016-09-06'
collection = db[since+'-'+until+'-'+LABEL]




while(True):
    try:
        print('User: ', USERS[switch], end=", ")

        collect_tweets_timeline(apis[switch], geocode , since, until, collection)
            
    except TweepError as te:
        if str(te)[-3:] == '429':
            
            errors[switch] = time.time()
            if abs(errors[-1] - errors[0]) < 2:
                print('Too much failures... go to sleep! ', time.ctime())
                time.sleep(60*3)
                errors = [0]*len(USERS)
            
            print('.', end="")
            # need to implement a fallback plan (use a different user)
            switch = (switch+1) % len(USERS)
            
            
            consumer_key, consumer_secret, access_key, access_secret, host, port =load_config(USERS[switch])

            if apis[switch] == None:            
                auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
                auth.set_access_token(access_key, access_secret)
                #
                apis[switch] = tweepy.API(auth)
        else:
            print (te)
            raise
            


#
#for tweet in tweepy.Cursor(api.user_timeline,id='USATODAY').items():
#    collection.insert(tweet._json)
#
#get_tweets_for_user(api, "username")



