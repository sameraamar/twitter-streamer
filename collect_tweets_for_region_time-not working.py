# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 11:32:02 2016

@author: Samer Aamar
"""

#%%

import configparser

CONF_INI_FILE = 'c:/temp/conf.ini'

def read_conf(section='DEFAULT'):
    #conf.ini should look like this (in c:/temp folder)
    #------------------
    #[DEFAULT]
    #consumer_key = <key>
    #consumer_secret = <secret>
    #access_key = <key>
    #access_secret = <secret>
    #
    #; default is localhost:27017 for mongodb
    #mongodb_host = localhost
    #mongodb_port = 27017
    #------------------
    
    config = configparser.ConfigParser()
    config.read(CONF_INI_FILE)
    
    user = config[section]
    consumer_key = user['consumer_key']
    consumer_secret = user['consumer_secret']
    access_key = user['access_key']
    access_secret = user['access_secret']
    
    default = config['DEFAULT']
    host = default['mongodb_host']
    port = default['mongodb_port']
    
    return consumer_key, consumer_secret, access_key, access_secret, host, port

#%%

from pymongo.errors import BulkWriteError
from tweepy import TweepError
import tweepy, datetime, time
from pymongo import MongoClient
from pprint import pprint

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

def bulk_execute(bulk):
    
    werrors = {}
    try:
        result = bulk.execute()
        #pprint(result)
        #print('saving tweets....')
        #all is good
    except BulkWriteError as bwe:
        werrors = bwe.details['writeErrors']
        print('There are ', len(werrors), ' Errors!')
        return False
        
    return True


def collect_tweets_timeline(geocode, since, until, maxid, collection):
    c = 0

    query = 'geocode:"'+ geocode+'" include:retweets'
    query = 'geocode:"37.781157,-122.398720,1mi" since:'+since+' until:'+until+' include:retweets'
        
    bulk = collection.initialize_unordered_bulk_op()
    
    
    for status in tweepy.Cursor(api.search,
                           #locations= boundingboxes['Europe'],
                           #languages = ['en'],
                           #q="lang:en since:"+since+" until:"+until+" include:retweets",
                           #q='since:'+since+' until:'+until+' include:retweets', # near:"Italy" within:500mi',
                           #q='geocode:"37.781157,-122.398720,1mi" since:'+since+' until:'+until+' include:retweets',
                           q='geocode:"42.6950869,13.2506592,300mi" since:'+since+' until:'+until+' include:retweets',
                           #q='near:"Italy" within:15mi '
                           #since="2016-08-23", 
                           #until="2016-08-24",
                           #count=100000,
                           #geocode="Italy",
                           #within="500mi",
                           result_type='recent',
                           include_entities=True,
                           monitor_rate_limit=True, 
                           wait_on_rate_limit=True).items(100):
    
#    for status in tweepy.Cursor(api.search,
#                           #locations= boundingboxes['Europe'],
#                           #languages = ['en'],
#                           #q="lang:en since:"+since+" until:"+until+" include:retweets",
#                           #q='since:'+since+' until:'+until+' include:retweets', # near:"Italy" within:500mi',
#                           #q='geocode:"37.781157,-122.398720,1mi" since:'+since+' until:'+until+' include:retweets',
#                           q='geocode:"42.9207731,12.5352991,50mi" since:'+since+' until:'+until+' include:retweets',
#                           #q='geocode:"'+ geocode +'" since:'+since+' until:'+until+' include:retweets',
#                           #q='near:"Italy" within:15mi '
#                           #since="2016-08-23", 
#                           #until="2016-08-24",
#                           #count=100000,
#                           #geocode="Italy",
#                           #within="500mi",
#                           #max_id=maxid,
#                           result_type='recent',
#                           include_entities=True,
#                           monitor_rate_limit=False, 
#                           wait_on_rate_limit=False
#                           #,
#                           #lang="en"
#                           ).items(1000):
        #print(status._json)
        #collection.insert_one({'_id': status.id, 'json':status._json})
        tweet = {'_id': status.id, 'json':status._json}
        #bulk.find({'_id': status.id}).replace_one(tweet)
        bulk.insert(tweet)                
        c+=1

        if c%100==0:
            bulk_execute(bulk)
            bulk = collection.initialize_unordered_bulk_op()
            print (c)
      
        
    print ('Total: ', c)


switch=0
USERS=[ 'USER1', 'USER2', 'USER3', 'USER4', 'USER5' ]

consumer_key, consumer_secret, access_key, access_secret, host, port = read_conf(section=USERS[switch])

#client = MongoClient("mongodb://"+host+":"+port)
client = MongoClient(host, int(port))
db = client['twitter_db']

since = '2016-08-23'
until = '2016-08-24'
collection = db[since+'-'+until+'a']


while(True):
    try:
        consumer_key, consumer_secret, access_key, access_secret, ignore1, ignore2 = read_conf(section=USERS[switch])
        
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_key, access_secret)
        api = tweepy.API(auth)
        
        
        # fnid the minimum we have and give it as the maximum
        maxid = collection.find_one({'$query':{},'$orderby':{'_id': 1}}) 
        if maxid != None:
            maxid = maxid['_id']
    
    #    boundingboxes = {}
    #    boundingboxes['NYC'] = [-74.0853, 40.3924, -73.5052, 40.5247]
    #    boundingboxes['Israel'] = [34.0600,29.2100,35.5500,33.2500]
    #    coord['Syria'] = [35.1100,32.1600,42.3400,37.2000]
    #    boundingboxes['Saudi'] = [34.2600,12.5600,60.0900,31.2800]
    #    boundingboxes['Europe'] = [-11.5100,36.4800,26.3200,54.3700]
        
        coord = {}
        coord['Italy'] = "42.9207731,12.5352991,500mi"
        coord['Syria'] = "35.1100,32.1600,50mi"
        
        #print(time.ctime(), end=" ")
        collect_tweets_timeline(since, coord['Italy'], until, maxid, collection)
        
    except TweepError as te:
        if str(te)[-3:] == '429':
            print('.', end="")
            # need to implement a fallback plan (use a different user)
            n = len(USERS)+1
            switch = (switch+1) % n
        else:
            print (te)
            


#%%

maxid = collection.find_one({'$query':{},'$orderby':{'_id':-1}}) ['_id']
minid = collection.find_one({'$query':{},'$orderby':{'_id': 1}}) ['_id']

print('Range: ',  minid, '-', maxid )


#
#for tweet in tweepy.Cursor(api.user_timeline,id='USATODAY').items():
#    collection.insert(tweet._json)
#
#get_tweets_for_user(api, "username")







