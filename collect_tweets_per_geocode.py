# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 11:32:02 2016

@author: Samer Aamar
"""

#%%

import configparser

CONF_INI_FILE = 'd:/data/conf.ini'

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
    
    print('     Range id: ', minid, '-', maxid)

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

    print('found: ', c)
    return c<=1  #we are done!


#%%
import sys


def load_arguments():
    param = {}
    for i in range(0, len(sys.argv)):
        if i % 2 == 1:
            param[sys.argv[i]] = sys.argv[i+1]
    print (param)
 
    geocode = param.get('-location', None)
    if geocode == None:
        raise Exception('wrong usage - <location> is missing')
        
    LABEL = param.get('-label', None)
    if LABEL == None:
        raise Exception('wrong usage - <label> is missing')

            
    return param


#%%
#*****
import random
import numpy as np

USERS=[ 'USER1', 'USER2', 'USER3', 'USER4',  'USER5',  'USER6',  'USER7',  'USER8',  'USER9' ]
switch = random.randint(0, len(USERS)-1)

#geocode="42.6950869,13.2506592,300mi"  #Accumoli , Italy
#geocode = "31.3435979,35.6433166,200mi" # Israel
geocode = "40.7060813,-73.7749913,15mi" #NYC

LABEL = 'NYC' 
since = '2016-09-27'
until = '2016-09-06'

param = load_arguments()

geocode = param['-location']
since = param['-since']
until = param['-until']
LABEL = param['-label']

#if len(sys.argv) == 9:
#    LABEL = sys.argv[2]
#    since = sys.argv[4]
#    until = sys.argv[6]
#    geocode = sys.argv[8]
#
#else:
#    print('wrong usage!')
#    print('-label <text> -since <yyyy-mm-dd> -until <yyyy-mm-yy> -geocode <long,lat,radius>')
#    print('Example:')
#    print('-label "Italy" -since "2016-09-01" -until "2016-09-01" -geocode "42.6950869,13.2506592,300mi"')
#    1/0  #halt
#    
#*****

print(param)


errors = [k*3 for k in range(len(USERS))]
apis = [None]*len(USERS)

consumer_key, consumer_secret, access_key, access_secret, host, port =load_config(USERS[switch])
host = param.get('-host', host)
port = param.get('-port', port)

#if apis[switch] == None:
#    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
#    auth.set_access_token(access_key, access_secret)
#    #
#    apis[switch] = tweepy.API(auth)
    #apis[switch] = tweepy.API(auth, retry_count=10, retry_delay=5, retry_errors=set([500]))

#client = MongoClient("mongodb://"+host+":"+port)
client = MongoClient(host, int(port))
db = client['streamer']

   

collection = db[since+'-'+until+'-'+LABEL]

connectagain = True


while(True):
    try:
        if connectagain:
            consumer_key, consumer_secret, access_key, access_secret, host, port =load_config(USERS[switch])
            host = param.get('-host', host)
            port = param.get('-port', port)
            
            connectagain = False
            if apis[switch] == None:            
                auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
                auth.set_access_token(access_key, access_secret)
                #
                #apis[switch] = tweepy.API(auth, retry_count=10, retry_delay=5, retry_errors=set([401, 404, 500, 503])) , wait_on_rate_limit=True)
    #            apis[switch] = tweepy.API(auth, retry_count=10, retry_delay=5, retry_errors=set([500]))
                apis[switch] = tweepy.API(auth)

                #apis[switch] = tweepy.API(auth)

        
        
        print('<', USERS[switch], end=', ')
        print(LABEL, end=', ')
        print(since, end=', ')
        print(until, end=', ')
        print(geocode, '>')
        #print('Label: ', LABEL, 'from: ', since, ' until: ', until, 'geocode: ', geocode, )
        #print('User: ', USERS[switch], end=', ')

        if collect_tweets_timeline(apis[switch], geocode , since, until, collection):
            print('Finished!')
            break
            
    except TweepError as te:
        if str(te)[-3:] == '429':
            
            errors[switch] = time.time()
            
            if abs(np.max(errors) - np.min( errors)) < len(USERS):  #give me len(USERS) seconds
                print('Too much failures... go to sleep 2 minutes! ', time.ctime(), te, str(te), errors)
                time.sleep(60*2)
                errors = [3*k for k in range(len(USERS))]
            else:
                print('Error: ', te)
            print('-------- switch to other user', end='[errors: ')
            print(np.max(errors), ',', np.min( errors), ', ', abs(np.max(errors) - np.min( errors)), ']')
            # need to implement a fallback plan (use a different user)
            switch = (switch+1) % len(USERS)
            
            connectagain = True
            
        else:
            print (te)
            n = 1
            if str(te)[0:43] == 'Failed to send request: HTTPSConnectionPool':
                n = 5
            print ("Tweepy error! go to sleep ",n," minutes")
            time.sleep(n*60)
            connectagain = True

        pass

    except Exception as e:
        print (e)
        print ("Some exception happened! go to sleep 5 minutes")
        time.sleep(5*60)


#
#for tweet in tweepy.Cursor(api.user_timeline,id='USATODAY').items():
#    collection.insert(tweet._json)
#
#get_tweets_for_user(api, "username")



