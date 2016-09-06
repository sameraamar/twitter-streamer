
from tweepy.models import Status
from tweepy.models import Relationship
from tweepy.streaming import StreamListener
from tweepy.api import API
import json
from tweepy.error import TweepError
#import logging
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import Cursor
import configparser
#import sys

LABEL = 'NYC'
MAX_BULK_SIZE = 100000

MODE=0  # 0 is a file, 1 is mongodb

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

def load_config(user='DEFAULT'):
    config = configparser.ConfigParser()
    config.read(CONF_INI_FILE)
    
    
#    conf1 = {}
#    for section in config.sections():
#        keys = {}
#        for k in config.items(section)        :
#            keys[k[0]] = k[1]
#        conf1[section] = keys
#    
    conf = {}
    
    default = config['DEFAULT']
    host = default['mongodb_host']
    port = int ( default['mongodb_port']  )
    
    conf['host'] = host
    conf['port'] = port 
    
    
    
    default = config[user]
    keys = {}
    
    keys['consumer_key'] = default['consumer_key']
    keys['consumer_secret'] = default['consumer_secret']
    keys['access_key'] = default['access_key']
    keys['access_secret'] = default['access_secret']

    conf[user] = keys
    return keys, host, port

out = None
dbcoll = None
bulk = None
bulkcount = 0

#%%
from pymongo.errors import BulkWriteError
import codecs

def rollfile(n):
#    folder = conf.get('folder', '.')
#    label = conf.get('label', '.')
    folder = './' 
    label = LABEL

    nstr =  "%07d" % n
    filename = folder + LABEL + '/' + '/tweets' + label + '-' + nstr + '.json'
    output = codecs.open(filename, 'w', 'utf-8')
    return output

def saveTweet(json, text, count):
    if MODE ==0 :
        global out
        out.write(text) #.encode('utf8'))
        #out.write(text.replace('\n', '\\n')) #.encode('utf8'))
        if count % MAX_BULK_SIZE == 0:
            out.close()
            out = rollfile(count / MAX_BULK_SIZE)
    else:
        global bulk
        global LABEL
        global bulkcount

        if bulk== None:
             bulk = dbcoll.initialize_unordered_bulk_op()
             
        tweet = {}
        tweet['_id'] = json['id']
        if 'user' in json:
            tweet['screen_name'] = json['user']['screen_name']
        tweet['json'] = json
            
        tweet['status'] = 'Loaded'
        tweet['LABEL'] = LABEL
        
        res = bulk.insert(tweet)

        bulkcount+=1

        if bulkcount > 100:    
            werrors = {}
            try:
                bulkcount = 0
                result = bulk.execute() #dbcoll.insert_many(bulk)
                print('saving tweets....')
                #all is good
            except BulkWriteError as bwe:
                #print(bwe.details)
                werrors = bwe.details['writeErrors']
                #pass
            bulkcount = 0
            bulk = None
#            for e in werrors:
#                out.write( str(e['op']['_id']) )
#                out.write('\t' + e['errmsg'] + '\n' )       
#            #bulk = dbcoll.initialize_unordered_bulk_op()
            
            
#def hook(count):
#    if count % 100==0:
#        # start a new file
#        print ('need to split file!')        

class MyStreamListener(StreamListener):
    firsttime = True
    #output = sys.stdout
    #_api = None
    counter = 0

    def on_connect(self):
        """Called once connected to streaming server.

        This will be invoked once a successful response
        is received from the server. Allows the listener
        to perform some work prior to entering the read loop.
        """
        pass

    def on_data(self, raw_data):
        super(MyStreamListener, self).on_data(raw_data)
        """Called when raw data is received from connection.

        Override this method if you wish to manually handle
        the stream data. Return False to stop stream and close connection.
        """
        data = json.loads(raw_data)

        self.counter += 1
        msg = ''
        if 'user' not in data:
            msg += str(data)
        else:
            msg += str(self.counter) + " tweets: " + data['id_str'] + ": " + data['user']['screen_name']
            row = ''
    
            #text = json.dumps(data, cls=json.JSONEncoder)
            
            row = row + raw_data #.encode('utf-8')
                    
            saveTweet(data, row, self.counter)
    
            if 'retweeted_status' in data and data['retweeted_status'] != None:
                msg += "\tretweet of ..." + data['retweeted_status']['id_str']
                
            if 'quoted_status' in data and data['quoted_status'] != None:
                msg += "\tQUOTE of ..." + data['quoted_status']['id_str']                
                   
            if 'in_reply_to_status_id' in data and data['in_reply_to_status_id'] != None:
                 msg += "\tin reply to..." + str(data['in_reply_to_status_id'] )
        print(msg)
            
    def on_error(self, status):
        print (status)

#%%

keys, host, port = load_config('USER7')


if MODE == 1:
    #from datetime import datetime
    import pymongo
    from pymongo import MongoClient
    #import time
    
    #client = MongoClient()
    #client = MongoClient("mongodb://176.106.230.128:27017")
    client = MongoClient("mongodb://"+host+":"+str(port))
    
    db = client.streamer
    dbcoll = db.tweet_ids  #relevance_judgments

    dbcoll.insert_one({'_id' : -1})
    
    info = dbcoll.index_information()
    if not '_id_1' in info:
        dbcoll.create_index([("_id", pymongo.ASCENDING)], unique=True, background=True)
    if not 'status_1' in info:
        dbcoll.create_index([("status", pymongo.ASCENDING)], background=True)
    
    tmp = dbcoll.initialize_ordered_bulk_op()
    tmp.find( {'_id' : -1} ).remove()
    tmp.execute()
    tmp = None

#%%
import time
import traceback
import codecs



if __name__ == '__main__':
    
    if MODE == 0:
        out = rollfile(0)
        
    tries = 0

    
    l = MyStreamListener()
    #l.output = out
    auth = OAuthHandler(keys['consumer_key'], keys['consumer_secret'])
    auth.set_access_token(keys['access_key'], keys['access_secret'])
    api = API(auth, wait_on_rate_limit=False, wait_on_rate_limit_notify=False)
    #l._api = api

    
    stream = Stream(auth, l)
    while True:
        list_terms = ['kill', 'news' , 'fight', 'peace', 'elect', 'terror', 'earthquake', 'death', 'disaster', 'attack', 'major sports', 'shooting', 'crash', 'ISIS', 'PKK']

        
        try:
            #stream.filter(languages = ['en'], track=list_terms) 
            #http://boundingbox.klokantech.com/
            boundingboxes = {}
            boundingboxes['NYC'] = [-74.0853, 40.3924, -73.5052, 40.5247]
            boundingboxes['Israel'] = [34.0600,29.2100,35.5500,33.2500]
            boundingboxes['Syria'] = [35.1100,32.1600,42.3400,37.2000]
            boundingboxes['Saudi'] = [34.2600,12.5600,60.0900,31.2800]
            boundingboxes['Europe'] = [-11.5100,36.4800,26.3200,54.3700]
            
            boundingboxes['USA'] = [-93.4600, 24.3100, -71.5800 , 45.2000]
            
            stream.filter(locations= boundingboxes['NYC'] )
            #stream.filter(languages = ['en'], locations= boundingboxes['USA'] )

        
        except Exception as e:
#            print ("Error...tried (",tries,")", str(e))    
            print ("Error...", str(e))    
            traceback.print_exc()
#            time.sleep(20)
#            tries += 1
#            if tries > 10:
#                break
        
    #USA and europe
    # track=list_terms)
    if MODE==0:
        out.close()