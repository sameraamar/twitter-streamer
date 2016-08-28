
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
#import nltk
#import sys


MODE=1  # 0 is a file, 1 is mongodb
LABEL = 'Europe-ME-20160824'

samer = {
'consumer_key': "15RlnMoVeVYMZZ7B75atfHoeT",
'consumer_secret': "C92Vv4p4DdeNpTtMMRgCB3SlZ2ZxGqhw3WKjniZluyCQFXbWWd",
'access_key': "743159365381787648-cviHOEuYncAL1DGnkiKx5DW5PkloSbi",
'access_secret': "f6vLHwcF5LJTKbAGoJWhYSAkOnF3m2r9KqPSwgT6QepTQ",
}


awael = {
'consumer_key': "GWsMkM2dqUzYJeo29HFAkkR9P",
'consumer_secret': "cRlCEDllvxnhxmDqQVY6cjBhXUsgeQIw2QjhM5ETJizRhl49Gf",
'access_key': "765638437219209218-0TrNekOne6QEgfsTQnR1SrXygXibW1H",
'access_secret': 'fUifaOe6tsG0bkzn9BWMbTfFUu2h5VFPdmRnYrpl8OYL3'
}


keys = awael

out = None
dbcoll = None
bulk = None
bulkcount = 0

#%%
from pymongo.errors import BulkWriteError
#import pymongo

def saveTweet(json, text):
    if MODE ==0 :
        global out
        out.write(text) #.encode('utf8'))
        out.write('\n')
    else:
        global bulk
        global bulkcount
        global LABEL
        
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
            
            
        

class MyStreamListener(StreamListener):
    firsttime = True
    #output = sys.stdout
    #_api = None
    counter = 0

#    def on_connect(self):
#        """Called once connected to streaming server.
#
#        This will be invoked once a successful response
#        is received from the server. Allows the listener
#        to perform some work prior to entering the read loop.
#        """
#        pass

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
                    
            saveTweet(data, row)
    
    
            if 'retweeted_status' in data and data['retweeted_status'] != None:
                msg += "\tretweet of ..." + data['retweeted_status']['id_str']
                
            if 'quoted_status' in data and data['quoted_status'] != None:
                msg += "\tQUOTE of ..." + data['quoted_status']['id_str']                
                   
            if 'in_reply_to_status_id' in data and data['in_reply_to_status_id'] != None:
                 msg += "\tin reply to..." + str(data['in_reply_to_status_id'] )
        print(msg)
            

#%%

if MODE == 1:
    #from datetime import datetime
    import pymongo
    from pymongo import MongoClient
    #import time
    
    #client = MongoClient()
    #client = MongoClient("mongodb://176.106.230.128:27017")
    client = MongoClient("mongodb://localhost:27017")
    
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
        out = codecs.open('tweets'+LABEL+'.json', 'w', 'utf-8')
        
    tries = 0

    while True:
    
        l = MyStreamListener()
        #l.output = out
        auth = OAuthHandler(keys['consumer_key'], keys['consumer_secret'])
        auth.set_access_token(keys['access_key'], keys['access_secret'])
        api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        #l._api = api

    
        stream = Stream(auth, l)
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
            
            #stream.filter(locations = boundingboxes['USA'] )
            stream.filter(locations= boundingboxes['Europe']+boundingboxes['Israel']+boundingboxes['Syria']+boundingboxes['Saudi'] )
            #stream.filter(languages = ['en'], locations= boundingboxes['Israel']+boundingboxes['Syria']+boundingboxes['Saudi'] )

        
        except Exception as e:
            print ("Error...tried (",tries,")", str(e))    
            traceback.print_exc()
            time.sleep(20)
            tries += 1
            if tries > 10:
                break
        
    #USA and europe
    # track=list_terms)
    if MODE==0:
        out.close()