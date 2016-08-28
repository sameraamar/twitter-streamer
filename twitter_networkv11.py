# -*- coding: utf-8 -*-
"""
Created on Sat Jun 18 06:11:42 2016

@author: SAMERA
"""

        
#%%

import json
from tail_f import follow
import traceback
from igraph_api import *
import igraph
import numpy
import time
import numpy as np
from heapq import merge
import re

#0 is Tweet mode and 1 is User mode
MODE = 0


numTweets = 1000000

MAX_BINS = 100
MINUTES = 10
MAX_DELTA = MINUTES*1000*60 #minutes

ID=0
TS=1
USR=2
RE2TW=3
RE2U=4
DT=5
TXT=6
HS_TG=7
LIKES=8
QT_ID=9
QT_TXT=10
QT_USR=11
USRS=12
URLS=13
MDIA=14
FLWs=15
FRNDs=16


#%%


from os import walk
from os.path import split, splitext, join, exists

def select_files(root, files, extensions=[]):
    """
    simple logic here to filter out interesting files based on extensions
    """

    selected_files = []

    for file in files:
        #do concatenation here to get full path 
        full_path = join(root, file)
        ext = splitext(file)[1]

        if ext in extensions:
            selected_files.append(full_path)

    return selected_files

def build_recursive_dir_tree(path, ext=[]):
    """
    path    -    where to begin folder scan
    """
    selected_files = []

    for root, dirs, files in walk(path):
        selected_files += select_files(root, files, extensions=ext)

    return selected_files
  

def tryint(s):
    try:
        return int(s)
    except:
        return s

def alphanum_key(s):
    """ Turn a string into a list of string and number chunks.
        "z23a" -> ["z", 23, "a"]
    """
    return [ tryint(c) for c in re.split('([0-9]+)', s) ]

def sort_nicely(l):
    """ Sort the given list in the way that humans expect.
    """
    newl = sorted(l, key=alphanum_key)
    return newl
    
#%%
    
#jsonfolder = 'Sensing trending topics in Twitter - tweets\\super_tuesday\\SuperTuesday_ids'
#filelist = build_recursive_dir_tree(jsonfolder, ['.json'])
#filelist = sort_nicely(filelist)
#
#folder = 'tweets-sensing2-users/'

filename = 'italy-earthquack-2'

suffix = 'users/'
if MODE==0:
    suffix = 'tweets/'    

folder = filename + '-'+str(MINUTES)+'min-'+suffix
filename += '.json'
filelist = [filename]


#%%

import datetime 

def gettimestamp(jsondata):
    timestamp = 0
    if 'timestamp_ms' in jsondata:
        timestamp = int(jsondata['timestamp_ms'])
    else:
        date = jsondata['created_at']
        date = date.replace('+0000 ', '')
        #Fri Jul 15 02:47:50 +0000 2016
        timestamp = 1000*time.mktime(datetime.strptime(date, "%c").timetuple())
        
    return int(timestamp)


#d = {}
#d["created_at"] = "Fri Jan 15 02:47:50 2021"
#d["created_at"] = "Thu Jul 14 23:20:04 +0000 2016"
#timestamp = gettimestamp(d)
#print(datetime.fromtimestamp(timestamp/1000))


#%%

def parse_json_tweet(tweet=None, line=None):
    if tweet == None :
        if line != None:
            tweet = json.loads(line,  encoding='utf-8')
        else:
            return None


    #print line
#    if 'lang' not in tweet or tweet['lang'] != 'en':
#     	#print "non-english tweet:", tweet['lang'], tweet
#     	return ['', '', '', [], [], [], [], [], []]

    date = tweet['created_at']
    favoriteCount = int(tweet["favorite_count"])
        
    timestamp = gettimestamp(tweet)
#    if 'timestamp_ms' in tweet:
#        timestamp = tweet['timestamp_ms']
#    else:
#        timestamp = fromutc(date)
        
    id = tweet['id_str']
    if 'screen_name' in tweet['user']:
        user_name = tweet['user']['screen_name']
    else:
        user_name = tweet['user']['id_str']
    
    nfollowers = 0
    if 'followers_count' in tweet['user']:
        nfollowers = tweet['user']['followers_count']
        
    nfriends = 0
    if 'friends_count' in tweet['user']:
        nfriends = tweet['user']['friends_count']

    reply_to_user = reply_to_tw = None
    if 'in_reply_to_status_id_str' in tweet and tweet['in_reply_to_status_id_str'] != None:
        #reply_to_user_id = jsondata['in_reply_to_user_id_str']
        reply_to_user = str(tweet['in_reply_to_screen_name'])
        reply_to_tw = tweet['in_reply_to_status_id_str']

    text = tweet['text']
    quote_id = quote_text = quoted_user = None
    if 'retweeted_status' in tweet:
        quote_text = tweet['retweeted_status']['text']
        if 'screen_name' in tweet['retweeted_status']['user']:
            quoted_user = tweet['retweeted_status']['user']['screen_name']
        else:
            quoted_user = tweet['retweeted_status']['user']['id_str']
        quote_id = tweet['retweeted_status']['id_str']

    elif 'quoted_status' in tweet:
        if str(tweet['quoted_status']['id_str']) != reply_to_tw:
            #print('both appear!', 1/0)
            #reply_to_user = tweet['quoted_status']['user']['screen_name']
            #reply_to_tw = str(tweet['quoted_status']['id_str'])
            
            quote_id = tweet['quoted_status']['id_str']
            if 'screen_name' in tweet['quoted_status']['user']:
                quoted_user = tweet['quoted_status']['user']['screen_name']
            else:
                quoted_user = tweet['quoted_status']['user']['id_str']
            quote_text = tweet['quoted_status']['text']
     

    media_urls = hashtags = users = urls = []
    if 'entities' in tweet:
        hashtags = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
        users = [user_mention['screen_name'] for user_mention in tweet['entities']['user_mentions']]
        urls = [url['expanded_url'] for url in tweet['entities']['urls']]
    
        if 'media' in tweet['entities']:
            media_urls = [media['media_url'] for media in tweet['entities']['media']]	    

    return [id, timestamp, user_name, reply_to_tw, reply_to_user, date, text, hashtags, favoriteCount, quote_id , quote_text , quoted_user, users, urls, media_urls, nfollowers, nfriends]
     
def scan():
    filename = 'tweets-NY5'
    file = open(filename + '.json', 'r', encoding='utf8')
    out  = open(filename + '.txt', 'w', encoding='utf8')
    
    for line in file:
        id, timestamp, user_name, reply_to_tw, reply_to_user, date, text, hashtags, favoriteCount, quote_id , quote_text , quoted_user, users, urls, media_urls, nfollowers, nfriends = parse_json_tweet(line=line)
        text = text.replace('\n', ' ')
        out.write(text)
        h = ' '.join(hashtags)
        out.write(h)
        out.write('\n')
        
    out.close()
    file.close()




#%%
def getAddNode(key, vLabels, indx):
    i = indx.get(key, None)
    if i == None:
        i = len(vLabels)
        vLabels.append(key)
        indx[key] = i
        
    return i

def networkUsers(structure):
    edges = []
    eLabels = []
    eWeights = []

    indx = {} #helper for performance considerations only
    vLabels = []
    for index, tweet_id in enumerate( structure.keys() ):
        tweet = structure[tweet_id]
        
        # (id, timestamp, user_name, reply_to_tw, reply_to_user, date, text, 1.0)
        reply_to = tweet[RE2TW]
        
        if reply_to == None:
            continue
        
#        reply_to = structure.get(tmp, None)
#        if reply_to == None:
#            reply_to = tmp #, tweet[4])
#            print ('**** Could not find tweet in the dataset - it is old one: ', tmp)

        key = tweet[USR]
#        i = indx.get(key, None)
#        if i == None:
#            i = len(vLabels)
#            vLabels.append(key)
#            indx[key] = i
        src = getAddNode(key, vLabels, indx) 
        
        key = tweet[RE2U]
#        i = indx.get(key, None)
#        if i == None:
#            i = len(vLabels)
#            vLabels.append(key)
#            indx[key] = i
        trgt = getAddNode(key, vLabels, indx) 



        edges.append((src, trgt))
        eLabels.append(tweet_id)
        eWeights.append(1.0)
        
    
    return structure, vLabels, eLabels, edges, eWeights
  
#%%
           
def network(structure):
#    if MODE == 1:
#        return networkUsers(structure)
    
    edges = []
    eLabels = []
    eWeights = []
    
    starttime = None

    indx = {} #helper for performance considerations only
    vLabels = []
    for index, tweet_id in enumerate( structure.keys() ):
        tweet = structure[tweet_id]
        
        # (id, timestamp, user_name, reply_to_tw, reply_to_user, date, text, 1.0)
        user_name = tweet[USR]        
        reply_to = tweet[RE2TW]
        reply_to_user = tweet[RE2U]
        
        if (reply_to == None and tweet[QT_ID] == None):
            continue
        
        if starttime==None:
            starttime = tweet[TS]
#        reply_to = structure.get(tmp, None)
#        if reply_to == None:
#            reply_to = tmp #, tweet[4])
#            print ('**** Could not find tweet in the dataset - it is old one: ', tmp)
        if MODE == 0:
            k = tweet_id
        else:
            k = user_name
        
        i = getAddNode(k, vLabels, indx) 
#        i = indx.get(k, None)
#        if i == None:
#            i = len(vLabels)
#            vLabels.append(k)
#            indx[k] = i

        if reply_to != None:
            if MODE == 0:
                k = reply_to
            else:
                k = reply_to_user
                
            j = getAddNode(k, vLabels, indx) 
    #        j = indx.get(k, None)
    #        if j == None:
    #            j = len(vLabels)
    #            vLabels.append(k)
    #            indx[k] = j
    
            edges.append((i, j))
            temp = 'REPLY ' + tweet_id
            if MODE == 0:
                temp = user_name + ' RPLY ' + reply_to_user       
            
            eLabels.append(temp)
            eWeights.append(1.0)

        if tweet[QT_ID] != None:
            if MODE == 0:
                k = tweet[QT_ID]
            else:
                k = tweet[QT_USR]
                
            j = getAddNode(k, vLabels, indx) 
    #        j = indx.get(k, None)
    #        if j == None:
    #            j = len(vLabels)
    #            vLabels.append(k)
    #            indx[k] = j
    
            edges.append((i, j))
            temp = 'QUOTE: ' + tweet_id
            if MODE == 0:
                temp = user_name + ' QUOTE ' + tweet[QT_USR]       
            
            eLabels.append(temp)
            eWeights.append(1.0)
        
    
    return structure, vLabels, eLabels, edges, eWeights, starttime

    
  
def buildGraph(structure):
    #structure, vLabels, eLabels, edges, eWeights = network(structure)
    structure, vLabels, eLabels, edges, eWeights, starttime = network(structure)
    
    g = igraph.Graph( edges, directed=True)
    g.es['weight'] = eWeights
    g.es['label'] = eLabels       
    g.vs['label'] = vLabels
    
    return g, starttime


def getNetwork(structure, jsondata):
    ## tweet to tweet
    id, timestamp, user_name, reply_to_tw, reply_to_user, date, text, hashtags, likes, quote_id , quote_text , quoted_user, users, urls, media_urls, nfollowers, nfriends = parse_json_tweet(tweet=jsondata)
           
    tup = (id, timestamp, user_name, reply_to_tw, reply_to_user, date, text, hashtags, likes, quote_id , quote_text , quoted_user)
    
    #if reply_to_tw != '' and reply_to_tw != None:
    #  reply_to_tw

    structure[id] = tup

    return structure


#%%
start = time.time()


#%%
import codecs
import webcolors as wc

def printGraphCSV(giant, structure, filename):
   
    out = codecs.open(filename + '.csv', 'wb', "utf-8")
    out.write('tweet_id\tscreen_name\treply_to_tweet\treply_to_user\tquote_id\tquote_user\tdate\ttext\tcomponent\n') #.encode('utf-8'))
    for e in range(giant.ecount()):
        #idstr = giant.es['label'][e]
        # (id, timestamp, user_name, reply_to_tw, reply_to_user, date, text, 1.0)
        #src = structure[idstr][3] #giant.vs['label'][giant.es[e].source]
        #trgt = structure[idstr][3] #giant.vs['label'][giant.es[e].target]
        
        i = giant.es[e].source
        j = giant.es[e].target
        
        if MODE == 0:
            tweetId = giant.vs['label'][i]
        else:
            tweetId = giant.es['label'][e]
            tweetId = tweetId.split()[1]
            
        tup = structure[tweetId]
        
        text = '' #str(giant.es[e].source) + '\t' + str(giant.es[e].target) + '\t'
        text += tweetId + '\t' + tup[USR] + '\t' 
        if tup[RE2TW] != None:
            text += tup[RE2TW] + '\t'
            text += tup[RE2U] + '\t' 
        else:
            text += '\t\t' 

        if tup[QT_ID] != None:
            text += tup[QT_ID] + '\t' 
            text += tup[QT_USR] + '\t' 
        else:
            text += '\t\t' 
        text += tup[DT] + '\t'
        text += tup[TXT].replace('\n', '\\n') 
        text += '\t' + graph.vs[i]["color"]
        text += '\n'
        out.write(text) #.encode('utf-8'))
    out.close()

def plotGraph(graph, filename, iLayout=None, elabel=True, vlabel=True):
    if iLayout == None:
        iLayout = graph.layout_fruchterman_reingold()
    
    eL = vL = None
    if elabel:
        eL = graph.es['label'] 
    if vlabel:
        vL = graph.vs['label'] 
        
    igraph.plot(graph , filename, bbox=[0,0,1200,1200], vertex_size=14,vertex_label=vL,edge_label=eL,layout=iLayout)
    
def generateGiantComponent(graph, filename, export_gephy=False):
    components = graph.components(mode=igraph.WEAK) #mode=igraph.STRONG)
    #giant = components.giant()   
    #
    colors = ["cyan", "magenta", "yellow", "blue", "green", "red", "lightgray"]
    
    #components = graph.components(mode=igraph.WEAK)

    sorted_components = sorted(components, key=len, reverse=True)
    for index, component in enumerate(sorted_components):
      color = colors[min(6, index)]
      for vidx in component: 
          graph.vs[vidx]["color"] = color

    cc = []
    for i in range(min(len(colors), len(sorted_components))):
        cc = cc + sorted_components[i]
    tmp = graph.subgraph(cc)

    tmpf = filename + '-LCCs.png'

    global MODE
    plotGraph(tmp, tmpf, vlabel=(MODE==1), elabel=False)
    
        
    #igraph.plot(giant , filename, vertex_size=20,vertex_label=None,edge_label=None,layout=layoutg)
    printGraphCSV(tmp, structure, tmpf)    


#    graphs = []
#    for i in range(len(colors)):
#        graphs.append(graph.subgraph(sorted_components[i]))
#
#    for i, tmp in enumerate(graphs): #len(colors)):
#        tmpf = filename + '-component-' + str(i) + '.png'
#        #tmp = components.giant() #components.subgraph()
#        plotGraph(tmp, tmpf, vlabel=True, elabel=True)
#        #igraph.plot(giant , filename, vertex_size=20,vertex_label=None,edge_label=None,layout=layoutg)
#        printGraphCSV(tmp, structure, tmpf)    


def generateGephyFile(graph, filename):
    inodes = graph.vs()
    iedges = graph.es()
    sedges = []
    edge_id = 0
    node_id = 0
    for i in range(graph.ecount()):
        weight = float(iedges[i]["weight"])
        label = iedges["label"][i]
            
    #    if pair_counts[edge] <= 5 or pair_counts[edge] > 200 or counts[edge[0]] > 500 or counts[edge[1]] > 500:
    #        continue
    #    weight = weights[i]
    #    if edge[0] not in nodes:
    #        nodes[edge[0]] = node_id
    #        node_id += 1
    #    if edge[1] not in nodes:
    #        nodes[edge[1]] = node_id
    #        node_id += 1       
        entry = '            <edge id="' + str(edge_id) + '" source="' + str(iedges[i].source) + '" target="' + str(iedges[i].target) + '" label="' + label + '" weight="' + str(weight) + '"></edge>'
        #entry = '            <edge id="' + str(edge_id) + '" source="' + str(iedges[i].source) + '" target="' + str(iedges[i].target) + '" weight="' + str(weight) + '"/>'
        sedges.append(entry + '\n')
        edge_id += 1
    
    out = open(filename + '.gexf', 'w')
    out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    
    out.write('<gexf xmlns="http://www.gexf.net/1.3" version="1.3" xmlns:viz="http://www.gexf.net/1.3/viz" ')
    out.write('xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ')
    out.write('xsi:schemaLocation="http://www.gexf.net/1.3 http://www.gexf.net/1.3/gexf.xsd">\n')    
    
#    out.write('<gexf xmlns="http://www.gexf.net/1.2draft" version="1.2"\n')
#    out.write('      xmlns:viz="http://www.gexf.net/1.2draft/viz">\n')
    out.write('    <meta lastmodifieddate="2016-04-01">\n')
    out.write('        <creator>someone</creator>\n')
    out.write('        <description>Visualization</description>\n')
    out.write('    </meta>')
    out.write('    <graph mode="static" defaultedgetype="directed">\n')
    out.write('        <nodes>\n')
    
    temp = ''
    bufsize = 0
    for node_id in range(graph.vcount()):
        temp += '            <node id="' + str(node_id) + '" label="' + inodes['label'][node_id] + '">\n'
        #out.write('                <viz:size value="5.0"></viz:size>\n')
        c = wc.name_to_rgb(graph.vs[node_id]["color"])
        c = [str(x) for x in c]
        temp += '                <viz:color r="'+c[0]+'" g="'+c[1]+'" b="'+c[2]+'"/>\n'
        temp += '            </node>\n'
        if bufsize%1000:
            out.write(temp)
            temp = ''
            bufsize = 0
            
    if len(temp) > 0:
        out.write(temp)
    
    out.write('        </nodes>\n')
    out.write('        <edges>\n')
    for edge in sedges:
        out.write(edge)
    
    out.write('        </edges>\n')
    out.write('    </graph>\n')
    out.write('</gexf>\n')
    out.close()
  

#%%
import time
from datetime import datetime
import time, pymongo
from pymongo import MongoClient

left = ''

n = 1
frame = 0
bin1 = None
bin2 = None

switch = 0

start = time.time()

structure = {}

DATA = 1
datasets = []
if DATA==0: #files
    datasets = [filelist]
else:
    client = MongoClient()
    client = MongoClient("mongodb://localhost:27017")

    db = client.twitter_db
    datasets = [ db['2016-08-24-2016-08-25-Italy'] ] 



for dataset in datasets:
    
    if DATA==0:
        cursor = codecs.open(filename, "r", "utf-8")
    else:
        #cursor = dataset.find({'status':"Loaded"}).sort([('_id', pymongo.ASCENDING)]).limit(100)
        cursor = dataset.find({}).sort([('_id', pymongo.ASCENDING)])
    
    for raw_data in cursor: #follow(file):
        data = {}
        if DATA==0:
            raw_data = raw_data.strip()
            if raw_data == '':
                continue 
            try:
                data = json.loads(raw_data,  encoding='utf-8')
            except Exception as e:
                print (e)
                print (raw_data)
                traceback.print_stack()
                break
        else:
            data = raw_data['json']

        
        if 'user' not in data:
            #skip
            continue
        
        n+=1
        if n%1000==0:
            print ("Bins of ", MINUTES, " minutes. Number of bins ", switch, " tweet: ", n)
        
        if bin1 == None:
            bin0 = bin1 = int(gettimestamp(data))
    
    
    
        #print ("load raw_data to json ", time.time() - start)
        structure= getNetwork(structure, data)
    
        #timestamp = data['created_at']
        #tweets[data['id_str']] = (data['text'], timestamp, data['in_reply_to_status_id_str'])
            
      
    
        frame += 1
        alpha = numpy.pi / 120.
        beta = numpy.pi / 200.
        #alpha = numpy.pi / 120.
        #beta = numpy.pi / 200.
    
        #layoutg = graph.layout_fruchterman_reingold_3d() 
        #layoutg = graph.layout_graphopt() #(niter=500, node_charge=0.001, node_mass=30, spring_length=0, spring_constant=1, max_sa_movement=5, seed=None)
        #layoutg = graph.layout_kamada_kawai()
        #layoutg = graph.layout_kamada_kawai_3d()
        #layoutg = graph.layout_lgl()
    
        
        bin2 = structure[data['id_str']][TS] #gettimestamp( data )
        delta = bin2-bin1
            
        stop = (switch>MAX_BINS or n>numTweets)
        
        if stop or delta>MAX_DELTA: 
            
            graph, starttime = buildGraph(structure)
            
            if graph.ecount()>0:
            
                switch += 1
                timetext = datetime.fromtimestamp(starttime/1000.0).strftime('%Y%m%d-%H%M%S')
                timetext += '-'
        
                tmp = folder + timetext + "%03d-" % switch
                print ("Get giant component : ", time.time() - start)
                generateGiantComponent(graph, tmp + '%08d-G' % frame)
                print ("Generate gephy for graph(", graph.vcount(), ", " , graph.ecount(), "): ", time.time() - start)
                generateGephyFile(graph, tmp + '%08d-gephi' % frame)
                print ("Print CSV : ", time.time() - start)
                printGraphCSV(graph, structure, tmp + '%08d-all' % frame)
                
                #plotGraph(graph, tmp + '%08d.png' % frame, elabel=False, vlabel=False)
                #layoutg = graph.layout_fruchterman_reingold() 
                #igraph.plot(graph , tmp + '%08d.png' % frame, vertex_size=15,vertex_label=None,edge_label=None, layout=layoutg)
                print ("finsihed plotting : ", time.time() - start)
        
        
                print ("ts1 : ", bin1, ", ts2: ", bin2, " , delta(min): ", delta/1000.0/60, " , total (min): ", (bin2-bin0)/1000.0/60)
                bin1 = bin1 + MAX_DELTA
        
                structure = {}
    
        if stop:
            break
    
    if DATA==0:
        file.close()


#%%

#generateGiantComponent(graph , 'frames/Giant')
#generateGephyFile(graph, 'frames/twitter')
#
#
## Now draw the frames while rotating.
#layout = graph.layout_fruchterman_reingold_3d() 
#
#for frame in range(10):
#    alpha = frame * numpy.pi / 120.
#    beta = frame * numpy.pi / 200.
#    #alpha = numpy.pi / 120.
#    #beta = numpy.pi / 200.
#    print (frame, alpha, beta)
#    drawGraph3D(graph, layout, (alpha, beta), "frames/XX%08d.png" % frame ) 



#%%
#from io import StringIO
#from igraph import Graph
#
#streamer = igraph.drawing.graph.GephiGraphStreamingDrawer
#buf = StringIO()
#streamer.draw(self=streamer, graph=giant) 
#streamer.streamer.post(giant, buf)
#print (buf.getvalue() )

#layout = graph.layout_fruchterman_reingold_3d() 
#
#for frame in range(3):
#    alpha = frame * numpy.pi / 120.
#    beta = frame * numpy.pi / 200.
#    #alpha = numpy.pi / 120.
#    #beta = numpy.pi / 200.
#    print (frame, alpha, beta)
#    drawGraph3D(graph, layout, (alpha, beta), "frames/GG%08d.png" % frame ) 

#
#m = 0
#mt = None
#for t in tmp:
#    if len(t) > m:
#        m = len(t)
#        mt = t
#
#for t in mt:
#    print (nodes[t])


#%%



#%%
