# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 11:32:02 2016

@author: Samer Aamar
"""

"""


THIS DIDN'T WORK FOR ME :(



"""

import urllib
import json, pprint


def collect_tweets_json_api(since, until):
    
    boundingboxes = {}
    boundingboxes['NYC'] = [-74.0853, 40.3924, -73.5052, 40.5247]
    boundingboxes['Israel'] = [34.0600,29.2100,35.5500,33.2500]
    boundingboxes['Syria'] = [35.1100,32.1600,42.3400,37.2000]
    boundingboxes['Saudi'] = [34.2600,12.5600,60.0900,31.2800]
    boundingboxes['Europe'] = [-11.5100,36.4800,26.3200,54.3700]
        
    #bbx = boundingboxes['Europe']+boundingboxes['Israel']+boundingboxes['Syria']+boundingboxes['Saudi']

    params = urllib.parse.urlencode(dict(q='#Obama', geocode='37.781157,-122.398720,1mi'))
    u = urllib.request.urlopen('http://search.twitter.com/search.json?' + params)
    j = json.load(u)
    pprint.pprint(j)
    len(j)


since = '2016-08-23'
until = '2016-08-25'
collect_tweets_json_api(since, until)

