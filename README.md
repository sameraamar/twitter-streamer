# twitter-streamer

twitter streamers
------------------

1. collect_tweets_per_geocode_simple.py 
                  A simple version of invoking tweepy and search for tweets based on geocode

2. collect_tweets_per_geocode.py	
                 A more complex code for invoking tweepy and search for tweets based on geocode and time
                 The results are loaded to mongodb database
                 The configurations are in config.ini file and it assume you have 5 different application keys
                 in order to handle better the Rate Limits of Twitter


3. collect_tweets_json_search_api.py	collect tweets with api	of http://search.twitter.com/search.json?
                                      It doesn't work for me. but i think it is a basis. Anyway, i left this approach later on
