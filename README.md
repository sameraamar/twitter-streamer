# twitter-streamer

Streamer 1:
------------
Search by time lines
Let me try to write this one!

How to build a query
---------------------
Read from https://dev.twitter.com/rest/public/search

Qouting:
The best way to build a query and test if it’s valid and will return matched Tweets is to first try it at twitter.com/search or using the Twitter advanced search query builder. As you get a satisfactory result set, the URL loaded in the browser will contain the proper query syntax that can be reused in the API endpoint. Here’s an example:

1. We want to search for tweets referencing @twitterapi account. First, we run the search on twitter.com/search
2. Check and copy the URL loaded. In this case, we got: https://twitter.com/search?q=%40twitterapi
3. Replace “https://twitter.com/search” with “https://api.twitter.com/1.1/search/tweets.json” and you will get: https://api.twitter.com/1.1/search/tweets.json?q=%40twitterapi
4. Execute this URL with a valid app or user token to return the latest results from our API.

Please note that now API v1.1 requires that the request must be authenticated, check Authentication & Authorization documentation for more details on how to do it. Also note that the search results at twitter.com may return historical results while the Search API usually only serves tweets from the past week.