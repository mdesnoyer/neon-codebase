#!/usr/bin/env python
import web
import sys
import tweepy
import os
import urllib
import urllib2
import Image
import tempfile
from StringIO import StringIO

urls = (
    '/tweet/(.*)', 'tweet_me',
)

CONSUMER_KEY = 'HeYzrmfhUFMnJ0vZ705T269mr'
CONSUMER_SECRET = '4r4iCxErwVGEPXKDcLR1fHFkfWjzjx3T8W640mATzewt3KXwWf'
ACCESS_KEY = '2711207737-x9n3vXIRGLXTGXkgzsVbyRXT6m3n5qkWp4XqsxV'
ACCESS_SECRET = 'cEyj7V65coILPq0Th3WcQwbBBD72xJ73zDVTi1U1kPy90'
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.secure = True
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

app = web.application(urls, globals())

class tweet_me:
    def GET(self, user):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
    	params=urllib.unquote(user).decode('utf8')
    	param = params.split(",")
    	url = param[0]
    	status = param[1]
    	tf = tempfile.NamedTemporaryFile(suffix=".jpeg")
    	urllib.urlretrieve(url, tf.name)
    	tf.flush()
    	api = tweepy.API(auth) 
    	api.update_with_media(tf.name, status=status)

if __name__ == "__main__":
    app.run()
