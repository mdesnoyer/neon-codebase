# simple_tweet_service
An endpoint that takes an image and a status and tweets both.

# Why
In order to post a photo to Twitter, you have to use the API's upload media functionality.  This web.py service uses the Tweepy Twitter API wrapper in order to submit the tweet, after downloading the URL provided.

# How
Run the service by typing ./rest.py.  This will start it on a localhost port.  Then GET the endpoint:
http://localhost:port/tweet/[encoded-url-t-image,status message]

The service will then download the image, and then tweet that image along with the status message.

# TODO
Fix the access-control issue that arises when calling this service from another web service.
