#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
import tornado.httpclient
import tornado.ioloop
import tornado.httputil
import tornado.escape
import sys
from StringIO import StringIO
from poster.encode import multipart_encode
import poster.encode
import urllib
from PIL import Image
import time
import urllib
import os
from cmsdb.neondata import *


class YoutubeApi(object):
    
    def __init__(self,refresh_token):
        self.client_id = '6194976668-0k91122985fi86ctfbmv7efculmjc3ih.apps.googleusercontent.com' #test
        self.secret = 'e1N4-XRmjCSXktVeRh638_U1'
        self.oauth_url = 'https://accounts.google.com/o/oauth2/token' 
        self.upload_thumbnail_url = 'https://www.googleapis.com/upload/youtube/v3/thumbnails/set' 
        self.refresh_token = refresh_token

    '''
    Given a refresh token, get a new access token and save it to the youtube account db
    '''

    def get_access_token(self,access_callback=None):
        
        def new_token_callback(response):
            if response and not response.error:
                data = tornado.escape.json_decode(response.body)
                access_callback(data['access_token'])
            else:
                access_callback(False)

        vals =  {   'grant_type' :  'refresh_token',
                    'client_id' : self.client_id, 
                    'client_secret' : self.secret,
                    'refresh_token' : self.refresh_token
                } 
        try: 
            #body = tornado.escape.url_escape(vals) 
            body = urllib.urlencode(vals)

            req = tornado.httpclient.HTTPRequest(url = self.oauth_url, method = "POST", 
                            body = body, request_timeout = 60.0, connect_timeout = 10.0)

            if not access_callback:
                http_client = tornado.httpclient.HTTPClient()
                response = http_client.fetch(req)
                if not response.error:
                    data = tornado.escape.json_decode(response.body)
                    return data['access_token']
                return

            http_client = tornado.httpclient.AsyncHTTPClient()
            http_client.fetch(req,new_token_callback)
        except Exception,e:
            print e
            raise e

    '''
    Upload image as default thumbnail to youtube, used by neon client
    '''
    def upload_youtube_thumbnail(self,video_id,img,atoken,expiry):
        
        if time.time() > expiry:
            atoken = self.get_access_token()

        filestream = StringIO()
        image.save(filestream, 'jpeg')
        filestream.seek(0)
        image_data = filestream.getvalue()
        h = { 'Content-Type' : 'image/jpeg',
              'Content-Length' : len(image_data)
            }
        headers = tornado.httputil.HTTPHeaders(h)
        body = image_data
        client_url = self.upload_thumbnail_url + "?videoId=" + video_id +'&access_token=' + atoken
        req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers =headers, 
                            body = body, request_timeout = 60.0, connect_timeout = 10.0)
        
        http_client = tornado.httpclient.HTTPClient()
        response = http_client.fetch(req)
        if "error" in response.body:
            return False

        return True

    '''
    Upload custom thumbnail to youtube and set it as defualt
    '''

    def async_set_youtube_thumbnail(self,video_id,img_url,atoken,upload_callback):
       
        def set_image_callback(response):
            if response and not response.error:
                data = tornado.escape.json_decode(response.body)
                upload_callback(data)
            else:
                upload_callback(False)

        def image_data_callback(image_response):
            if not image_response.error:
                imfile = StringIO(image_response.body)
                image =  Image.open(imfile)
                filestream = StringIO()
                image.save(filestream, 'jpeg')
                filestream.seek(0)
                image_data = filestream.getvalue()

                h = { 'Content-Type' : 'image/jpeg',
                      'Content-Length' : len(image_data)
                    }

                headers = tornado.httputil.HTTPHeaders(h)
                body = image_data
                client_url = self.upload_thumbnail_url + "?videoId=" + video_id +'&access_token=' + atoken
                req = tornado.httpclient.HTTPRequest(url = client_url, method = "POST",headers =headers, 
                            body = body, request_timeout = 60.0, connect_timeout = 10.0)
        
                http_client = tornado.httpclient.AsyncHTTPClient()
                http_client.fetch(req,set_image_callback)
            else:
                upload_callback(False)

        #Download the image
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(url = img_url,
                                                method = "GET",
                                                request_timeout = 60.0,
                                                connect_timeout = 10.0)
        http_client.fetch(req,image_data_callback)
  
    '''
    Get the latest videos for a particular playlist 
    '''

    def get_videos(self,atoken,playlist_id,vid_callback):
        
        def get_videos_callback(response):
            if not response.error:
                vid_callback(response)
                #data = tornado.escape.json_decode(response.body)
                #items = data['items']
                #vid_callback(items)
            else:
                vid_callback(False)

        getvid_url = "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&playlistId=" + playlist_id + '&access_token=' + atoken
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(url = getvid_url,
                                                method = "GET",
                                                request_timeout = 60.0,
                                                connect_timeout = 10.0)
        http_client.fetch(req,get_videos_callback)         

    '''
    List all the channels for a given user
    '''

    def get_channels(self,atoken,channel_callback):
       
        #get 50 most recent videos
        def get_channels_callback(response):
            if not response.error:
                data = tornado.escape.json_decode(response.body)
                pinfo = data['pageInfo']
                total = pinfo['totalResults']
                items = data['items']
                channel_callback(items)
            else:
                channel_callback(False)

        self.channel_url = "https://www.googleapis.com/youtube/v3/channels?part=contentDetails&mine=true&access_token=" + atoken
        http_client = tornado.httpclient.AsyncHTTPClient()
        req = tornado.httpclient.HTTPRequest(url = self.channel_url,
                                                method = "GET",
                                                request_timeout = 60.0,
                                                connect_timeout = 10.0)
        http_client.fetch(req,get_channels_callback)         

    def get_youtube_identification(self):
        #channel id, user id
        #http://stackoverflow.com/questions/14366648/how-can-i-get-an-channel-id-from-youtube
        pass


    ### YOUTUBE ANALYTICS ###
    def get_report(self):
        pass


if __name__ == "__main__" :
    #yt = YoutubeApi()
    #im = Image.open('test.jpg')
    #vid = 'MvGovQxEqxw'
    #atoken = 'ya29.AHES6ZS9GENcY23AdWXPyS1EVY3VSt9xqhpAa_hpEjnqaMsX'    
    #yt.upload_custom_thumbnail(vid,im,atoken)
    pass
