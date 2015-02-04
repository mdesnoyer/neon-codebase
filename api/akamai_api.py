'''
Akamai Netstorage API
'''


import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)

import base64
import json
import hmac
import hashlib
import logging
import random
import time
import tornado.gen
import tornado.httpclient
import urllib
import utils.http
import utils.logs
import utils.neon
import utils.sync

from utils.http import RequestPool

_log = logging.getLogger(__name__)

HTTP_METHODS = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']    

### Helper classes ###

class NoG2OAuth(Exception): pass

class G2OInvalidVersion(object): pass

# This gets raised if you specify both fd and srcfile in chunked_upload().
class MultipleUploadSources(Exception): pass

class G2OAuth(object):
    '''G2OAuth: Object which contains the G2O secret, with methods to generate
    sign strings as used by the Akamai NetStorage HTTP Content Management API'''

    def __init__(self, key, nonce, version=5, client=None, server=None):
        '''__init__(): G2OAuth constructor.  You should create one of these for
        each request, as it fills in the time for you.
        Params:
        key: the value of the G2O secret 
        nonce: the "nonce" (or username) associated with the G2O secret.

        Other fields:
        version: 4 = SHA1, 5 = SHA256.
        client and server are their respective IPs, but currently reserved
        fields, both always "0.0.0.0" 
        time: the Epoch time associated with the request.
        id: a unique id number with some randomness which will guarantee
        uniqueness for the headers we will generate. 
        '''
        self.key = key
        self.nonce = nonce
        self.version = int(version)
        if self.version < 3 or self.version > 5:
            raise G2OInvalidVersion
        # We'll probably use these eventually, currently they must be "0.0.0.0"
        #self.client = client
        #self.server = server
        self.client = "0.0.0.0"
        self.server = "0.0.0.0"
        self.time = str(int(time.time()))
        self.header_id = str(random.getrandbits(32))
        self._auth_data = None

    def get_auth_data(self):
        '''Returns just the value portion of the X-Akamai-G2O-Auth-Data header,
        from the fields of the object.  ''' 
        fmt = "%s, %s, %s, %s, %s, %s"
        if not self._auth_data:
            self._auth_data = fmt % (self.version, self.server, self.client, 
                                     self.time, self.header_id, self.nonce)
        return self._auth_data

    def get_auth_sign(self, uri, action):
        '''use our key to produce a sign string from the value of the
        X-Akamai-G2O-Auth-Data header and the URI of the request.  '''
        lf = '\x0a' # line feed (note, NOT '\x0a\x0d')
        label = 'x-akamai-acs-action:'
        authd = self.get_auth_data()
        sign_string = authd + uri + lf + label + action + lf
        
        # Convert the key to String
        self.key = str(self.key)

        # version is guaranteed to be in (4,5) by the constructor
        # Version 3 is deprecated and will be removed, don't support it.
        if self.version == 3:
            d = hmac.new(self.key)
        if self.version == 4:
            d = hmac.new(self.key, digestmod=hashlib.sha1)
        if self.version == 5:
            d = hmac.new(self.key, digestmod=hashlib.sha256)
        d.update(sign_string)
        return base64.b64encode(d.digest())

    # TODO: Consider removing everything below here for the final version.
    # They're here mainly for testing, though may provide some utility.
    def get_time(self):
        '''Return the string representing the number of seconds since Epoch time
        associated with the request time of the object'''
        return self.time

    def _set_id(self, header_id):
        # Allow the auth data unique ID to be set manually, for testing only
        self.header_id = header_id

    def _set_time(self, time):
        # Allow the request time to be set manually, for testing only
        self.time = str(time)


class AkamaiNetstorage(object):
    
    '''
    ak = AkamaiNetstorage()
    ak.upload(req, body)
    '''

    def __init__(self, host, netstorage_key, netstorage_name, baseurl):
        self.host = host
        self.g2o = None
        self.md5 = None
        self.sha1 = None
        self.sha256 = None
        self.version = 1 # API Version
        self.key = netstorage_key
        self.name = netstorage_name
        self.baseurl = baseurl

    def _get_hashes(self, body):
        '''
        # This is a helper function that returns the size of the specified body, with
        # its cryptographic hashes, as a tuple.
        '''

        m = hashlib.md5()
        md5 = m.hexdigest(body)
        s = hashlib.sha1()
        sha1 = s.hexdigest(body)
        sh = hashlib.sha256
        sha256 = sh.hexdigest(body)
        return (len(body), md5, sha1, sha256)

    # set the Akamai ACS authentication header values
    def prepare_g2o(self, version=5):
        '''
        prepare_g2o(): set the Akamai ACS authentication header values.
        fields:
        key: a string containing the G2O key (password)
        name: the "nonce" (key name or username associated with the key).
        version: version of G2O auth to use; selects the hashing algorithm.
        
        The version field must be one of (3, 4, 5) and selects the hashing
        algorithm as follows:
          3: md5
          4: sha1
          5: sha256
        '''
        self.g2o = G2OAuth(self.key, self.name, version)
        
        if not self.g2o:
            raise NoG2OAuth

    #def send_request(self, action, url, body=None):
    #    ''' Send request to akamai
    #        @action : action to be perfomed (upload, download..)
    #        @url : baseURL or the filename relative to host
    #        @body : file contents if applicable
    #    '''        
    #
    #    if action == "upload":
    #        return self.upload(url, body)
    #    else:
    #        raise NotImplementedError()

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def upload(self, url, body):
        '''
        Upload data to Akamai
        @url : baseURL or the filename relative to host
        @body : file contents if applicable
        
        Return: HTTPResponse object
        '''
        # Prepare g20 Auth
        self.prepare_g2o()
        
        m = hashlib.md5()
        m.update(body)
        md5 = m.hexdigest()

        # assemble action string
        action = "upload"
        fmt = "version=%s&action=%s&format=xml"
        action_string = fmt % (self.version, action)

        # If md5
        if md5:
            action_string += "&md5=%s" % md5

        # Do g2o and send the request
        encoded_url = urllib.quote(url)
        g2o_auth_data = self.g2o.get_auth_data()
        g2o_auth_sign = self.g2o.get_auth_sign(encoded_url, action_string)
        headers = {
            'X-Akamai-ACS-Action': action_string,
            'X-Akamai-ACS-Auth-Data': g2o_auth_data,
            'X-Akamai-ACS-Auth-Sign': g2o_auth_sign
        }

        length = 0
        if (body):
            length = len(body)

        headers['Content-Length'] = length
        request_url = self.host + self.baseurl + encoded_url
        req = tornado.httpclient.HTTPRequest(url=request_url,
                    method="POST",
                    body=body,
                    headers=headers,
                    request_timeout=10.0,
                    connect_timeout=5.0)
        response = yield tornado.gen.Task(
                        utils.http.send_request, req) 
                        
        raise tornado.gen.Return(response)


if __name__ == "__main__" :
    utils.neon.InitNeon()
    host = "http://fbnneon-nsu.akamaihd.net"
    key = "kx6L370D6gcHP17emUs8f1203io6DhvjDGu88H1KEa9230uwPn"
    name = "fbneon"
    baseURL = "/344611"
    ak = AkamaiNetstorage(host, key, name, baseURL)
    r = ak.upload("/test3", "foo bar tornado2")
    print r
