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
import urlparse
import utils.http
import utils.logs
import utils.neon
import utils.sync

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

    def __init__(self, host, netstorage_key, netstorage_name, cpcode):
        self.g2o = None
        self.md5 = None
        self.sha1 = None
        self.sha256 = None
        self.version = 1 # API Version
        self.key = netstorage_key
        self.name = netstorage_name
        self.cpcode = cpcode.strip('/') # String cpcode (e.g. 764573)

        # Normalize the host so that it includes the transport scheme
        # (e.g. http)
        host_split = urlparse.urlparse(host, 'http')
        if host_split.netloc == '':
            path_split = host_split.path.partition('/')
            host_split = [x for x in host_split]
            host_split[1] = path_split[0]
            host_split[2] = path_split[1]
        scheme_added = urlparse.urlunparse(host_split)
        self.host = scheme_added.strip('/')

    def _get_hashes(self, body):
        '''
        # This is a helper function that returns the size of the specified body, with
        # its cryptographic hashes, as a tuple.
        '''

        m = hashlib.md5(body)
        md5 = m.hexdigest()
        s = hashlib.sha1(body)
        sha1 = s.hexdigest()
        sh = hashlib.sha256(body)
        sha256 = sh.hexdigest()
        return (len(body), md5, sha1, sha256)

    def _generate_akamai_headers(self, action_string, url, version=5):
        '''Gets the akamai headers for a call.

        This resets the authorization because the auth tokens only
        last one minute.

        Inputs:
        action_string - The akamai action string
        url - The url in the bucket that's being called
        version - must be one of (3, 4, 5) and selects the hashing
          algorithm as follows:
            3: md5
            4: sha1
            5: sha256

        Outputs:
        dictionary of Akamai headers
        '''
        self.g2o = G2OAuth(self.key, self.name, version)
        encoded_url = urllib.quote(url)
        g2o_auth_data = self.g2o.get_auth_data()
        g2o_auth_sign = self.g2o.get_auth_sign(encoded_url, action_string)
        return {
            'X-Akamai-ACS-Action': action_string,
            'X-Akamai-ACS-Auth-Data': g2o_auth_data,
            'X-Akamai-ACS-Auth-Sign': g2o_auth_sign
        }

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def stat(self, url, ntries=5, do_logging=True):
        '''fetch file attributes for a file in XML format.

        Inputs:
        @url : filename relative to the host when serving

        Return: HTTPResponse object
        '''
        response = yield self._read_only_action(url, 'stat', ntries=ntries,
                                                do_logging=do_logging)
        raise tornado.gen.Return(response)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def dir(self, url, ntries=5):
        '''ffetch directory listing for an object in XML format.

        Inputs:
        @url : filename relative to the host when serving

        Return: HTTPResponse object
        '''
        response = yield self._read_only_action(url, 'dir', ntries=ntries)
        raise tornado.gen.Return(response)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(self, url, ntries=5):
        '''Delete a relative url from akamai'

        Inputs:
        @url : filename relative to the host when serving

        Return: HTTPResponse object
        '''
        response = yield self._update_action(url, 'delete', ntries=ntries)
        raise tornado.gen.Return(response)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def upload(self, url, body, index_zip=None, mtime=None, size=None,
               md5=None, sha1=None, sha256=None, ntries=5):
        '''
        Upload data to Akamai
        @url : filename relative to host when serving
        @body : string of file contents to upload if applicable
        @index_zip: Boolean which sets whether to enable az2z processing to index
                   uploaded .zip archive files for the "serve-from-zip" feature
        @mtime: String of decimal digits representing the Unix Epoch time to
               which the modification time of the file should be set.
        @size: Enforce that the uploaded file has the specified size
        @md5: Endforce that the uploaded file has the specified MD5 sum.
        @sha1: Enforce that the uploaded file has the specified SHA1 hash.
        @sha256: Enforce that the uploaded file has the specified SHA256 hash.
        
        Return: HTTPResponse object
        '''
        if md5 is None:
            m = hashlib.md5(body)
            md5 = m.hexdigest()

        response = yield self._update_action(
            url, 'upload', body, index_zip, mtime, size,
            md5, sha1, sha256, ntries=ntries)
        raise tornado.gen.Return(response)

    @tornado.gen.coroutine
    def _read_only_action(self, url, action, ntries=5, base_delay=0.4,
                          do_logging=True):
        # This internal function implements all of the read-only actions.  They
        # are all essentially identical, aside from the action name itself, and
        # of course the output.  But the output is returned via the same type of
        # object, regardless of its form.  Read-only actions must use the "GET"
        # method, and all such actions require the "format=xml" key-value pair.

        url = '/%s/%s' % (self.cpcode, url.strip('/'))
        
        fmt = "version=%s"
        if action:
            fmt += "&action=%s"
            if action != 'download':
                fmt += "&format=xml"
            action_string = fmt % (self.version, action)
        else:
           action_string = fmt % (self.version)

        encoded_url = urllib.quote(url)

        # We control the retries here because the auth header is time
        # based and we need to regenerate it.
        for cur_try in range(ntries):
            req = tornado.httpclient.HTTPRequest(
                url=self.host + encoded_url,
                method='GET',
                headers=self._generate_akamai_headers(action_string, url),
                request_timeout=10.0,
                connect_timeout=5.0)
            response = yield utils.http.send_request(req,
                                                     ntries=1,
                                                     do_logging=do_logging,
                                                     async=True)
            if response.error is None:
                raise tornado.gen.Return(response)
            delay = (1 << cur_try) * base_delay * random.random()
            yield tornado.gen.sleep(delay)

        raise tornado.gen.Return(response)

    @tornado.gen.coroutine
    def _update_action(self, url, action, body='', index_zip=None, mtime=None,
                       size=None, md5=None, sha1=None, sha256=None, 
                       destination=None, target=None, qd_confirm=None,
                       field=None, ntries=5, base_delay=0.4, do_logging=True):
        # This internal function implements all of the update actions.
        # Each has optional or required arguments; whether or not they
        # are present when required is enforced by the wrapper method
        # interface.  Update-actions require the "POST" or "PUT"
        # method, which we treat equivalently.  Unlike read-only
        # actions, "format=xml" is not required or used.

        url = '/%s/%s' % (self.cpcode, url.strip('/'))

        # assemble action string
        fmt = "version=%s"
        if action:
            fmt += "&action=%s"
            if action != 'download':
                fmt += "&format=xml"
                action_string = fmt % (self.version, action)
        else:
            action_string = fmt % (self.version)
        if index_zip:
            action_string += "&index-zip=%s" % index_zip
        if mtime is not None:
            action_string += "&mtime=%s" % mtime
        if size:
            action_string += "&size=%s" % size
        if md5:
            action_string += "&md5=%s" % md5
        if sha1:
            action_string += "&sha1=%s" % sha1
        if sha256:
            action_string += "&sha256=%s" % md5
        if destination:
            action_string += "&destination=%s" % urllib.quote_plus(destination)
        if target:
            action_string += "&target=%s" % urllib.quote_plus(target)
        if qd_confirm:
            action_string += "&quick-delete=%s" % qd_confirm

        encoded_url = urllib.quote(url)
        
        length = 0
        if (body):
            length = len(body)
        headers = {'Content-Length': length}

        # We control the retries here because the auth header is time
        # based and we need to regenerate it.
        for cur_try in range(ntries):
            headers.update(self._generate_akamai_headers(action_string, url))
            req = tornado.httpclient.HTTPRequest(
                url=self.host + encoded_url,
                method='POST',
                body=body,
                headers=headers,
                request_timeout=30.0,
                connect_timeout=15.0)
            response = yield utils.http.send_request(
                req,
                ntries=1,
                do_logging=do_logging,
                async=True)
            if response.error is None:
                raise tornado.gen.Return(response)
            delay = (1 << cur_try) * base_delay * random.random()
            yield tornado.gen.sleep(delay)

        raise tornado.gen.Return(response)


if __name__ == "__main__" :
    utils.neon.InitNeon()
    host = "http://fbnneon-nsu.akamaihd.net"
    key = "kx6L370D6gcHP17emUs8f1203io6DhvjDGu88H1KEa9230uwPn"
    name = "fbneon"
    baseURL = "344611"
    ak = AkamaiNetstorage(host, key, name, baseURL)
    print ak.stat('/test267', ntries=4)
    print ak.upload("/test2", "foo bar tornado4")
    #print r
    #print ak.delete("/test3")
