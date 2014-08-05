'''
Host images on the CDN Module 
'''
import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)

import base64
import json
import hashlib
import properties
import socket
import supportServices.neondata
import time
import urllib
import urllib2
import utils.s3

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from poster.encode import multipart_encode
from StringIO import StringIO
from utils.imageutils import PILImageUtils
from utils import pycvutils

import logging
_log = logging.getLogger(__name__)

class CDNHosting(object):
    '''Abstract class for hosting images on a CDN.'''

    def upload_image(self, image, tid):
        '''
        Host images on the CDN
        NOTE: This method only uploads the images to S3, the bucket is
        preconfigured with cloudfront to be used as origin.

        The sizes for a given image is specified in the properties file
        size is a tuple (width, height)

        Save the mappings in ThumbnailServingURLs object 
        '''
        raise NotImplementedError()

    @staticmethod
    def create(db_entry):
        '''Creates the appropriate connection based on a database entry.

        TODO(sunil): Set this up in the database
        '''

        if db_entry == 'cloudinary':
            return CloudinaryHosting()

        # For now, as default we just use AWS
        return AWSHosting()

class AWSHosting(CDNHosting):
    
    def upload(self, image, tid):
        s3conn = S3Connection(properties.S3_ACCESS_KEY, properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_IMAGE_CDN_BUCKET_NAME
        s3bucket = s3conn.get_bucket(s3bucket_name)
        sizes = properties.CDN_IMAGE_SIZES
        s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
        fname_fmt = "neontn%s_w%s_h%s.jpg" 
        serving_urls = supportServices.neondata.ThumbnailServingURLs(tid)

        for sz in sizes:
            #im = PILImageUtils.resize(image, im_w=sz[0], im_h=sz[1])
            cv_im = pycvutils.from_pil(image)
            cv_im_r = pycvutils.resize_and_crop(cv_im, sz[1], sz[0])
            im = pycvutils.to_pil(cv_im_r)
            fname = fname_fmt % (tid, sz[0], sz[1])
            cdn_url = "http://%s/%s" % (properties.CDN_URL_PREFIX, fname)
            fmt = 'jpeg'
            filestream = StringIO()
            im.save(filestream, fmt, quality=90) 
            filestream.seek(0)
            imgdata = filestream.read()
            k = s3bucket.new_key(fname)
            utils.s3.set_contents_from_string(k, imgdata,
                                              {"Content-Type":"image/jpeg"})
            s3bucket.set_acl('public-read', fname)
            serving_urls.add_serving_url(cdn_url, sz[0], sz[1])
        
        serving_urls.save()

class CloudinaryHosting(CDNHosting):
    
    '''
    Upload a single sized base image to Cloudinary

    uploaded images look like the following
    http://res.cloudinary.com/neon-labs/image/upload/{CloudinaryUID}/{NEON_TID}.jpg

    To dynamically resize these images, the cloudinary UID is to be replaced by 
    the requeired dimensions as in this example w_120,h_90
    http://res.cloudinary.com/neon-labs/image/upload/w_120,h_90/{NEON_TID}.jpg

    '''
    def upload(self, image, tid):
        '''
        image: s3 url of the image
        Note: No support for uploading raw images yet 
        '''

        # 0, 0 indicates original (base image size)
        img_name = "neontn%s_w%s_h%s.jpg" % (tid, "0", "0") 
        
        params = {}
        params['timestamp'] = int(time.time())
        params['public_id'] = img_name 
        #params['use_filename'] = True #original file name of the uploaded image
        #params['unique_filename'] = False #don't add random characters at the end of the filename

        self.sign_request(params)
        headers = {}

        #not to be used for signing the request 
        if not isinstance(image, basestring): 
            #filestream = StringIO()
            #image.save(filestream, 'jpeg', quality=90) 
            #filestream.seek(0)
            #datagen, headers = multipart_encode({'file': filestream})
            #params['file'] = 'fakefname' 
            #self.make_request(params, datagen, headers)
            raise Exception("No support for upload raw images yet")
        else:
            params['file'] = image
            return self.make_request(params, None, headers)


    def make_request(self, params, imdata=None, headers=None):
        api_url  = "https://api.cloudinary.com/v1_1/%s/image/upload" %\
                     properties.CLOUDINARY_NAME          
        
        encoded_params = urllib.urlencode(params)
        request = urllib2.Request(api_url, encoded_params)
        
        #if imdata:
            #body = imdata #"".join([data for data in imdata])
            #request = urllib2.Request(api_url + "?" + encoded_params,
            #                           body, headers)
        
        try:
            response = urllib2.urlopen(request)
            body = response.read()
            resp = json.loads(body)
            if not resp.has_key('error'):
                return resp['url']
            else:
                # There was an error uploading the image
                _log.error("Error uploading image to cloudinary %s" %\
                            params['public_id'])
                return 

        except socket.error, e:
            _log.error("Socket error uploading image to cloudinary %s" %\
                        params['public_id'])
            return
        except urllib2.HTTPError, e:
            _log.error("http error uploading image to cloudinary %s" %\
                        params['public_id'])
            return

    def sign_request(self, params):

        api_secret = properties.CLOUDINARY_API_SECRET

        params["signature"] = self.api_sign_request(params, api_secret)
        params["api_key"] = properties.CLOUDINARY_API_KEY
        
        return params
      
    def api_sign_request(self, params_to_sign, api_secret):
        '''
        You need to sign a string with all parameters sorted by their names alphabetically. 
        Separate parameter name and value with '=' and join parameters with '&'.
        '''
        to_sign = "&".join(sorted([(k+"="+(",".join(v) if isinstance(v, list) else str(v))) for k, v in params_to_sign.items() if v]))
        return hashlib.sha1(str(to_sign + api_secret)).hexdigest()

    
