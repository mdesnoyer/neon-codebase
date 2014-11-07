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
import random
import properties
import socket
import supportServices.neondata
import time
import tornado.gen
import urllib
import urllib2
import utils.s3

from boto.exception import S3ResponseError, BotoServerError, BotoClientError
from boto.s3.connection import S3Connection
from poster.encode import multipart_encode
from StringIO import StringIO
import utils.botoutils
from utils.imageutils import PILImageUtils
from utils import pycvutils
import utils.sync

import logging
_log = logging.getLogger(__name__)

random.seed(125135)

@utils.sync.optional_sync
@tornado.gen.coroutine
def upload_image_to_s3(
        keyname,
        data,
        bucket=None,
        bucket_name=properties.S3_IMAGE_HOST_BUCKET_NAME,
        access_key=properties.S3_ACCESS_KEY,
        secret_key=properties.S3_SECRET_KEY,
        *args, **kwargs):
    '''
    Upload image to s3 bucket 'host-thumbnails' 

    This is the bucket where the primary copy of thumbnails are stored.
    ThumbnailMetadata structure stores these thumbnails
    CMS API returns these urls to be populated in the Neon UI
    
    Uses AWS keys from the properties file

    @keyname: the basename of the file or name relative to bucket 
    @data: string data (of image) to be uploaded to S3 
    @bucket: Optional bucket object to upload to.
             Allows reuse of the bucket object.
    @bucket_name: bucket to upload to
    @access_key: s3 key
    @secret_key: s3 secret key
    *args, **kwargs: Arguments passed to set_contents_from_string 
    '''
    try:
        if bucket is None:
            s3conn = S3Connection(access_key, secret_key)
            bucket = yield utils.botoutils.run_async(s3conn.get_bucket,
                                                     bucket_name)

        key = bucket.new_key(keyname)
            
        yield utils.botoutils.run_async(
            key.set_contents_from_string,
            data,
            {"Content-Type":"image/jpeg"},
            *args, **kwargs)
    except BotoServerError as e:
        _log.error(
            'AWS Server error when uploading image to s3://%s: %s' % 
                    (keyname, e))
        raise IOError(str(e))

    except BotoClientError as e:
        _log.error(
            'AWS client error when uploading image to s3://%s : %s' % 
                    (keyname, e))
        raise IOError(str(e))

# TODO(Sunil): Change this to use the options instead of properties
def get_s3_hosting_bucket():
    '''Returns the bucket that hosts the images.'''
    return properties.S3_IMAGE_HOST_BUCKET_NAME

@utils.sync.optional_sync
@tornado.gen.coroutine
def create_s3_redirect(dest_key, src_key, dest_bucket=None, 
                       src_bucket=None):
    '''Creates a 301 redirect in s3 that points to another location.

    Inputs:
    @dest_key: The key of the redirect to point to
    @src_key: The key that will hold the redirect
    @src_bucket: S3 bucket where the redirect object will be.
                 Defaults to the image hosting bucket
    @dest_bucket: S3 bucket where the redirect object will be.
                  Defaults to the image hosting bucket
    '''
    if src_bucket is None:
        src_bucket = get_s3_hosting_bucket()
    if dest_bucket is None:
        dest_bucket = get_s3_hosting_bucket()

    if src_bucket == dest_bucket:
        redirect_loc = dest_key
    else:
        redirect_loc = "https://%s.s3.amazonaws.com/%s" % (
            dest_bucket, dest_key)

    s3conn = S3Connection(properties.S3_ACCESS_KEY, properties.S3_SECRET_KEY)
    try:
        bucket = yield utils.botoutils.run_async(s3conn.get_bucket, src_bucket)
        key = bucket.new_key(src_key)
        yield utils.botoutils.run_async(
            key.set_contents_from_string, 
            '',
            headers={'x-amz-website-redirect-location' : redirect_loc},
            policy='public-read')
    except BotoServerError as e:
        _log.error('AWS Server error when creating a redirect s3://%s/%s -> '
                   '%s : %s' % (src_bucket, src_key, redirect_loc, e))
        raise IOError(str(e))

    except BotoClientError as e:
        _log.error('AWS client error when creating a redirect s3://%s/%s -> '
                   '%s : %s' %  (src_bucket, src_key, redirect_loc, e))
        raise IOError(str(e))
    
    

class CDNHosting(object):
    '''Abstract class for hosting images on a CDN.'''

    def __init__(self, cdn_metadata):
        '''Abstract CDN hosting class.

        @cdn_metadata - The metadata specifying how to access the CDN
        '''
        self.resize = cdn_metadata.resize
        self.update_serving_urls = cdn_metadata.update_serving_urls

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def upload(self, image, tid):
        '''
        Host images on the CDN

        The sizes for a given image is specified in the properties file
        size is a tuple (width, height)

        Saves the mappings in ThumbnailServingURLs object 
        '''
        new_serving_thumbs = [] # (url, width, height)

        if self.resize:
            for sz in properties.CDN_IMAGE_SIZES:
                cv_im = pycvutils.from_pil(image)
                cv_im_r = pycvutils.resize_and_crop(cv_im, sz[1], sz[0])
                im = pycvutils.to_pil(cv_im_r)
                cdn_url = yield self._upload_impl(im, tid, async=True)
                new_serving_thumbs.append((cdn_url, sz[0], sz[1]))

        else:
            cdn_url = yield self._upload_impl(image, tid, async=True)
            new_serving_thumbs.append((cdn_url, image.size[0], image.size[1]))

        if self.update_serving_urls:
            def add_serving_urls(obj):
                for params in new_serving_thumbs:
                    obj.add_serving_url(*params)
                    
            yield tornado.gen.Task(
                supportServices.neondata.ThumbnailServingURLs.modify,
                tid,
                add_serving_urls,
                create_missing=True)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _upload_impl(self, image, tid):
        '''Upload the specific image to the CDN service.

        Note that this could be called multiple times for the same
        image, but they could be different sizes.

        To be implemented by a subclass.

        Inputs:
        @image: PIL image to upload
        @tid: tid of the image being uploaded

        @returns: The serving url for the image
        '''
        raise NotImplementedError()

    @staticmethod
    def create(cdn_metadata):
        '''
        Creates the appropriate connection based on a database entry.
        '''

        if isinstance(cdn_metadata,
                      supportServices.neondata.S3CDNHostingMetadata):
            return AWSHosting(cdn_metadata)
        elif isinstance(cdn_metadata,
                        supportServices.neondata.CloudinaryCDNHostingMetadata):
            return CloudinaryHosting(cdn_metadata)

        else:
            raise ValueError("CDNHosting type %s not supported yet, please"
                             "implement" % cdn_metadata.__class__.__name__)

class AWSHosting(CDNHosting):

    neon_fname_fmt = "neontn%s_w%s_h%s.jpg" 

    def __init__(self, cdn_metadata):
        super(AWSHosting, self).__init__(cdn_metadata)
        self.neon_bucket = isinstance(
            cdn_metadata, supportServices.neondata.NeonCDNHostingMetadata)
        self.s3conn = S3Connection(cdn_metadata.access_key,
                                   cdn_metadata.secret_key)
        self.s3bucket_name = cdn_metadata.bucket_name
        self.s3bucket = None
        self.cdn_prefixes = cdn_metadata.cdn_prefixes
        self.folder_prefix = cdn_metadata.folder_prefix or ''
        if self.folder_prefix.endswith('/'):
            self.folder_prefix = self.folder_prefix[:-1]

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _upload_impl(self, image, tid):
        # Connect to the bucket if not done already
        if self.s3bucket is None:
            self.s3bucket = yield utils.botoutils.run_async(
                self.s3conn.get_bucket,
                self.s3bucket_name)
        
        if self.cdn_prefixes and len(self.cdn_prefixes) > 0:
            cdn_prefix = random.choice(self.cdn_prefixes)
        else:
            cdn_prefix = "%s.s3.amazonaws.com" % self.s3bucket_name
        
        fname = AWSHosting.neon_fname_fmt % (tid, image.size[0], image.size[1])
        if self.folder_prefix:
            key_name = '%s/%s' % (self.folder_prefix, fname)
        else:
            key_name = fname

        cdn_url = "http://%s/%s" % (cdn_prefix, key_name)
        fmt = 'jpeg'
        filestream = StringIO()
        image.save(filestream, fmt, quality=90) 
        filestream.seek(0)
        imgdata = filestream.read()

        # You may not have permission to do this for
        # customer bucket, so check if neon bucket 
        policy = None
        if self.neon_bucket:
            policy = 'public-read'

        yield upload_image_to_s3(key_name, imgdata, bucket=self.s3bucket,
                                 policy=policy, async=True)

        raise tornado.gen.Return(cdn_url)
        

# TODO(Sunil): Update this class with the _upload_impl
# approach. Should raise an IOError on a problem.
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
            #TODO(Sunil): support this later when you have tim
            #filestream = StringIO()
            #image.save(filestream, 'jpeg', quality=90) 
            #filestream.seek(0)
            #datagen, headers = multipart_encode({'file': filestream})
            #params['file'] = 'fakefname' 
            #self.make_request(params, datagen, headers)
            raise NotImplementedError("No support for upload raw images yet")
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

    
