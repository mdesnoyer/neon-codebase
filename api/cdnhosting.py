'''
Host images on the CDN Module 
'''
import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import properties
import utils.s3

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from supportServices import neondata
from StringIO import StringIO
from utils.imageutils import PILImageUtils

class CDNHosting(object):

    @classmethod 
    def host_images_neon_cdn(cls, image, neon_pub_id, tid):
        '''
        Host images on the CDN
        NOTE: This method only uploads the images to S3, the bucket is
        preconfigured with cloudfront to be used as origin.

        The sizes for a given image is specified in the properties file
        size is a tuple (width, height)

        Save the mappings in ThumbnailServingURLs object 
        '''
        s3conn = S3Connection(properties.S3_ACCESS_KEY, properties.S3_SECRET_KEY)
        s3bucket_name = properties.S3_IMAGE_CDN_BUCKET_NAME
        s3bucket = s3conn.get_bucket(s3bucket_name)
        sizes = properties.CDN_IMAGE_SIZES
        s3_url_prefix = "https://" + s3bucket_name + ".s3.amazonaws.com"
        fname_fmt = "neontn%s_w%s_h%s.jpg" 
        serving_urls = neondata.ThumbnailServingURLs(tid)

        for sz in sizes:
            im = PILImageUtils.resize(image, im_w=sz[0], im_h=sz[1])
            fname = fname_fmt % (tid, sz[0], sz[1])
            keyname = "%s/%s" % (neon_pub_id, fname)
            cdn_url = "http://%s/%s" % (properties.CDN_URL_PREFIX, keyname)
            fmt = 'jpeg'
            filestream = StringIO()
            im.save(filestream, fmt, quality=90) 
            filestream.seek(0)
            imgdata = filestream.read()
            k = s3bucket.new_key(keyname)
            utils.s3.set_contents_from_string(k, imgdata, {"Content-Type":"image/jpeg"})
            s3bucket.set_acl('public-read', keyname)
            serving_urls.add_serving_url(cdn_url, sz[0], sz[1])
        
        serving_urls.save()

    @classmethod
    def host_images_customer_cdn(cls, image, neon_pub_id, tid):

        '''
        to be implemented : any uploads to specific customer CDNs
        '''
        pass
