#Sync S3 Data with the filesystem for a given Customer API Key

#!/bin/python
import properties
import sys
import os

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket


class APIDataUsageDownloader(object):

    def __init__(self):
        
        #S3 Connection
        self.s3conn = S3Connection(properties.S3_ACCESS_KEY,properties.S3_SECRET_KEY)
        self.s3bucket_name = properties.S3_BUCKET_NAME
        self.s3bucket = Bucket(name = self.s3bucket_name, connection = self.s3conn)
        self.k = Key(self.s3bucket)

    def sync(self,api_keys,download_directory):
        #S3 bucket and all the keys in that bucket
        #Note : Folders may exist - <Key: neon-beta-test,d08f9ecc747ea16712f86fd192b9c574_$folder$>
        bucket = self.s3conn.lookup(self.s3bucket_name)

        files_to_download = ['request.txt','response.txt','status.txt','result.tar.gz','video_metadata.txt']

        #Iterate through the bucket and download keys, that haven't been synced yet
        for s3_key in bucket:
            keyname = s3_key.key

            parts = keyname.split('/')
            
            #If key is a folder
            if len(parts) < 3:
                continue

            fname = parts[-1]
            request_id = parts[-2]
            api_key = parts[-3]
         
            # sync only for the api_key requested  
            if api_key in api_keys:
              
                #Create the request_id directory
                dir = download_directory + "/" + api_key + "/" + request_id
                if not os.path.exists(dir): 
                    os.mkdir(dir)

                if fname in files_to_download:
                    #check if path exists, then skip sync
                    download_fname = download_directory + "/" + keyname      

                if not os.path.exists(download_fname):
                    try:
                        self.k.key = keyname
                        self.k.get_contents_to_filename(download_fname)
                    except Exception,e:
                        print "Error", e
                
            print "synced " , api_key

######## MAIN ###############
if __name__ == '__main__':
    
    try:
        api_keys = [] 
        download_directory = '/Users/sunilmallya/s3download' #default
        
        with open(sys.argv[1]) as f:
            api_keys = [ k.rstrip('\n') for k in f.readlines()]
        
        api_requests_downloaded = os.listdir(download_directory)
        
        #Create the download directory
        if not os.path.exists(download_directory):
            os.mkdir(download_directory)
        
        #Create all the api key directories
        for api_key in api_keys:
            dir = download_directory + "/" + api_key
            if not os.path.exists(dir):
                os.mkdir(dir)
        
        ud = APIDataUsageDownloader()
        ud.sync(api_keys,download_directory)
    
    except Exception, e:
        print "./download <file with api keys>",e
        exit(0)

