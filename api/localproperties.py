#!/usr/bin/env python

#====== Properties file - DEV ===============#

#======== API Spec ====================#
THUMBNAIL_RATE = "rate"
TOP_THUMBNAILS = "topn"
THUMBNAIL_SIZE = "size"
THUMBNAIL_INTERVAL = "interval"
ABTEST_THUMBNAILS = "abtest"
CALLBACK_URL = "callback_url"
VIDEO_ID = "video_id"
VIDEO_DOWNLOAD_URL = "video_url"
VIDEO_TITLE = "video_title"
BCOVE_READ_TOKEN = "read_token"
BCOVE_WRITE_TOKEN = "write_token"
LOG_FILE = "/tmp/neon-server.log"
REQUEST_UUID_KEY = "uuid"
API_KEY = "api_key"
JOB_SUBMIT_TIME = "submit_time"
JOB_END_TIME = "end_time"
VIDEO_PROCESS_TIME = "process_time"
YOUTUBE_VIDEO_URL = 'youtube_url'
LOCALHOST_URL = "http://localhost:8081"
BASE_SERVER_URL = LOCALHOST_URL
IMAGE_SIZE = 256,256
THUMBNAIL_IMAGE_SIZE = 256,144
MAX_THUMBNAILS  = 25
MAX_SAMPLING_RATE = 0.25
SAVE_DATA_TO_S3 = False
DELETE_TEMP_TAR = False
YOUTUBE = True

#=========== S3 Config ===============#

S3_KEY_PREFIX = 'internal_test_'
#DEV 
#S3_ACCESS_KEY = 'AKIAIHY5JHBE2BTFR4SQ'
#S3_SECRET_KEY = '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'
#S3_BUCKET_NAME = 'internal-test' 

#Prod
#S3_ACCESS_KEY = 'AKIAI5CLWOBKJDWTWZDA'
#S3_SECRET_KEY = '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'
S3_ACCESS_KEY = 'AKIAJ5G2RZ6BDNBZ2VBA'
S3_SECRET_KEY = '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'
S3_BUCKET_NAME = 'neon-beta-test' 
S3_IMAGE_HOST_BUCKET_NAME = 'host-thumbnails' 

#=========== API KEY MAPPING ==========#

API_DATA = {}
API_DATA['brightcove'] = 'd08f9ecc747ea16712f86fd192b9c574'
API_DATA['neon'] = 'a63728c09cda459c3caaa158f4adff49'
API_DATA['sophie'] = '6988ec3aba1eaddf2435141bf10487ca'
API_DATA['deborah'] = '51977f38bb3afdf634dd8162c7a33691'
API_DATA['mike'] = '18126e7bd3f84b3f3e4df094def5b7de'
API_DATA['david']= '172522ec1028ab781d9dfd17eaca4427'
API_DATA['expotv']= 'a7a86a763e97ce7276c40138733f5e68'
API_DATA['postgazette']= '1a1887842e4da19de2980538b1ae72d4'
API_DATA['danceon']= '190407db22daa11c454ea70139513b28'
API_DATA['bigframe']= '3926403a2163233129fe59bccbcbbf53'
