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
API_KEY_FILE = 'apikeys.json'
BRIGHTCOVE_THUMBNAILS = "brightcove"

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

