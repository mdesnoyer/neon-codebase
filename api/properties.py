

#====== Properties file - DEV ===============#

#======== API Spec ====================#
THUMBNAIL_RATE = "rate"
TOP_THUMBNAILS = "topn"
THUMBNAIL_SIZE = "size"
ABTEST_THUMBNAILS = "abtest"
THUMBNAIL_INTERVAL = "interval"
CALLBACK_URL = "callback_url"
VIDEO_ID = "video_id"
VIDEO_DOWNLOAD_URL = "video_url"
VIDEO_TITLE = "video_title"
BCOVE_READ_TOKEN = "read_token"
BCOVE_WRITE_TOKEN = "write_token"
LOG_FILE = "/tmp/neon-server.log"
REQUEST_UUID_KEY = "job_id"
API_KEY = "api_key"
JOB_SUBMIT_TIME = "submit_time"
JOB_END_TIME = "end_time"
VIDEO_PROCESS_TIME = "process_time"
YOUTUBE_VIDEO_URL = 'youtube_url'
LOCALHOST_URL = "http://localhost:8081"
BASE_SERVER_URL = "http://50.19.216.114:8081" #EIP
IMAGE_SIZE = 256,256
THUMBNAIL_IMAGE_SIZE = 256,144
MAX_THUMBNAILS  = 25
MAX_SAMPLING_RATE = 0.25
SAVE_DATA_TO_S3 = False
DELETE_TEMP_TAR = False 
YOUTUBE = False
API_KEY_FILE = 'apikeys.json'
BRIGHTCOVE_THUMBNAILS = "brightcove"
PUBLISHER_ID = "publisher_id"
PREV_THUMBNAIL = "previous_thumbnail"
INTEGRATION_ID = "integration_id"
#=========== S3 Config ===============#
S3_KEY_PREFIX = 'internal_test_'

#Prod
S3_BUCKET_NAME = 'neon-beta-test' 
S3_IMAGE_HOST_BUCKET_NAME = 'host-thumbnails' 
S3_CUSTOMER_ACCOUNT_BUCKET_NAME = 'neon-customer-accounts'

#IMAGE CDN
CDN_IMAGE_SIZES = [(120, 67), (160, 90), (210, 118), (320, 180), (480, 270), 
        (640, 360), (120, 90), (160, 120), (320, 240), (480, 360),
        (640, 480), (1280, 720)]

