import avro.io
import avro.schema
import os.path

data = {'eventData': {'thumbnailId': None, 'playerId': None, 'isVideoClick': True, 'videoId': u'27103143880'}, 'ipGeoData': {'city': None, 'zip': None, 'country': u'JPN', 'region': None, 'lon': 139.69, 'lat': 35.69}, 'pageId': u'ZjXyuFpYZsnsJsPr', 'agentInfo': {'os': {'version': 'XP', 'name': 'Windows'}, 'browser': {'version': u'8.0', 'name': 'Microsoft Internet Explorer'}}, 'neonUserId': '', 'eventType': 'VIDEO_CLICK', 'isp_port': 8089, 'serverTime': 1412380661095L, 'isp_host': '127.0.0.1', 'clientTime': 1412380458002L, 'pageURL': u'http://www.stack.com/video/3815844599001/nfl-wide-reciever-randall-cobb-outworks-everyone/', 'refURL': u'http://www.stack.com/video_min_ad_1/', 'trackerType': 'BRIGHTCOVE', 'userAgent': u'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)', 'clientIP': '150.70.173.48', 'trackerAccountId': u'1510551506'}

schema = avro.schema.parse(open(os.path.dirname(__file__)+"/compiled/TrackerEvent.avsc").read())

assert(avro.io.validate(schema, data))
