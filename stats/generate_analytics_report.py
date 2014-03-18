#!/usr/bin/env python

'''
Get thumbnail data for a customer on AB tested videos
each row corresponds to : loads, clicks, thumbnail_id 

'''
import os
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)

import json
import MySQLdb as sqldb
from supportServices import neondata
import urllib2
import urllib
from utils.options import options, define

define("stats_host", default="stats.cnvazyzlgq2v.us-east-1.rds.amazonaws.com",
        type=str, help="")
define("stats_port", default=3306, type=int, help="")
define("stats_user", default="mrwriter", type=str, help="")
define("stats_pass", default="kjge8924qm", type=str, help="")
define("stats_db", default="stats_prod", type=str, help="")

class ABTestData(object):

    def __init__(self, api_key, a_id, i_id):
        self.api_key = api_key
        self.a_id = a_id
        self.i_id = i_id
        self.statsdb_connect()
        self.query_template = 'select sum(loads), sum(clicks), thumbnail_id from hourly_events where thumbnail_id like "%s" group by thumbnail_id;'

    def statsdb_connect(self):
        '''Create connection to the stats DB and create tables if necessary.'''
        try:
            self.db_conn = sqldb.connect(
                user=options.stats_user,
                passwd=options.stats_pass,
                host=options.stats_host,
                port=options.stats_port,
                db=options.stats_db)
        except sqldb.Error as e:
            _log.exception('Error connecting to stats db: %s' % e)
            raise
        self.cursor = self.db_conn.cursor()
    
    def get_published_videos(self):
        url = 'http://services.neon-lab.com/api/v1/accounts/%s/brightcove_integrations/%s/videos/published' %(self.a_id, self.i_id)
        headers = {'X-Neon-API-Key' : self.api_key}
        req =  urllib2.Request(url, headers=headers)
        resp = urllib2.urlopen(req)
        data = json.loads(resp.read())
        vids = [] 
        for item in data['items']:
            vid = item['video_id']
            vids.append(vid)

        return vids


    def execute(self):
        #Get published videos
        video_ids = self.get_published_videos()
        image_urls = {} #image_url => ttype map 
        #get thumb data for them
        for vid in video_ids:
            i_vid = neondata.InternalVideoID.generate(self.api_key, vid)
            tid = "%s_%s_" %(self.api_key, vid)
            tid += "%"
            self.cursor.execute(self.query_template %tid)
            rows = self.cursor.fetchall()
            vmdata = neondata.VideoMetadata.get(i_vid)
            all_tids = neondata.ThumbnailMetadata.get_many(vmdata.thumbnail_ids)
            for row in rows:
                loads = row[0]
                clicks = row[1]
                tid = row[2]
                tdata = neondata.ThumbnailMetadata.get_many([tid])
                tdata = tdata[0].thumbnail_metadata
                url = tdata.urls[0]
                ttype = tdata.type
                print loads, clicks, url ,ttype, vid 
                image_urls[url] = [vid, ttype]

            if len(rows) == 1:
                other_thumb = None
                for t in all_tids:
                    if t.type != ttype:
                        other_thumb = t
                
                url = other_thumb.urls[0]
                ttype = other_thumb.type
                image_urls[url] = [vid, ttype]

                print "0", "0", url, ttype, vid 

            print ""

        #TODO: Get the earliest time stamp of Neon thumbnail & then only get thumb B data since that time
        #Get When thumbnail was published
        #save images as video id _N/B .jpg
        d = self.api_key + "_images"
        if not os.path.exists(d):
            os.mkdir(d)
        for im_url, (vid, ttype) in image_urls.iteritems():
            fname = d + "/%s_%s.jpg" %(vid, ttype)
            urllib.urlretrieve(im_url, fname)

def main():
    #PPG data
    ab = ABTestData('6d3d519b15600c372a1f6735711d956e', '52', '30')
    ab.execute()

if __name__ == "__main__":
    main()

