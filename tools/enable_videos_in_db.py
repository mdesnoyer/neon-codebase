#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import atexit
from cmsdb import neondata
import dateutil.parser
import logging
import signal
import time
import tornado.gen
import tornado.ioloop
import utils.http
import utils.neon

from utils.options import define, options
'''
define('api_key', default=None, help='api key of the account to backfill')
'''

class Enabler(object):
    def __init__(self): 
        self.waiting_on_isp_videos = set([]) 
        self.enable_video_checker = utils.sync.PeriodicCoroutineTimer(
            self.enable_videos_in_database,
            300. * 1000.)

    def start(self): 
        self.enable_video_checker.start() 
    
    def stop(self): 
        self.enable_video_checker.stop()

    @tornado.gen.coroutine
    def enable_videos_in_database(self):
        '''Flags a video as being updated in the database and sends a
        callback if necessary.
        '''

        db = neondata.PostgresDB()
        conn = yield db.get_connection()
        query = "SELECT _data \
                 FROM request \
                 WHERE _data->>'state' = 'finished' \
                   AND created_time > NOW() - INTERVAL '1 DAY'"

        cursor = yield conn.execute(query)
        rows = cursor.fetchall()
        db.return_connection(conn)
        id_list = []
        for row in rows:
            video_id = '%s_%s' % (
                row['_data']['api_key'],
                row['_data']['video_id'])
            id_list.append(video_id)
        video_ids = id_list

        _log.info('Starting Enable for %s' % video_ids)  
        video_list = list(video_ids)
        CHUNK_SIZE=500
        list_chunks = [video_list[i:i+CHUNK_SIZE] for i in
                       xrange(0, len(video_list), CHUNK_SIZE)]

        for video_ids in list_chunks:
            videos = yield neondata.VideoMetadata.get_many(
                         video_ids, 
                         async=True) 
            videos = [x for x in videos if x]
            job_ids = [(v.job_id, v.get_account_id())  
                          for v in videos]
            requests = yield neondata.NeonApiRequest.get_many(
                           job_ids,
                           async=True)
 
            for video, request in zip(videos, requests):
                if video is None or \
                   request is None or \
                   request.state != neondata.RequestState.FINISHED: 
                    continue 
                tornado.ioloop.IOLoop.current().spawn_callback( 
                    functools.partial(self._enable_video_and_request, 
                        video, request))
                        
            # Throttle the callback spawning
            yield tornado.gen.sleep(5.0)

        _log.info('Finished Enable')  
  
    @tornado.gen.coroutine
    def _enable_video_and_request(self, video, request): 
        try:
            video_id = video.get_id() 
            start_time = time.time()
            if video_id in self.waiting_on_isp_videos:
                # we are already waiting on this video_id, do not 
                # start another long loop for it 
                return
            else: 
                # Now we wait until the video is serving on the isp
                self.waiting_on_isp_videos.add(video_id)
                _log.info('Currently waiting on %d videos for ISP' % 
                    len(self.waiting_on_isp_videos))  
                found = True 
                image_available = yield video.image_available_in_isp(
                    async=True)
                while not image_available:
                    if (time.time() - start_time) > 1800.0:
                        _log.error(
                            'Timed out waiting for ISP for video %s' %
                             video.key)
                        self.waiting_on_isp_videos.discard(video_id) 
                        found = False 
                        break
                    yield tornado.gen.sleep(5.0 * random.random())
                    image_available = yield video.image_available_in_isp(
                        async=True)

                if not found: 
                    return

            self.waiting_on_isp_videos.discard(video_id) 
            _log.info('Discarde and currently waiting on %d videos for ISP' % 
                len(self.waiting_on_isp_videos))  

            # Wait a bit so that it gets to all the ISPs
            yield tornado.gen.sleep(30.0)

            # Now do the database updates
            def _set_serving_url(x):
                x.serving_url = x.get_serving_url(save=False)
            yield neondata.VideoMetadata.modify(
                video_id, 
                _set_serving_url, 
                async=True)
            def _set_serving(x):
                x.state = neondata.RequestState.SERVING
            request = yield neondata.NeonApiRequest.modify(
                video.job_id,
                video.get_account_id(),
                _set_serving, 
                async=True)

            # And send the callback
            if (request is not None and 
                request.callback_state == 
                neondata.CallbackState.NOT_SENT and 
                request.callback_url):
                _log.info('sending callback for request %s', request) 
                yield self._send_callback(request)

        except Exception as e:
            _log.exception('Unexpected error when enabling video '
                           'in database %s' % e)

            self.waiting_on_isp_videos.add(video_id)

        finally:
            _log.info_n('Done with modify',20)  

    @tornado.gen.coroutine
    def _send_callback(self, request):
        '''Send the callback for a given video request.'''
        try:
            # Do really slow retries on the callback request because
            # often, the customer's system won't be ready for it.
            yield request.send_callback(send_kwargs=dict(base_delay=120.0),
                                        async=True)
        except Exception as e:
            _log.warn('Unexpected error when sending a customer callback: %s'
                      % e)
              

def main(): 
     ioloop = tornado.ioloop.IOLoop.current()
     enabler = Enabler() 
     enabler.start() 
     atexit.register(ioloop.stop)
     atexit.register(enabler.stop)
     ioloop.start()

if __name__ == "__main__": 
    utils.neon.InitNeon()
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    signal.signal(signal.SIGINT, lambda sig, y: sys.exit(-sig))
    main()
