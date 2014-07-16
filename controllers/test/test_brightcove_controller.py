#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import controllers.brightcove_controller
import json
import logging
from mock import patch, MagicMock
import PIL.Image
import random
from StringIO import StringIO
from supportServices import neondata
from supportServices.url2thumbnail import URL2ThumbnailIndex
import time
import test_utils.redis
import test_utils.mock_boto_s3
import test_utils.neontest
import tornado
from tornado.gen import YieldPoint, Task
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
from tornado.testing import AsyncHTTPTestCase, AsyncTestCase, AsyncHTTPClient
import unittest
from utils import imageutils

class TestScheduler(test_utils.neontest.TestCase):
    '''
    ABcontroller testing. Checking the scheduled events to see if they
    are as expected.
    
    '''

    def setUp(self):
        super(TestScheduler, self).setUp()
        
        self.controller = \
          controllers.brightcove_controller.BrightcoveABController()

        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        self.max_interval = 10

        # Add a video
        v1 = neondata.VideoMetadata('int_vid1', tids=['A', 'B'])
        v1.save()
        t1 = neondata.ThumbnailMetadata('A', v1.key, [])
        t2 = neondata.ThumbnailMetadata('B', v1.key, [])
        neondata.ThumbnailMetadata.save_all([t1, t2])

        
        self.controller.apply_directive(
            ("int_vid1", [('B', 0.2), ('A', 0.8)]),
            self.max_interval)
        
    def tearDown(self):
        self.redis.stop()
        super(TestScheduler, self).tearDown()

    def get_video_task_map(self):
        task_map = {} 
        #drain the entire queue
        while len(self.controller.taskmgr.taskQ.pq) >0:
            try:
                task, priority = self.controller.taskmgr.pop_task()
            except:
                break

            if not task:
                print task_map
                continue    
            vid = task.video_id
            if task_map.has_key(vid):
                task_map[vid].append((task, priority))
            else:
                task_map[vid] = []
                task_map[vid].append((task, priority))
        return task_map

    def test_timeslice_end(self):
        '''
        Pop tasks one by one and execute the timeslice task and verify
        the ordering after timeslice end task
        '''
        task_map = self.get_video_task_map() 
        for vid, tasks in task_map.iteritems():
            for (task, priority) in tasks:
                if isinstance(
                        task,
                        controllers.brightcove_controller.TimesliceEndTask):
                    task.execute()
    
        #Verify ordering of tasks after timesliceend task execution
        #i.e "idempontency" of thumbnail_change_scheduler method 
        self._test_add_tasks()

    def test_add_tasks(self):
        '''
        Verify the ordering of tasks after intial setup
        '''
        self._test_add_tasks()

    def _test_add_tasks(self):
        ' check schedule event timings for all tests and task types'

        #inspect TaskQ for expeted task ordering
        task_map = self.get_video_task_map() 
        #add correction since the actual scheduling of task happend earlier
        cur_time = time.time() - 0.5 
        cushion_time = self.controller.cushion_time
        abtest_start = self.controller.timeslice - cushion_time

        expected_order = ["ThumbnailCheckTask", "ThumbnailChangeTask_A",
                "ThumbnailCheckTask", "ThumbnailChangeTask_B",
                "ThumbnailChangeTask_A", "TimesliceEndTask"]
        expected_time_interval = [ 
                        (cur_time, cur_time + self.max_interval),
                        (cur_time, cur_time + self.max_interval),
                        (cur_time + self.max_interval, 
                                cur_time + self.max_interval + abtest_start),
                        (cur_time + self.max_interval + cushion_time, cur_time 
                                + self.controller.timeslice),
                        (cur_time + self.max_interval + cushion_time, 
                            cur_time + self.controller.timeslice 
                                + self.max_interval)
                ] 

        for vid,tasks in task_map.iteritems():
            task_order = []
            for (task, priority) in tasks:
                tname = task.__class__.__name__ 
                if "ThumbnailChangeTask" == tname: 
                    tname = task.__class__.__name__ + '_' + task.tid
                task_order.append((tname, priority))
           
            for etask, (task, p), interval in zip(expected_order,
                    task_order, expected_time_interval):
               
                    self.assertEqual(etask, task)
                    self.assertGreaterEqual(p, interval[0] - 0.5) #delay 
                    self.assertGreaterEqual(interval[1] + 0.5, p) #grace

    def test_new_directive(self):
        '''send new directive, check if task updated for a video.
            Also test the case when scheduling of minority thumbnail
            exceeds the timeslice, hence the abtest start time is corrected
        '''
        self.get_video_task_map() 
        neondata.ThumbnailMetadata('C', 'int_vid1', []).save()
        
        new_video_distribution =("int_vid1", [('B', 0.9), ('A', 0.1),
                                              ('C', 0.0)])
        self.controller.apply_directive(new_video_distribution)
        
        task_map = self.get_video_task_map() 
        expected_order = ["ThumbnailCheckTask", "ThumbnailChangeTask_B",
                "ThumbnailCheckTask", "ThumbnailChangeTask_A",
                "ThumbnailChangeTask_B", "TimesliceEndTask"]
        self._verify_task_map(task_map, new_video_distribution, expected_order)
        
        new_video_distribution = ("int_vid1", [('B', 0.51),('A', 0.49)])
        self.controller.apply_directive(new_video_distribution)
       
        task_map = self.get_video_task_map() 
        expected_order = ["ThumbnailCheckTask", "ThumbnailChangeTask_B",
                "ThumbnailCheckTask", "ThumbnailChangeTask_A",
                "ThumbnailChangeTask_B", "TimesliceEndTask"]
        self._verify_task_map(task_map, new_video_distribution, expected_order)

    def _verify_task_map(self, task_map, new_video_distribution,
                         expected_order):
        ''' Helper method to verify task map '''
        for vid, tasks in task_map.iteritems():
            
            #if vid in new_video_distribution.keys():
            if vid == new_video_distribution[0]:
                task_order = []
                for (task, priority) in tasks:
                    tname = task.__class__.__name__
                    if "ThumbnailChangeTask" == tname: 
                        tname = task.__class__.__name__ + '_' + task.tid
                    task_order.append((tname, priority))
            
            for etask,(task,p) in zip(expected_order, task_order):
                self.assertEqual(etask, task)

    def test_convert_from_percentages(self):
        ''' test conversion from % to time '''
        fraction_dist = [('B', 0.6), ('A', 0.4)] #the output is sorted
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [(x,y*ts) for x, y in fraction_dist]
        self.assertEqual(time_slices, expected_result)
        
        fraction_dist = [('B', 0.6), ('A', 0.0)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [(x,y*ts) for x, y in fraction_dist]
        self.assertEqual(time_slices, expected_result)

        fraction_dist = [('B', 0.8), ('A', 0.2), ('C', 0.0)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [(x,y*ts) for x, y in fraction_dist]
        self.assertEqual(time_slices, expected_result)

        fraction_dist = [('B', 0.8), ('A', 0.1), ('C', 0.1)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [(x,y*ts) for x, y in fraction_dist]
        self.assertEqual(time_slices, expected_result)

    def test_new_directive_with_0_percentage(self):
        '''
        Test when a new directive where only a signle thumbnail is 
        scheduled to run. all other thumbs have 0.0 % of timeslice 
        '''
        i_vid = 'int_vid1'
        new_video_distribution = (i_vid, [('B', 1.0), ('A', 0.0)])
        
        self.controller.apply_directive(new_video_distribution)
        
        task_map = self.get_video_task_map() 
        #Verify no task created for i_vid 
        self.assertFalse(task_map.has_key(i_vid))
    
    def test_new_directive_with_0_percentage_chosen(self):
        '''
        Test when a new directive where only a signle thumbnail is 
        scheduled to run. all other thumbs have 0.0 % of timeslice 
        where a thumbnail is Chosen
        '''
        i_vid = 'int_vid1'
        new_video_distribution = (i_vid, [('A', 1.0), ('B', 0.0)])
        def choose(x): x.chosen = True
        neondata.ThumbnailMetadata.modify('A', choose)
        
        self.controller.apply_directive(new_video_distribution) 
        
        task_map = self.get_video_task_map()
        expected_order = ["ThumbnailCheckTask", "ThumbnailChangeTask_A"]
        self._verify_task_map(task_map, new_video_distribution, expected_order)


    def test_multiple_neon_thumbnails(self):
        '''Test controller logic for multiple thumbnails (>2) '''
        #new_video_distribution = {"d": ("int_vid3", [('B', 0.5), ('A', 0.3), 
        #                                                    ('C',0.2)])}
        #brightcove_controller.setup_controller_for_video(
        #                        json.dumps(new_video_distribution)) 
        
        #TODO: Test logic
        pass

    def test_random_directives(self):
        '''
        Test directives with random fractions
        
        Since test code expects A to have higher fraction, the max fraction
        in the code below is assigned to A. 
        '''
        random.seed(2003)
        for i in range(50):
            rand = random.random()
            a = max(rand, 1 - rand)
            b = 1 - a
            new_video_distribution = ("int_vid1", [('B', b), ('A', a)])
            self.controller.apply_directive(new_video_distribution)
            self._test_add_tasks()

class TestThumbnailCheckTask(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestThumbnailCheckTask, self).setUp()
        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        random.seed(198948)

        self.images = [self._compress_image(
            imageutils.PILImageUtils.create_random_image(640, 480))
            for x in range(5)]

        # Set up an account in the database
        acct1 = neondata.BrightcovePlatform('acct1', 'i1', 'api1')
        acct1.add_video('v1', 'j1')
        acct1.save()
        self.v1 = neondata.VideoMetadata(
            neondata.InternalVideoID.generate('api1', 'v1'),
            ['t1', 't2'], 
            i_id='i1')
        self.v1.save()
        t1 = neondata.ThumbnailMetadata(
            't1', self.v1.key, ['one.jpg', 'one_cmp.jpg'], enabled=True,
            ttype='brightcove', rank=0)
        t1.update_phash(self.images[0])
        t1.save()
        neondata.ThumbnailURLMapper('one.jpg', 't1').save()
        neondata.ThumbnailURLMapper('one_cmp.jpg', 't1').save()
        t2 = neondata.ThumbnailMetadata(
            't2', self.v1.key,  ['two.jpg'], ttype='neon', rank=0)
        t2.update_phash(self.images[1])
        t2.save()
        neondata.ThumbnailURLMapper('two.jpg', 't2').save()

        self.url2thumb = URL2ThumbnailIndex()
        self.url2thumb.build_index_from_neondata()

        # Mock out the Brightcove API call
        self.bc_platform_patcher = patch(
            'controllers.brightcove_controller.BrightcovePlatform.get_account')
        self.bc_platform_mock = MagicMock()
        self.bc_get_account_mock = self.bc_platform_patcher.start()
        self.bc_get_account_mock.side_effect = [self.bc_platform_mock]

        # Mock out the image downloading
        self.im_download_patcher = patch(
            'controllers.brightcove_controller.PILImageUtils.download_image')
        self.im_download_mock = self.im_download_patcher.start()

        # Create the task to run
        self.task = controllers.brightcove_controller.ThumbnailCheckTask(
            self.v1.key, self.url2thumb)

    def tearDown(self):
        self.bc_platform_patcher.stop()
        self.im_download_patcher.stop()
        self.redis.stop()
        super(TestThumbnailCheckTask, self).tearDown()

    def _compress_image(self, image):
        buf = StringIO()
        image.save(buf, format='JPEG')
        buf.seek(0)

        return PIL.Image.open(buf)

    def _set_bc_url(self, thumb_url, still_url):
        '''Set the brightcove urls that will be returned by the mock.'''
        self.bc_platform_mock.get_api().get_current_thumbnail_url.return_value \
          = (thumb_url, still_url)

    def _set_download_image(self, image):
        def fake_download(url, callback=None):
            if callback:
                callback(image)
            else:
                return image
        self.im_download_mock.side_effect = fake_download

    def test_known_thumbnail_url(self):
        self._set_bc_url('one.jpg', 'one_still.jpg')

        self.task.execute()

        self.bc_get_account_mock.assert_called_with('api1', 'i1')
        self.bc_platform_mock.get_api().get_current_thumbnail_url.assert_called_with('v1')

        self.assertEqual(self.im_download_mock.call_count, 0)

        self.assertEqual('t1', self.url2thumb.get_thumbnail_info(
            'one.jpg', internal_video_id=self.v1.key).key)

    def test_known_thumbnail_new_url(self):
        self._set_bc_url('one_new.jpg', 'one_still_new.jpg')
        self._set_download_image(self.images[0])

        self.task.execute()

        self.assertEqual(self.im_download_mock.call_count, 1)
        self.assertEqual(neondata.ThumbnailURLMapper.get_id('one_new.jpg'),
                         't1')
        thumb = self.url2thumb.get_thumbnail_info(
            'one_new.jpg', internal_video_id=self.v1.key)
        self.assertEqual(thumb.key, 't1')
        self.assertItemsEqual(thumb.urls,
                              ['one.jpg', 'one_cmp.jpg', 'one_new.jpg'])

    def test_null_video_id(self):
        task = controllers.brightcove_controller.ThumbnailCheckTask(
            None, self.url2thumb)

        with self.assertLogExists(logging.ERROR,
                                  'Could not find video id:'):
            task.execute()

    def test_video_id_unknown(self):
        task = controllers.brightcove_controller.ThumbnailCheckTask(
            'unknown_video_id', self.url2thumb)

        with self.assertLogExists(logging.ERROR,
                                  'Could not find video id: unknown_video_id'):
            task.execute()

    @unittest.skip("temp")
    def test_unknown_bc_platform(self):
        self.bc_get_account_mock.side_effect = [None]

        with self.assertLogExists(
                logging.ERROR,
                'Could not find brightcove platform for video: '
                'api1_v1'):
            self.task.execute()

    def test_error_getting_current_bc_url(self):
        self._set_bc_url(None, None)

        with self.assertLogExists(
                logging.ERROR,
                'Could not find thumbnail url for video: api1_v1'):
            self.task.execute()

    def test_new_image_appeared_in_brightcove(self):
        self._set_bc_url('three.jpg', 'three_still.jpg')
        self._set_download_image(self.images[2])

        self.task.execute()

        tid = neondata.ThumbnailID.generate(self.images[2], self.v1.key)

        # Make sure the new thumb is in the database
        thumb = neondata.ThumbnailMetadata.get(tid)
        self.assertEqual(thumb.rank, 0)
        self.assertEqual(thumb.video_id, self.v1.key)
        self.assertItemsEqual(thumb.urls, ['three.jpg'])
        self.assertEqual(thumb.width, self.images[2].size[0])
        self.assertEqual(thumb.height, self.images[2].size[1])
        self.assertEqual(thumb.type, 'brightcove')
        self.assertTrue(thumb.enabled)
        self.assertFalse(thumb.chosen)
        self.assertIsNotNone(thumb.phash)

        # Make sure that the video was updated
        video = neondata.VideoMetadata.get(self.v1.key)
        self.assertItemsEqual(video.thumbnail_ids, [tid, 't1', 't2'])

        # Make sure that all the other brightcove thumbnails for the
        # video had their rank increased.
        self.assertEqual(neondata.ThumbnailMetadata.get('t1').rank,
                         1)
        self.assertEqual(neondata.ThumbnailMetadata.get('t2').rank,
                         0)

        # Finally make sure the new thumbnail was added to the index
        self.assertEqual(self.url2thumb.get_thumbnail_info('three.jpg').key,
                         tid)

class TestS3DirectiveFileLoad(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestS3DirectiveFileLoad, self).setUp()
        
        # Mock out the connection to S3
        self.s3_patcher = patch(
            'controllers.brightcove_controller.S3Connection')
        self.s3conn = test_utils.mock_boto_s3.MockConnection()
        self.s3_patcher.start().return_value = self.s3conn
        self.s3conn.create_bucket('neon-image-serving-directives')
        bucket = self.s3conn.get_bucket('neon-image-serving-directives')
        key = bucket.new_key('mastermind')
        key.set_contents_from_string(
            'expiry=2014-05-06T12:00:00Z\n'
            '{"type":"pub","pid":"pub0","aid":"acc0"}\n'
            '{"type":"pub","pid":"pub1","aid":"acc1"}\n'
            '{"type":"dir","aid":"acc0","vid":"vid0","sla":"expiry=2014-07-13T07:09:56Z","fractions":[{"pct":0.7,"tid":"thumb1","default_url":"http://default_image_url.jpg","imgs":[{"h":500,"w":600,"url":"http://neon-image-cdn.s3.amazonaws.com/pixel.jpg"},{"h":700,"w":800,"url":"http://neon/thumb2_700_800.jpg"}]},{"pct":0.2,"tid":"thumb2","default_url":"http://default_image_url.jpg","imgs":[{"h":500,"w":600,"url":"http://neont2/thumb1_500_600.jpg"},{"h":300,"w":400,"url":"http://neont2/thumb2_300_400.jpg"}]},{"pct":0.1,"tid":"thumb3","default_url":"http://default_image_url.jpg","imgs":[{"h":500,"w":600,"url":"http://neont3/thumb1_500_600.jpg"},{"h":300,"w":400,"url":"http://neont3/thumb2_300_400.jpg"}]}]}')

        self.controller = \
          controllers.brightcove_controller.BrightcoveABController()

        # Mock out the apply_directive function
        self.controller.apply_directive = MagicMock()

    def tearDown(self):
        self.s3_patcher.stop()
        super(TestS3DirectiveFileLoad, self).tearDown()

    def test_load_directives(self):
        with self.assertLogExists(logging.INFO, 'New directive file found'):
            self.controller.load_directives()

        # Make sure that a directive was received
        self.controller.apply_directive.assert_called_once_with(
            ('vid0', [('thumb1', 0.7), ('thumb2', 0.2), ('thumb3', 0.1)]),
            0)

        # Check the directive file again and it shouldn't be applied
        self.controller.apply_directive.reset_mock()
        with self.assertLogNotExists(logging.INFO, 'New directive file found'):
            self.controller.load_directives()
        self.assertFalse(self.controller.apply_directive.called)

    def test_file_missing(self):
        bucket = self.s3conn.get_bucket('neon-image-serving-directives')
        key = bucket.delete_key('mastermind')

        with self.assertLogExists(logging.ERROR,
                                  'Error getting directive file'):
            self.controller.load_directives()
        self.assertFalse(self.controller.apply_directive.called)
        
        


if __name__ == '__main__':
    unittest.main()

