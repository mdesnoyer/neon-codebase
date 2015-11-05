#!/usr/bin/env python
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                             '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import controllers.brightcove_controller
import json
import logging
from mock import patch, MagicMock
import PIL.Image
import random
from StringIO import StringIO
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
        req = neondata.NeonApiRequest("job1", "testapikey", "vid1", "", "", "", "")
        req.state = "serving_active"
        req.save()
        v1 = neondata.VideoMetadata('testapikey_vid1', ['A', 'B'], 'job1')
        v1.save()
        t1 = neondata.ThumbnailMetadata('A', v1.key, [])
        t2 = neondata.ThumbnailMetadata('B', v1.key, [])
        t3 = neondata.ThumbnailMetadata('C', v1.key, [])
        neondata.ThumbnailMetadata.save_all([t1, t2, t3])

        # mock check testing
        m_check = MagicMock()
        m_check.return_value = True
        self.controller._check_testing_with_bcontroller = m_check

        self.controller.apply_directive(
            ("testapikey_vid1", [('B', 0.2), ('A', 0.8)]),
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
        
        # Add a new thumbnail 
        v1 = neondata.VideoMetadata.get('testapikey_vid1')
        v1.thumbnail_ids.append('C')
        v1.save()

        self.controller.apply_directive(
            ("testapikey_vid1", [('B', 0.2), ('A', 0.6), ('C', 0.2)]),
            self.max_interval)
        
        expected_order = ["ThumbnailChangeTask_B", "ThumbnailChangeTask_C",
                "ThumbnailChangeTask_A", "TimesliceEndTask"]

        task_map = self.get_video_task_map()
        self._verify_tasks(task_map, expected_order)

    def _test_add_tasks(self):
        ' check schedule event timings for all tests and task types'

        #inspect TaskQ for expeted task ordering
        task_map = self.get_video_task_map()

        #add correction since the actual scheduling of task happend earlier
        cur_time = time.time() - 0.5 
        cushion_time = self.controller.cushion_time
        abtest_start = self.controller.timeslice - cushion_time

        expected_order = ["ThumbnailChangeTask_B",
                "ThumbnailChangeTask_A", "TimesliceEndTask"]

        self._verify_tasks(task_map, expected_order)

    def _verify_tasks(self, task_map, expected_order):

        for vid,tasks in task_map.iteritems():
            task_order = []
            for (task, priority) in tasks:
                tname = task.__class__.__name__ 
                if "ThumbnailChangeTask" == tname: 
                    tname = task.__class__.__name__ + '_' + task.tid
                task_order.append((tname, priority))
           
            for etask, (task, p) in zip(expected_order, task_order):
                self.assertEqual(etask, task)

    def test_convert_from_percentages(self):
        ''' test conversion from % to time '''
        fraction_dist = [('B', 0.6), ('A', 0.4)] #the output is sorted
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [(x, self.controller.enforce_minimum_time_slice(y*ts)) for x, y in fraction_dist]
        self.assertEqual(time_slices, expected_result)
        
        fraction_dist = [('B', 0.8), ('A', 0.1), ('C', 0.1)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [(x, self.controller.enforce_minimum_time_slice(y*ts)) for x, y in fraction_dist]
        self.assertEqual(time_slices, expected_result)
        
        # a total pct less than 1.0  
        fraction_dist = [('B', 0.6)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [('B',  self.controller.timeslice)] 
        self.assertEqual(time_slices, expected_result)

        # a total pct less than 1.0  
        fraction_dist = [('B', 0.6), ('A', 0.0)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [('B', self.controller.timeslice)] 
        self.assertEqual(time_slices, expected_result)

        # a total pct more than 1.0
        fraction_dist = [('B', 1.1)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        scale = 1.0 / 1.1
        expected_result =[('B', 1.1*scale*self.controller.timeslice)]
        self.assertEqual(time_slices, expected_result)
        
        # a total pct more than 1.0
        fraction_dist = [('B', 0.8), ('A', 0.2), ('C', 0.2)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        scale = 1.0 / (0.8 + 0.2 + 0.2) 
        expected_result = [('B', 0.8*scale*self.controller.timeslice), ('A', 0.2*scale*self.controller.timeslice), ('C', 0.2*scale*self.controller.timeslice)]
        self.assertEqual(time_slices, expected_result)

        # a total pct of 0.0, nothing done
        fraction_dist = [('B', 0.0)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = None 
        self.assertEqual(time_slices, expected_result)

        # a total pct of 0.0, nothing done
        fraction_dist = [('B', 0.0), ('A', 0.0), ('C', 0.0)]
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = None 
        self.assertEqual(time_slices, expected_result)

        # no fractions
        fraction_dist = []
        time_slices = self.controller.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = None 
        self.assertEqual(time_slices, expected_result)
    
    def test_thumbnail_change_scheduler(self):
        controller = controllers.brightcove_controller.BrightcoveABController()
        
        # Mock out the apply_directive function
        controller.apply_directive = MagicMock()

        video_id = 'vid0'
        distribution = [(u'thumb1', 0.7), (u'thumb2', 0.2), (u'thumb3', 0.1)] 
        controller.thumbnail_change_scheduler(video_id, distribution) 
        change_tids = []
        for dist in distribution:
            task, priority = controller.taskmgr.pop_task()
            # ignore end task
            if task.__class__.__name__ != "TimesliceEndTask": 
                change_tids.append(task.tid)

        self.assertListEqual(sorted(change_tids), ['thumb1', 'thumb2', 'thumb3'])

    def test_apply_directive(self):
       
        # Apply a change with different tids
        self.controller.apply_directive(("testapikey_vid1", [('B', 1)]), 
                              self.max_interval)
        
        expected_order = ["ThumbnailChangeTask_A", "TimesliceEndTask"]
        task_map = self.get_video_task_map()
        self._verify_tasks(task_map, expected_order)
        
        # Apply the same directive again and verify change scheduler wasn't called
        # Mock method
        self.controller.thumbnail_change_scheduler = MagicMock()
        self.controller.apply_directive(("testapikey_vid1", [('B', 1)]), 
                              self.max_interval)
        self.assertEqual(
                self.controller.thumbnail_change_scheduler._mock_call_count, 0)

        
class TestBrightcoveController(test_utils.neontest.TestCase):
    def setUp(self):
        super(TestBrightcoveController, self).setUp()
        
        # Mock out the connection to S3
        self.s3_patcher = patch(
            'controllers.brightcove_controller.S3Connection')
        self.s3conn = test_utils.mock_boto_s3.MockConnection()
        self.s3_patcher.start().return_value = self.s3conn
        self.s3conn.create_bucket('neon-image-serving-directives')
        bucket = self.s3conn.get_bucket('neon-image-serving-directives')
        self.directive =\
            '{"type":"dir","aid":"acc0","vid":"vid0","sla":"expiry=2014-07-13T07:09:56Z","fractions":[{"pct":0.7,"tid":"thumb1","default_url":"http://default_image_url.jpg","imgs":[{"h":500,"w":600,"url":"http://neon-image-cdn.s3.amazonaws.com/pixel.jpg"},{"h":700,"w":800,"url":"http://neon/thumb2_700_800.jpg"}]},{"pct":0.2,"tid":"thumb2","default_url":"http://default_image_url.jpg","imgs":[{"h":500,"w":600,"url":"http://neont2/thumb1_500_600.jpg"},{"h":300,"w":400,"url":"http://neont2/thumb2_300_400.jpg"}]},{"pct":0.1,"tid":"thumb3","default_url":"http://default_image_url.jpg","imgs":[{"h":500,"w":600,"url":"http://neont3/thumb1_500_600.jpg"},{"h":300,"w":400,"url":"http://neont3/thumb2_300_400.jpg"}]}]}'
        key = bucket.new_key('mastermind')
        key.set_contents_from_string(
            'expiry=2014-05-06T12:00:00Z\n'
            '{"type":"pub","pid":"pub0","aid":"acc0"}\n'
            '{"type":"pub","pid":"pub1","aid":"acc1"}\n'
            '%s' % self.directive)

        self.controller = \
          controllers.brightcove_controller.BrightcoveABController()
        
        # Mock out the apply_directive function
        self.controller.apply_directive = MagicMock()


    def tearDown(self):
        self.s3_patcher.stop()
        super(TestBrightcoveController, self).tearDown()

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
