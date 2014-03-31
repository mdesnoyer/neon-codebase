#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import controllers.brightcove_controller
import json
from mock import patch, MagicMock
import random
from StringIO import StringIO
from supportServices import neondata
import time
import test_utils.redis
import test_utils.neontest
import tornado
from tornado.gen import YieldPoint, Task
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
from tornado.testing import AsyncHTTPTestCase, AsyncTestCase, AsyncHTTPClient
import unittest

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
            json.dumps({'d': ("int_vid1", [('B', 0.2), ('A', 0.8)])}),
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
        
        new_video_distribution =\
                {"d": ("int_vid1", [('B', 0.9), ('A', 0.1), ('C', 0.0)])}
        self.controller.apply_directive(json.dumps(new_video_distribution))
        
        task_map = self.get_video_task_map() 
        expected_order = ["ThumbnailCheckTask", "ThumbnailChangeTask_B",
                "ThumbnailCheckTask", "ThumbnailChangeTask_A",
                "ThumbnailChangeTask_B", "TimesliceEndTask"]
        self._verify_task_map(task_map, new_video_distribution, expected_order)
        
        new_video_distribution = {"d": ("int_vid1", [('B', 0.51),('A', 0.49)])}
        self.controller.apply_directive(json.dumps(new_video_distribution)) 
       
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
            if vid == new_video_distribution["d"][0]:
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
        new_video_distribution = {"d": (i_vid, [('B', 1.0), ('A', 0.0)])} 
        
        self.controller.apply_directive(json.dumps(new_video_distribution)) 
        
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
        new_video_distribution = {"d": (i_vid, [('A', 1.0), ('B', 0.0)])}
        def choose(x): x.chosen = True
        neondata.ThumbnailMetadata.modify('A', choose)
        
        self.controller.apply_directive(json.dumps(new_video_distribution)) 
        
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

class TestDryRunBrighcoveController(AsyncHTTPTestCase):

    '''
    Dry Run - end to end with reduced time
    '''

    def setUp(self):
        self.controller = \
          controllers.brightcove_controller.BrightcoveABController(
            timeslice=10.0, cushion_time=0)
        super(TestDryRunBrighcoveController, self).setUp()

        self.redis = test_utils.redis.RedisServer()
        self.redis.start()

        self.max_interval = 10

        # Add a video
        v1 = neondata.VideoMetadata('int_vid1', tids=['A', 'B'])
        v1.save()
        t1 = neondata.ThumbnailMetadata('A', v1.key, [])
        t2 = neondata.ThumbnailMetadata('B', v1.key, [])
        neondata.ThumbnailMetadata.save_all([t1, t2])
        
        self.test_video_distribution = {'d': ("int_vid1", [('B', 0.2), ('A', 0.8)])}

        self.client_sync_patcher = patch(
            'controllers.brightcove_controller.tornado.httpclient.HTTPClient') 
        self.client_async_patcher = patch(
            'controllers.brightcove_controller.tornado.httpclient.AsyncHTTPClient') 
        self.client_mock_client = self.client_sync_patcher.start()
        self.client_mock_async_client = self.client_async_patcher.start()

    def tearDown(self):
        self.redis.stop()
        self.client_sync_patcher.stop()
        self.client_async_patcher.stop()
        super(TestDryRunBrighcoveController, self).tearDown()
   
    def get_app(self):
        return self.controller.tornado_app

    def test_task_execution(self):
        #TODO: Recreate scheduler logic and exec task one by one; 
        #verify return value 
       
        request = HTTPRequest('http://neon-lab.com')
        response = HTTPResponse(request, 200,
            buffer=StringIO('{"some response"}'))
        def _sf(*args, **kwargs):
            return response

        #genmock = MagicMock()
        #genmock().is_ready().return_value = True
        #genmock().get_result().return_value = [response]
        self.client_mock_client().fetch.side_effect = _sf 
        self.client_mock_async_client().fetch.side_effect = _sf 

        #send the new directive
        self.fetch('/',
                   method='POST',
                   body=json.dumps(self.test_video_distribution))

        # Wait for the directives to come out
        def do_wait():
            while 1: pass
        self.io_loop.run_sync(do_wait, 11)

        # Make sure that the thumbnail was changed 3 times.
        # TODO: This isn't the correct way to do this test, but it's 
        # something fast.
        self.assertEqual(self.client_mock_async_client.call_count, 3)

if __name__ == '__main__':
    unittest.main()

