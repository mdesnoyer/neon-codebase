#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

from controllers import brightcove_controller
import json
from mock import patch, MagicMock
from StringIO import StringIO
import time
import test_utils.neontest
import tornado
from tornado.gen import YieldPoint, Task
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
from tornado.testing import AsyncHTTPTestCase, AsyncTestCase, AsyncHTTPClient
import unittest

class TestScheduler(unittest.TestCase):
    '''
    ABcontroller testing ( Schedule 10 videos, check if 
    network calls made. And then scheduled again)
    - Next send updates and see if rescheduled
    '''

    def setUp(self):
        self.controller =\
                    brightcove_controller.BrightcoveABController(delay=10)

        self.tq = brightcove_controller.PriorityQ() #create the global taskQ 
        brightcove_controller.taskQ = self.tq 
        self.taskmgr = brightcove_controller.TaskManager(
                        brightcove_controller.taskQ)

        brightcove_controller.taskmgr = self.taskmgr
        brightcove_controller.SERVICE_URL = "http://localhost:8083"
        
        self.test_video_distribution = {'d': ("int_vid1", [('B', 0.2), ('A', 0.8)])}

        #setup video distribution
        brightcove_controller.setup_controller_for_video(
                                json.dumps(self.test_video_distribution), delay=10)
                                
        self.test_video_distribution2 =\
                {'d': ("int_vid2", [('B', 0.30), ('A', 0.70)]) }
        brightcove_controller.setup_controller_for_video(
                                json.dumps(self.test_video_distribution2), delay=10)

    def get_video_task_map(self):
        task_map = {} 
        #drain the entire queue
        while len(self.taskmgr.taskQ.pq) >0:
            try:
                task, priority = self.taskmgr.pop_task()
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
                if isinstance(task, brightcove_controller.TimesliceEndTask):
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
                "ThumbnailChangeTask_B", "ThumbnailChangeTask_A", 
                "TimesliceEndTask"]
        expected_time_interval = [ 
                        (cur_time, cur_time + self.controller.max_update_delay),
                        (cur_time, cur_time + self.controller.max_update_delay),
                        (cur_time + self.controller.max_update_delay, 
                                cur_time + self.controller.max_update_delay + abtest_start),
                        (cur_time + self.controller.max_update_delay + cushion_time, cur_time 
                                + self.controller.timeslice),
                        (cur_time + self.controller.max_update_delay + cushion_time, 
                            cur_time + self.controller.timeslice 
                                + self.controller.max_update_delay)
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
                    self.assertGreaterEqual(p, interval[0])
                    self.assertGreaterEqual(interval[1], p + 0.5) #grace

    def test_new_directive(self):
        '''send new directive, check if task updated for a video.
            Also test the case when scheduling of minority thumbnail
            exceeds the timeslice, hence the abtest start time is corrected
        '''
        new_video_distribution = {"d": ("int_vid1", [('B', 0.9), ('A', 0.1)])}
        brightcove_controller.setup_controller_for_video(
                                json.dumps(new_video_distribution)) 
        
        task_map = self.get_video_task_map() 
        expected_order = ["ThumbnailCheckTask", "ThumbnailChangeTask_B",
                "ThumbnailChangeTask_A", "ThumbnailChangeTask_B", 
                "TimesliceEndTask"]
        self._verify_task_map(task_map, new_video_distribution, expected_order)
        
        new_video_distribution = {"d": ("int_vid1", [('B', 0.51), ('A', 0.49)])}
        brightcove_controller.setup_controller_for_video(
                json.dumps(new_video_distribution)) 
       
        task_map = self.get_video_task_map() 
        expected_order = ["ThumbnailCheckTask", "ThumbnailChangeTask_B",
                "ThumbnailChangeTask_A", "ThumbnailChangeTask_B", 
                "TimesliceEndTask"]
        self._verify_task_map(task_map, new_video_distribution, expected_order)

    def _verify_task_map(self, task_map, new_video_distribution, expected_order):
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
        bc = brightcove_controller.BrightcoveABController()
        fraction_dist = [('B', 0.6), ('A', 0.4)] #the output is sorted
        time_slices = bc.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [(x,y*ts) for x, y in fraction_dist]
        self.assertEqual(time_slices, expected_result)
        
        fraction_dist = [('B', 0.6), ('A', 0.0)]
        time_slices = bc.convert_from_percentages(fraction_dist)
        ts = self.controller.timeslice
        expected_result = [(x,y*ts) for x, y in fraction_dist]
        self.assertEqual(time_slices, expected_result)

    def test_new_directive_with_0_percentage(self):
        '''
        Test when a new directive where only a signle thumbnail is 
        scheduled to run. all other thumbs have 0.0 % of timeslice 
        '''
        new_video_distribution = {"d": ("int_vid1", [('B', 1.0), ('A', 0.0)])}
        brightcove_controller.setup_controller_for_video(
                                json.dumps(new_video_distribution)) 
        
        task_map = self.get_video_task_map() 
        expected_order = ["ThumbnailCheckTask", "ThumbnailChangeTask_B"]
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
        super(TestDryRunBrighcoveController, self).setUp()
        self.controller =\
                    brightcove_controller.BrightcoveABController(delay=0,
                            timeslice=20, cushion_time=0)

        self.tq = brightcove_controller.PriorityQ() #create the global taskQ 
        brightcove_controller.taskQ = self.tq 
        self.taskmgr = brightcove_controller.TaskManager(
                        brightcove_controller.taskQ)

        brightcove_controller.taskmgr = self.taskmgr
        brightcove_controller.SERVICE_URL = "http://localhost:8083"
        self.test_video_distribution = {'d': ("int_vid1", [('B', 0.2), ('A', 0.8)])}

    def tearDown(self):
        super(TestDryRunBrighcoveController, self).tearDown()
   
    def get_app(self):
        return brightcove_controller.application

    def get_new_ioloop(self):
        ''' new ioloop '''
        return tornado.ioloop.IOLoop.instance() 

    def test_task_execution(self):
        #TODO: Recreate scheduler logic and exec task one by one; 
        #verify return value 
       
        request = HTTPRequest('http://neon-lab.com')
        response = HTTPResponse(request, 200,
            buffer=StringIO('{"some response"}'))
        def _sf(*args, **kwargs):
            return response

        #setup video distribution
        brightcove_controller.setup_controller_for_video(
                                json.dumps(self.test_video_distribution), delay=0)
        
        self.client_sync_patcher = patch(
            'controllers.brightcove_controller.tornado.httpclient.HTTPClient') 
        self.client_async_patcher = patch(
          'controllers.brightcove_controller.tornado.httpclient.AsyncHTTPClient') 
        self.client_mock_client = self.client_sync_patcher.start()
        self.client_mock_async_client = self.client_async_patcher.start()
        #genmock = MagicMock()
        #genmock().is_ready().return_value = True
        #genmock().get_result().return_value = [response]
        self.client_mock_client().fetch.side_effect = _sf 
        self.client_mock_async_client().fetch.side_effect = _sf 

        #self.taskmgr.check_scheduler()
        t, p = self.taskmgr.pop_task()
        t, p = self.taskmgr.pop_task()
        t.execute()

        self.client_sync_patcher.stop()
        self.client_async_patcher.stop()

if __name__ == '__main__':
        unittest.main()

