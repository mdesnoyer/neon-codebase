#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import json
import unittest
from controllers import brightcove_controller
from supportServices.neondata import *


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
        
        self.test_video_distribution = {"int_vid1": [('B',0.2),('A',0.8)],
                                "int_vid2": [('B',0.30),('A',0.70)] }

        #setup video distribution
        brightcove_controller.setup_controller_for_vids(
                                json.dumps(self.test_video_distribution),delay=10)

    def get_video_task_map(self):
        task_map = {} 
        #drain the entire queue
        while len(self.taskmgr.taskQ.pq)>0:
            try:
                task,priority = self.taskmgr.pop_task()
            except:
                break

            if not task:
                print task_map
                continue    
            vid = task.video_id
            if task_map.has_key(vid):
                task_map[vid].append((task,priority))
            else:
                task_map[vid] = []
                task_map[vid].append((task,priority))
        return task_map

    def test_timeslice_end(self):
        '''
        Pop tasks one by one and execute the timeslice task and verify
        the ordering after timeslice end task
        '''
        task_map = self.get_video_task_map() 
        for vid,tasks in task_map.iteritems():
            for (task,priority) in tasks:
                if isinstance(task,brightcove_controller.TimesliceEndTask):
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
        cur_time = time.time() - 0.5 # actual schedulinge happened a lil earlier
        cushion_time = brightcove_controller.BrightcoveABController.cushion_time
        abtest_start = brightcove_controller.BrightcoveABController.timeslice - cushion_time

        expected_order = ["ThumbnailCheckTask","ThumbnailChangeTask_A","ThumbnailChangeTask_B",
                "ThumbnailChangeTask_A","TimesliceEndTask"]
        expected_time_interval = [ 
                        (cur_time, cur_time + self.controller.max_update_delay),
                        (cur_time, cur_time + self.controller.max_update_delay),
                        (cur_time + self.controller.max_update_delay, 
                                cur_time + self.controller.max_update_delay + abtest_start),
                        (cur_time + self.controller.max_update_delay + cushion_time, cur_time 
                                + brightcove_controller.BrightcoveABController.timeslice),
                        (cur_time + self.controller.max_update_delay + cushion_time, 
                            cur_time + brightcove_controller.BrightcoveABController.timeslice 
                                + self.controller.max_update_delay)
                ] 

        for vid,tasks in task_map.iteritems():
            task_order = []
            for (task,priority) in tasks:
                tname = task.__class__.__name__ 
                if "ThumbnailChangeTask" == tname: 
                    tname = task.__class__.__name__ + '_' + task.tid
                task_order.append((tname,priority))
            
            for etask,(task,p),interval in zip(expected_order,
                    task_order,expected_time_interval):
                
                    self.assertEqual(etask,task)
                    self.assertTrue( p >= interval[0] and p <= interval[1] )

    def test_new_directive(self):
        '''send new directive, check if task updated for a video.
            Also test the case when scheduling of minority thumbnail
            exceeds the timeslice, hence the abtest start time is corrected
        '''
        new_video_distribution = {"int_vid1": [('B',0.9),('A',0.1)]}
        brightcove_controller.setup_controller_for_vids(
                                json.dumps(new_video_distribution)) 
        
        task_map = self.get_video_task_map() 
        expected_order = ["ThumbnailCheckTask","ThumbnailChangeTask_B",
                "ThumbnailChangeTask_A","ThumbnailChangeTask_B","TimesliceEndTask"]
        self._verify_task_map(task_map,new_video_distribution,expected_order)
        
        new_video_distribution = {"int_vid1": [('B',0.51),('A',0.49)]}
        brightcove_controller.setup_controller_for_vids(
                json.dumps(new_video_distribution)) 
       
        task_map = self.get_video_task_map() 
        expected_order = ["ThumbnailCheckTask","ThumbnailChangeTask_B",
                "ThumbnailChangeTask_A","ThumbnailChangeTask_B","TimesliceEndTask"]
        self._verify_task_map(task_map,new_video_distribution,expected_order)

    def _verify_task_map(self,task_map,new_video_distribution,expected_order):
        for vid,tasks in task_map.iteritems():
            
            if vid in new_video_distribution.keys():
                task_order = []
                for (task,priority) in tasks:
                    tname = task.__class__.__name__
                    if "ThumbnailChangeTask" == tname: 
                        tname = task.__class__.__name__ + '_' + task.tid
                    task_order.append((tname,priority))
            
            for etask,(task,p) in zip(expected_order,task_order):
                self.assertEqual(etask,task)

    def test_convert_from_percentages(self):
        bc = brightcove_controller.BrightcoveABController()
        fraction_dist = [('B',0.6),('A',0.4)] #the output is sorted
        time_slices = bc.convert_from_percentages(fraction_dist)
        ts = brightcove_controller.BrightcoveABController.timeslice
        expected_result = [ (x,y*ts) for x,y in fraction_dist]
        self.assertEqual(time_slices,expected_result)


    #TODO: Test controller logic for multiple thumbnails (>2)


if __name__ == '__main__':
        unittest.main()

