#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import unittest
from controllers import brightcove_controller
from supportServices.neondata import *

#ab controller testing ( Schedule 10 videos, check if network calls made. And then scheduled again)
#                    -- Next send updates and see if rescheduled

class TestScheduler(unittest.TestCase):

    def setUp(self):
        self.controller = brightcove_controller.BrightcoveABController(delay=10)
        self.tq = brightcove_controller.PriorityQ() 
        brightcove_controller.taskQ = self.tq 
        self.taskmgr = brightcove_controller.TaskManager(brightcove_controller.taskQ)
        brightcove_controller.taskmgr = self.taskmgr
        brightcove_controller.SERVICE_URL = "http://localhost:8083"
        
        test_video_distribution = {"int_vid1": [('B',0.2),('A',0.8)],"int_vid2": [('B',0.30),('A',0.70)] }

        #setup video distribution
        for vid,tdist in test_video_distribution.iteritems():
            self.taskmgr.add_video_info(vid,tdist)
            self.controller.thumbnail_change_scheduler(vid,tdist)

    def get_video_task_map(self):
        task_map = {} 
        #drain the entire queue
        while len(self.taskmgr.taskQ.pq)>0:
            task,priority = self.taskmgr.pop_task()
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

    #TODO: check schedule event timings for all tests and not just task types

    def test_add_tasks(self):

        #inspect TaskQ for expeted task ordering
        task_map = self.get_video_task_map() 
        cur_time = time.time()
        cushion_time = brightcove_controller.BrightcoveABController.cushion_time
        abtest_start = brightcove_controller.BrightcoveABController.timeslice - cushion_time

        expected_order = ["ThumbnailCheckTask","ThumbnailChangeTask_A","ThumbnailChangeTask_B",
                "ThumbnailChangeTask_A","TimesliceEndTask"]
        expected_time_interval = [ 
                        (cur_time,self.controller.delay),
                        (cur_time,cur_time + self.controller.delay),
                        (cur_time + self.controller.delay,cur_time + self.controller.delay+abtest),
                        (cur_time + self.controller.delay + cushion_time, brightcove_controller.BrightcoveABController.timeslice)
                ] 

        for vid,tasks in task_map.iteritems():
            task_order = []
            for (task,priority) in tasks:
                tname = task.__class__.__name__ 
                if "ThumbnailChangeTask" == tname: 
                    tname = task.__class__.__name__ + '_' + task.tid
                task_order.append(tname)
            #task_order = [task.__class__.__name__ for task in tasks]
            self.assertItemsEqual(expected_order,task_order)
            
            #Verify the timeslice sched logic
            
            #Check Task
            #self.max_update_delay


    def test_new_directive(self):
        
        new_video_distribution = {"int_vid1": [('B',0.6),('A',0.4)]}
        for vid,tdist in new_video_distribution.iteritems():
            self.taskmgr.add_video_info(vid,tdist)
            self.controller.thumbnail_change_scheduler(vid,tdist)
       
        task_map = self.get_video_task_map() 
        expected_order = ["ThumbnailCheckTask","ThumbnailChangeTask_B","ThumbnailChangeTask_A",
                "ThumbnailChangeTask_B","TimesliceEndTask"]
        for vid,tasks in task_map.iteritems():
            
            if vid in new_video_distribution.keys():
                task_order = []
                for (task,priority) in tasks:
                    tname = task.__class__.__name__
                    if "ThumbnailChangeTask" == tname: 
                        tname = task.__class__.__name__ + '_' + task.tid
                    task_order.append(tname)
                self.assertItemsEqual(expected_order,task_order)

    def test_convert_from_percentages(self):
        bc = brightcove_controller.BrightcoveABController()
        fraction_dist = [('B',0.6),('A',0.4)]
        time_slices = bc.convert_from_percentages(fraction_dist)
        ts = brightcove_controller.BrightcoveABController.timeslice
        expected_result = [ (x,y*ts) for x,y in fraction_dist]
        self.assertEqual(time_slices,expected_result)

if __name__ == '__main__':
        unittest.main()

