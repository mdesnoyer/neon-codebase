'''
Module that handles the pushing serving directives to the controllers.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
import concurrent.futures
import json
import logging
import threading
import urllib2

_log = logging.getLogger(__name__)

class Manager:
    '''Manages where the directives get sent.'''
    def __init__(self, max_connections=100):
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_connections)
        self.destinations = {} # distribution_type -> [destinations]
        self.video_dists = {} # video_id -> distribution type
        self.lock = threading.Lock()

    def register_destination(self, distribution_type, destination):
        '''Registers a destination for a particular distribution type.

        Inputs:
        distrubition_type - An entry from core.DistributionType
        destination - A destination URL for the controller
        '''
        with self.lock:
            self.destinations.setdefault(distribution_type, []).append(
                destination)

    def register_video_distribution(self, video_id, distribution_type):
        '''Register that a video should go to a particular distribution type.

        Inputs:
        video_id - Id for the video
        distribution_type - An entry from core.DistributionType
        '''
        
        with self.lock:
            self.video_dists[video_id] = distribution_type

    def send(self, directive):
        '''Asynchronously sends a directive to the A/B controller.

        Inputs:
        directive - (video_id, [(thumb_id, fraction)])
        '''
        with self.lock:
            try:
                distribution_type = self.video_dists[directive[0]]
            except KeyError:
                _log.error('No distribution type for video %s' % directive[0])
                return
            
            try:
                for destination in self.destinations[distribution_type]:
                    self.thread_pool.submit(_send_directive, directive,
                                            destination)
            except KeyError:
                _log.critical('No destination for distribution type: %s' %
                              distribution_type)
            

def _send_directive(directive, destination):
    '''Blocking call to send the directive.

    This should not be called directly. Use the functionality in Manager.
    
    Inputs:
    directive - (video_id, [(thumb_id, fraction)])
    destination - destination url of the controller


    '''
    try:
        urllib2.urlopen(destination, json.dumps(directive))
    except IOError as e:
        _log.exception('Error sending directive to %s: %s' % (destination, e))


