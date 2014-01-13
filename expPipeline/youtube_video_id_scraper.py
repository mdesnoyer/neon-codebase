#!/usr/bin/python
'''Script that searches on youtube and returns a set of video ids for the search.
'''
USAGE = '%prog [options]'

import logging
from optparse import OptionParser
import xml.etree.ElementTree
import random
import re
import urllib
import urllib2
import string
import sys

from apiclient.discovery import build as api_build
import apiclient.errors

__YOUTUBE_KEY = 'AIzaSyCI1sGIS5svU8FO6cd7S4XG-Z9EvN0DYHE'
_log = logging.getLogger(__name__)

def generate_url(query, cur_idx):
    return 'http://gdata.youtube.com/feeds/api/videos?' + \
      urllib.urlencode({'vq': string.replace(query.strip(), ' ', '+'),
       'v': '2',
       'start-index': '%i' % cur_idx,
       'max-results': '%i' % 50})

def get_video_ids(query, n_videos=25, max_duration=600):
    return [x for x in generate_video_ids(query, n_videos, max_duration)]

def generate_video_ids(query, n_videos=25, max_duration=600):
    idRegex = re.compile('http://www.youtube.com/watch\?v=([\-_0-9a-zA-Z]+)')

    videos_found = 0
    cur_idx = 1
    while videos_found < n_videos:
    
        url = generate_url(query, cur_idx)
        cur_idx += 50

        xmlStream = urllib2.urlopen(url)
        try:
            xmlDoc = xml.etree.ElementTree.parse(xmlStream)
            root = xmlDoc.getroot()

            for media_group in root.iter('{http://search.yahoo.com/mrss/}group'):
                duration = int(media_group.find(
                    '{http://gdata.youtube.com/schemas/2007}duration').
                    attrib['seconds'])
                if duration < max_duration:
                    player = media_group.find(
                        '{http://search.yahoo.com/mrss/}player')
                    yield idRegex.search(player.attrib['url']).groups()[0]
                    videos_found += 1
                    if videos_found == n_videos:
                        break
        
        finally:
            xmlStream.close()

def find_similar_videos(video_id, n_videos=100):
    '''Returns a generator for similar videos.

    Inputs:
    video_id: youtube video id to find similar videos to. 
    n_videos: max number of videos to return

    returns:
    A generator that spits out youtube video_ids of similar videos.
    '''
    yt_service = api_build('youtube', 'v3', developerKey=__YOUTUBE_KEY)
    videos_found = 0

    try:
        cur_response = yt_service.search().list(
            part='id',
            maxResults=min(n_videos, 50),
            relatedToVideoId=video_id,
            type='video').execute()
    except IOError as e:
        _log.error('Error querying youtube: %s' % e)
        return
    except apiclient.errors.HttpError as e:
        _log.error('Error querying youtube: %s' % e)
        return
    while cur_response:
        for result in cur_response.get("items", []):
            videos_found += 1
            yield result["id"]["videoId"]
            if videos_found >= n_videos:
                return
        
        nextPage = cur_response.get("nextPageToken", None)
        if nextPage is None:
            break
        try:
            cur_response = yt_service.search().list(
                part='id',
                maxResults=min(n_videos - videos_found, 50),
                relatedToVideoId=video_id,
                type='video',
                pageToken=nextPage).execute()
        except IOError as e:
           _log.error('Error querying youtube: %s' % e)
           return

def get_video_duration(video_id):
    '''Retrieves the duration of the video.'''
    yt_service = api_build('youtube', 'v3', developerKey=__YOUTUBE_KEY)

    try:
        response = yt_service.videos().list(
            id=video_id,
            part='contentDetails').execute()
    except IOError as e:
        _log.error('Error getting the video length: %s' % e)
        return float('inf')

    if len(response["items"]) == 0:
        return float('+inf')

    time_str = response["items"][0]["contentDetails"]["duration"]
    minRe = re.compile('([0-9]+)M')
    secRe = re.compile('([0-9]+)S')
    hourRe = re.compile('([0-9]+)H')
    timeval = 0
    minParse = minRe.search(time_str)
    if minParse:
        timeval += 60 * int(minParse.groups()[0])

    secParse = secRe.search(time_str)
    if secParse:
        timeval += int(secParse.groups()[0])

    hourParse = hourRe.search(time_str)
    if hourParse:
        timeval += 3600 * int(hourParse.groups()[0])

    return timeval

def get_new_videos(video_ids, old_video_ids=[], max_duration=600,
                   n_videos=25):
    '''Retrieves videos similar to video_ids, skipping old ones.

    Inputs:
    video_ids - List of video ids to find ones similar to it
    old_video_ids - list of video ids to exclude from returning
    max_duration - Maximum duration of the video
    n_videos - Number of videos to return total

    Outputs:
    list of new video_ids similar to those in video_ids
    '''
    retval = []
    q = []
    q.extend(video_ids)
    n_found = 0
    while len(q) > 0 and n_found < n_videos:
        random.shuffle(q)
        video_id = q.pop()
        for candidate in find_similar_videos(video_id, 10):
            if candidate in old_video_ids:
                q.append(candidate)
            else:
                if get_video_duration(candidate) < max_duration:
                    retval.append(candidate)
                    n_found += 1
                else:
                    q.append(candidate)

            if n_found >= n_videos:
                break

    return retval

if __name__ == '__main__':
    parser = OptionParser(usage=USAGE)

    parser.add_option('-o', '--output', default=None,
                      help='Output file. Otherwise uses stdout')
    parser.add_option('-i', '--input', default=None,
                      help='Input file, one query per line. Otherwise, uses stdin')
    parser.add_option('-n', type='int', default=25,
                      help='Number of videos to retrieve per query')
    parser.add_option('--max_duration', type='int', default=600,
                      help='Maximum duration in seconds')
    parser.add_option('--yt_key',
                      default='AIzaSyCI1sGIS5svU8FO6cd7S4XG-Z9EvN0DYHE',
                      help='YouTube API key')
    parser.add_option('--seed', type='int', default=19987,
                      help='Seed for the random number generator')
    parser.add_option('--find_similar', action='store_true', default=False,
                      help='Finds videos similar to a set of input ids')
    parser.add_option('--use_queries', action='store_true', default=False,
                      help='Finds videos using text search queries')

    options, args = parser.parse_args()

    __YOUTUBE_KEY = options.yt_key
    random.seed(options.seed)
    logging.basicConfig(level=logging.INFO)

    inStream = sys.stdin
    if options.input is not None:
        inStream = open(options.input, 'r')

    video_ids = []

    if options.use_queries:
        for line in inStream:
            video_ids.extend(['http://www.youtube.com/watch?v=%s' % x for x in
                              get_video_ids(line,
                                            n_videos=options.n,
                                            max_duration=options.max_duration)])
    elif options.find_similar:
        seed_ids = [x.strip() for x in inStream]
        video_ids = get_new_videos(seed_ids,
                                   max_duration = options.max_duration,
                                   n_videos=(len(seed_ids)*options.n))
                                        

    outStream = sys.stdout
    if options.output is not None:
        outStream = open(options.output, 'w')

    outStream.write('\n'.join(video_ids))
    outStream.write('\n')
