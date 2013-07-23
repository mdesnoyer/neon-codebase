#!/usr/bin/python
'''Script that searches on youtube and returns a set of video ids for the search.
'''
USAGE = '%prog [options]'

from optparse import OptionParser
import xml.etree.ElementTree
import re
import urllib
import urllib2
import string
import sys

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

    options, args = parser.parse_args()

    inStream = sys.stdin
    if options.input is not None:
        inStream = open(options.input, 'r')

    video_ids = []
    for line in inStream:
        video_ids.extend(['http://www.youtube.com/watch/?v=%s' % x for x in
                          get_video_ids(line,
                                        n_videos=options.n,
                                        max_duration=options.max_duration)])

    outStream = sys.stdout
    if options.output is not None:
        outStream = open(options.output, 'w')

    outStream.write('\n'.join(video_ids))
    outStream.write('\n')
