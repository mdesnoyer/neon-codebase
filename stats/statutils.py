'''
Utilities for dealing with the stats database

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2014

'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
from datetime import datetime
import dateutil.parser
import impala.dbapi
import impala.error
import logging
from utils.options import options, define

_log = logging.getLogger(__name__)

define("stats_host", default=None, type=str,
       help="Host to talk to for the stats db")
define('stats_cluster_type', default='video_click_stats',
       help='cluster-type tag on the stats cluster to use')
define("stats_port", default=21050, type=int,
       help="Port to connect to the stats db on")

impala_col_map = {
    MetricTypes.LOADS: 'imloadclienttime',
    MetricTypes.VIEWS: 'imvisclienttime',
    MetricTypes.CLICKS: 'imclickclienttime'
    }

def impala_connect():
    return impala.dbapi.connect(host=options.stats_host or find_cluster_ip(),
                                port=options.stats_port,
                                timeout=10000)

def find_cluster_ip():
        '''Finds the private ip of the stats cluster.'''
        _log.info('Looking for cluster of type: %s' % 
                  options.stats_cluster_type)
        cluster = stats.cluster.Cluster(options.stats_cluster_type)
        try:
            cluster.find_cluster()
        except stats.cluster.ClusterInfoError as e:
            _log.error('Could not find the cluster.')
            raise
        return cluster.master_ip

def filter_video_objects(videos, start_video_time=None, end_video_time=None):
    '''Filter a list of video objects to be those video starting between
    two times
    '''
    _log.info('Loading video info')
    requests = neondata.NeonApiRequest.get_many([(x.job_id, x.get_account_id())
                                                 for x in videos if x.job_id])
    retval = []
    if start_video_time is not None:
        start_video_time = dateutil.parser.parse(start_video_time)
    if end_video_time is not None:
        end_video_time = dateutil.parser.parse(end_video_time)
    for video, request in zip(videos, requests):
        if video is None or request is None:
            continue

        if start_video_time:
            if video.publish_date is None:
                if (request.publish_date is None or
                    dateutil.parser.parse(request.publish_date) < 
                    start_video_time):
                    continue
            elif (dateutil.parser.parse(video.publish_date) < 
                  start_video_time):
                continue

        if end_video_time:
            if video.publish_date is None:
                if (request.publish_date is None or
                    dateutil.parser.parse(request.publish_date) > 
                    end_video_time):
                    continue
            elif (dateutil.parser.parse(video.publish_date) > 
                  end_video_time):
                continue

        retval.append(video)
    return retval

def get_video_objects(impression_metric, pub_id,
                      start_time=None, end_time=None,
                      start_video_time=None, end_video_time=None,
                       video_id_file=None):
    '''Returns all the videos in the impala database within the time we 
    care about
    '''
    
    if video_id_file:
        _log.info('Using video ids from %s' % video_id_file)
        with open(video_id_file) as f:
            video_ids = [x.strip() for x in f]
    else:
        _log.info('Querying for video ids')
        conn = impala_connect()
        cursor = conn.cursor()
        cursor.execute(
        """select distinct regexp_extract(thumbnail_id, 
        '([A-Za-z0-9]+_[A-Za-z0-9~\\.\\-]+)_', 1) from eventsequences where 
        thumbnail_id is not NULL and
        %s is not null and
        tai='%s' %s""" % (impala_col_map[impression_metric],
                          pub_id, 
                          get_time_clause(start_time,
                                          end_time)))

        vidRe = re.compile('(neontn)?([0-9a-zA-Z]+_[0-9a-zA-Z\.\-\~]+)')
        video_ids = [vidRe.match(x[0]).group(2) for 
                     x in cursor if vidRe.match(x[0])]

    videos = neondata.VideoMetadata.get_many(video_ids)
    videos = [x for x in videos if x is not None]

    return filter_video_objects(videos, start_video_time,
                                end_video_time)

def get_thumb_metadata(video_objs):
    '''Returns a pandas DataFrame for metadata about each thumbnail.'''
    _log.info('Extracting metadata about videos')
    video_data = pandas.DataFrame(
        [{'video_id': x.video_id,
          'video_url': x.url,
          'integration_id':x.integration_id} for x in
          video_objs])

    request_keys = [(x.job_id, x.get_account_id()) for x in video_objs if 
                    x is not None]
    requests = neondata.NeonApiRequest.get_many(request_keys)
    request_data = pandas.DataFrame(
        [{'video_id': x.video_id,
          'title': x.video_title} 
          for x in requests if x is not None])

    video_data = video_data.join(request_data, on='video_id', how='outer')

    _log.info('Extracting metadata about thumbnails')
    thumbnail_objs = neondata.ThumbnailMetadata.get_many(
        reduce(lambda x, y: x | y,
               [set(x.thumbnail_ids) for x in video_objs.itervalues()]))
    thumb_data = pandas.DataFrame(
        [{'thumbnail_id' : x.key,
          'video_id': x.video_id, 
          'type': x.type,
          'rank': x.rank if x.type=='neon' else 0}
           for x in thumbnail_objs if x is not None])

    retval = thumb_data.join(video_data, on='video_id')
    return retval.set_index('thumbnail_id', inplace=True)
    

def get_time_clause(start_time=None, end_time=None):
    '''Returns the where clause to make sure the results are between the
    start and end times.
    '''
    clauses = []
    if start_time is not None:
        if not isinstance(start_time, datetime):
            start_time = dateutil.parser.parse(start_time)
        clauses.extend([
            '(yr > {year} or (yr = {year} and mnth >= {month}))'.format(
                year=start_time.year, month=start_time.month),
            "cast(serverTime as timestamp) >= '%s'" % 
            start_time.strftime('%Y-%m-%d %H:%M:%S')])

    if end_time is not None:
        if not isinstance(end_time, datetime):
            end_time = dateutil.parser.parse(end_time)
        clauses.extend([
            '(yr < {year} or (yr = {year} and mnth <= {month}))'.format(
                year=end_time.year, month=end_time.month),
            "cast(serverTime as timestamp) <= '%s'" % 
            end_time.strftime('%Y-%m-%d %H:%M:%S')])

    if len(clauses) == 0:
        return ''

    return ' and ' + ' and '.join(clauses)

def get_mobile_clause(do_mobile):
    if do_mobile:
        _log.info('Only collecting mobile data')
        return (" and agentinfo_os_name in "
                "('iPhone', 'Android', 'IPad', 'BlackBerry') ")

    return ''

def get_desktop_clause(do_desktop):
    if do_desktop:
        _log.info('Only collecting desktop data')
        return (" and agentinfo_os_name in "
                "('Windows', 'MacOS', 'Ubuntu', 'Linux') ")

    return ''

def get_page_clause(page, impression_metric):
    '''Returns a clause to only select results from a given page.

    page - The page where events must have occured.
           A * will be treated like a wildcard.
    '''
    if page:
        _log.info('Only collecting data from page(s): %s' % page)
        col_map = {
            'loads' : 'imloadpageurl',
            'views' : 'imloadpageurl',
            'clicks' : 'imclickpageurl',
            'plays' : 'videopageurl'
            }
        if '*' in page:
            # It's a wildcard
            return (" and %s like '%s' " %
                    (col_map[impression_metric],
                     page.replace('%', '\%').replace('*', '%')))
        else:
            return (" and %s = '%s' " % (col_map[impression_metric], page))
    return ''

def get_groupby_clause(page_regex=None,
                       desktop_mobile_split=False):
    '''Return a group by clause to split the data up.

    Inputs:
    page_regex - A regex with a group that will extract a page type of interest
    desktop_mobile_split - If true, groups by desktop vs. mobile

    Returns:
    The list of fields to group by
    '''
    clauses = []
    if page_regex:
        clauses.append('page_type')
    if desktop_mobile_split:
        clauses.append('is_mobile')

    return clauses

def get_groupby_select(impression_metric=None, page_regex=None, 
                       desktop_mobile_split=False):
    
    '''Return a string in the select part of the statement to support group by.

    Inputs:
    page_regex - A regex with a group that will extract a page type of interest
    impression_metric - The metric to be used for finding the page of
    desktop_mobile_split - If true, groups by desktop vs. mobile

    Returns:
    The string for the group by clause (including "GROUP BY")
    '''
    clauses = []
    if page_regex and impression_metric:
        col_map = {
            'loads' : 'imloadpageurl',
            'views' : 'imloadpageurl',
            'clicks' : 'imclickpageurl',
            'plays' : 'videopageurl'
            }
        clauses.append('regexp_extract(%s, %s, 1) as page_type' %
                       col_map[impression_metric],
                       page_regex)
    if desktop_mobile_split:
        clauses.append("agentinfo_os_name in "
                    "('iPhone', 'Android', 'IPad', 'BlackBerry') "
                    "as is_mobile")
    return clauses

def get_baseline_thumb(thumb_info, impressions, baseline_types=['default'],
                       min_impressions=500):
    '''Returns the thumbnail id of the baseline type.

    Inputs:
    thumb_info - pandas DataFrame indexed by thumbnail id with columns of 
                 'rank' and 'type'
    impressions - Object keyed by thumbnail id that can be used to lookup the
                  number of impressions for that thumb.
    baseline_types - List of types, in order of preference that can be a 
                     baseline
    '''
    tinfo = thumb_info.join(impressions, how='outer', rsuffix='imp')

    for btype in baseline_types:
        valid_bases = tinfo.loc((tinfo['type'] == btype) &
                                (tinfo['imp'] > min_impressions)).sort_values(
            'rank', axis=1, ascending=True)
        if len(valid_bases) > 0:
            return valid_bases.index[0]

    return None
