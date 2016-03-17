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

from datetime import datetime
import dateutil.parser
import logging

_log = logging.getLogger(__name__)

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
    The string for the group by clause (including "GROUP BY")
    '''
    clauses = []
    if page_regex:
        clauses.append('pg_type')
    if desktop_mobile_split:
        clauses.append('is_mobile')

    if clauses:
        return ' GROUP BY %s' % ','.join(clauses)
    return ''

def get_groupby_select(page_regex=None, impression_metric=None,
                       desktop_mobile_split=False):
    
    '''Return a string in the select part of the statement to support group by.

    Inputs:
    page_regex - A regex with a group that will extract a page type of interest
    impression_metric - The metric to be used for finding the page of
    desktop_mobile_split - If true, groups by desktop vs. mobile

    Returns:
    The string for the group by clause (including "GROUP BY")
    '''
    clauses = ''
    if page_regex and impression_metric:
        _log.info('Grouping by page %s' % page_regex)
        col_map = {
            'loads' : 'imloadpageurl',
            'views' : 'imloadpageurl',
            'clicks' : 'imclickpageurl',
            'plays' : 'videopageurl'
            }
        clauses += (', regexp_extract(%s, %s, 1) as pg_type' %
                       col_map[impression_metric],
                       page_regex)
    if desktop_mobile_split:
        _log.info('Grouping by mobile vs. desktop')
        clauses += (", agentinfo_os_name in "
                    "('iPhone', 'Android', 'IPad', 'BlackBerry') "
                    "as is_mobile")
    return clauses
