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
        start_time = dateutil.parser.parse(start_time)
        clauses.extend([
            '(yr > {year} or (yr = {year} and mnth >= {month}))'.format(
                year=start_time.year, month=start_time.month),
            "cast(serverTime as timestamp) >= '%s'" % 
            start_time.strftime('%Y-%m-%d %H:%M:%S')])

    if end_time is not None:
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
