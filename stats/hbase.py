'''
Tools to work with Hbase

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015

'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import datetime
import happybase
import logging
import pandas
import struct
import utils.prod
from utils.options import options, define

define("stack_name", default=None,
       help="Name of the stack where the database is")
define("host", default="hbase3", help="Name of the hbase host")

def get_hourly_stats_by_time(api_key, start_time=None, end_time=None):
    '''Returns a pandas array of hourly stats between two times.

    Inputs:
    api_key - The api key of the account to get data for
    start_time - A datetime object
    end_time - A datetime object

    Outputs:
    A pandas array with columns of hr, thumb_id, [events]
    '''
    conn = happybase.Connection(utils.prod.find_host_private_address(
        options.host,
        options.stack_name))
    try:
        table = conn.table('TIMESTAMP_THUMBNAIL_EVENT_COUNTS')
        data = []
        if start_time:
            start_key = '%s_%s' % (start_time.strftime('%Y-%m-%dT%H'),
                                   api_key)
        else:
            start_key = None

        if end_time:
            end_key = '%s_%sa' % (end_time.strftime('%Y-%m-%dT%H'),
                                  api_key)
        else:
            end_key = None

        for key, row in table.scan(row_start=start_key,
                                   row_stop=end_key,
                                   columns=['evts'],
                                   filter="(RowFilter(=, 'substring:%s'))" % api_key):
            hr, garb, thumb_id = key.partition('_')
            hr = datetime.datetime.strptime(hr, '%Y-%m-%dT%H')
            row_data = dict([(k.partition(':')[2],
                              struct.unpack('>q', v)[0])
                              for k, v in row.iteritems()])
            row_data['hr'] = hr
            row_data['thumb_id'] = thumb_id
            data.append(row_data)

        return pandas.DataFrame(data)
    finally:
        conn.close()


def get_hourly_stats(api_key, thumbnail_ids=None, start_time=None,
                     end_time=None):
    if thumbnail_ids is None:
        return get_hourly_stats_by_time(api_key, start_time, end_time)

if __name__ == '__main__':
    import dateutil.parser
    data = get_hourly_stats_by_time('gvs3vytvg20ozp78rolqmdfa', 
                                    dateutil.parser.parse('2015-11-01T00:00:00'),
                                    dateutil.parser.parse('2015-11-13T23:59:59'))
    data['date'] = data['hr'].date
    by_day = data.groupby(['date']).sum()
    with pandas.ExcelWriter('/tmp/discovery_raw_daily_counts.xls') as writer:
        by_day.to_excel(writer)
