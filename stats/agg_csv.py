#!/usr/bin/env python

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import pandas
import stats.metrics
import utils.neon
from utils.options import options, define

define("output", default=None, type=str,
       help="Output file. If not set, outputs to STDOUT")

def main(input):
    data = pandas.read_csv(input, header=1, index_col=[0,1,2,3,4])
    data.index.names = [u'integration_id', u'video_id', u'type', u'rank',
                        u'thumbnail_id']

    agg_data = stats.metrics.calc_aggregate_click_based_stats_from_dataframe(
        data)
    agg_data = pandas.DataFrame({'Overall' : agg_data})

    with pandas.ExcelWriter(options.output, encoding='utf-8') as writer:
        data.to_excel(writer, sheet_name='Per Video Stats')
        agg_data.to_excel(writer, sheet_name='Overall')

if __name__ == "__main__":
    inputs = utils.neon.InitNeon()
    main(inputs[0])
