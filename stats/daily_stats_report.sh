#!/bin/sh

cd /opt/neon/neon-codebase/core/
source enable_env

TODAY=`date -I -u`
YESTERDAY=`date -I -u -d "yesterday"`

# IGN
stats/generate_analytics_report_at_winner_lift.py --pub_id 2089095449 --baseline_types default,customupload --start_time ${YESTERDAY} --end_time ${TODAY} --min_impressions 500 --output ~/reports/ign_${YESTERDAY}_${TODAY}.xls 