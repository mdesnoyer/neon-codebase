#!/bin/bash

cd /opt/neon/neon-codebase/core/
source enable_env

TODAY=`date -I -u`
YESTERDAY=`date -I -u -d "yesterday"`

COMMON_PARAMS="--start_time ${YESTERDAY} --end_time ${TODAY} --cmsdb.neondata.wants_postgres 1 --cmsdb.neondata.db_user readonly_user --cmsdb.neondata.db_password e73zfjiRO3Mr --cmsdb.neondata.db_address postgres-prod.cnvazyzlgq2v.us-east-1.rds.amazonaws.com --stats.cluster.cluster_name=\"Neon Event Cluster\""

# IGN
echo stats/generate_analytics_report_at_winner_lift.py --pub_id 2089095449 --baseline_types default,customupload "${COMMON_PARAMS}" --min_impressions 500 --output ~/reports/ign_${YESTERDAY}_${TODAY}.xls 

# CNN
#stats/generate_analytics_report_at_winner_lift.py --pub_id 1657678658 --baseline_types default "${COMMON_PARAMS}" --min_impressions 500 --output ~/reports/cnn_${YESTERDAY}_${TODAY}.xls 

# Fox
#stats/generate_analytics_report_at_winner_lift.py --pub_id 1930337906 --baseline_types default "${COMMON_PARAMS}" --min_impressions 500 --output ~/reports/fox_${YESTERDAY}_${TODAY}.xls 

# Gannett
#stats/generate_analytics_report_at_winner_lift.py --pub_id 1600036805  --baseline_types default "${COMMON_PARAMS}" --min_impressions 500 --output ~/reports/gannett_${YESTERDAY}_${TODAY}.xls 