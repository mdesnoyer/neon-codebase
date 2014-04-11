cd /opt/neon
source enable_env
python /opt/neon/api/create_brightcove_requests.py -c config/neon-prod.conf >> /mnt/logs/neon/brightcove_cron.log
