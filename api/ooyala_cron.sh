cd /opt/neon
source enable_env
python /opt/neon/api/create_ooyala_requests.py -c config/neon-prod.conf >> /mnt/logs/neon/ooyala_cron.log
