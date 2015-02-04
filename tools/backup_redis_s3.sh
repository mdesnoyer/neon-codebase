#!/bin/bash

## Backup script to upload production DB to S3

#################################################################
## USER VARIABLES [S3 Options]
#################################################################
AWS_ACCESS_KEY="AKIAJ5G2RZ6BDNBZ2VBA"
AWS_SECRET_KEY="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"
BUCKET="neon-db-backup"


#################################################################
## USER VARIABLES [Backup Params]
#################################################################
SERVER=`hostname | tr '.' ' ' | awk '{print $1}'`
DAY=`date +"%Y-%m-%d"`
TS=`date +"%Y-%m-%d_%H%M"`
LASTDAY=`date +%Y-%m-%d --date='1 week ago'`
DESTINATION="s3://$BUCKET/redis/$SERVER/$DAY/$TS"
PURGEDESTINATION="s3://$BUCKET/redis/$SERVER/$LASTDAY"
LOGFILE=/mnt/logs/redisbackup.$DAY.log

#################################################################
## USER VARIABLES [Redis Locations]
#################################################################
REDIS_BASE=/mnt/redis-db
REDIS_CONF=/etc/redis
echo $FOLDER


#################################################################
## Perform Checks [check for root user]
#################################################################
#if [[ $EUID -ne 0 ]]; then
#   echo "You must be root to run this script. Exiting...";
#   exit 1;
#fi

#################################################################
## Perform Checks [check for existence of s3cmd]
#################################################################
if [[ `which s3cmd | wc -l` -eq 0 ]]; then
   echo "Missing AWS s3cmd tool. Exiting...";
   exit 1;
fi

#################################################################
## Perform Checks [check for s3cmd config]
#################################################################
if [ ! -f ~/.s3cfg ]; then
echo "Missing s3cmd config.  Creating for you.  Note, you can create one yourself by running \"s3cmd --configure\""
touch ~/.s3cfg
cat>~/.s3cfg<<EOF
[default]
access_key = $AWS_ACCESS_KEY
bucket_location = US
cloudfront_host = cloudfront.amazonaws.com
cloudfront_resource = /2010-07-15/distribution
default_mime_type = binary/octet-stream
delete_removed = False
dry_run = False
encoding = ANSI_X3.4-1968
encrypt = False
follow_symlinks = False
force = False
get_continue = False
gpg_command = /usr/bin/gpg
gpg_decrypt = %(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_passphrase = 
guess_mime_type = True
host_base = s3.amazonaws.com
host_bucket = %(bucket)s.s3.amazonaws.com
human_readable_sizes = False
list_md5 = False
log_target_prefix = 
preserve_attrs = True
progress_meter = True
proxy_host = 
proxy_port = 0
recursive = False
recv_chunk = 4096
reduced_redundancy = False
secret_key = $AWS_SECRET_KEY
send_chunk = 4096
simpledb_host = sdb.amazonaws.com
skip_existing = False
socket_timeout = 10
urlencoding_mode = normal
use_https = True
verbosity = WARNING
EOF
fi


#################################################################
## Functions [logging]
#################################################################

f_LOG() {
echo -e "`date`\t$@\t" >> $LOGFILE
}

f_INFO() {
echo "$@"
f_LOG "INFO\t $@"
}

f_WARNING() {
echo "$@"
f_LOG "WARNING\t $@"
}

f_ERROR() {
echo "$@"
f_LOG "ERROR\t $@"
}

#################################################################
## Main
#################################################################
for instance in `ls $REDIS_CONF/redis-prod*.conf`; do

  ## get config 
	CONFIGFILE=`echo $instance`
	INSTANCENAME=`echo $(basename $CONFIGFILE) | tr '-' ' ' | tr '.' ' ' | awk '{print $3}'`
	PORT=`cat ${CONFIGFILE} | grep port | awk '{print $2}'`
	AOF=`cat ${CONFIGFILE} | grep appendfilename | awk '{print $2}'`
	RDB=`cat ${CONFIGFILE} | grep dbfilename | awk '{print $2}'`
	AOF=`echo ${REDIS_BASE}/${AOF}`
	RDB=`echo ${REDIS_BASE}/${RDB}`
	AOFEXT=`echo ${AOF}|awk -F . '{print $NF}'`
	RDBEXT=`echo ${RDB}|awk -F . '{print $NF}'`
	## build tarball
	f_INFO "${INSTANCENAME}  Begin Tarball"
	cd ${REDIS_BASE}
	tar cvzf ${TS}_${INSTANCENAME}.tar.gz -C ${REDIS_BASE} *.${AOFEXT} *.rdb > /dev/null
	f_INFO "${INSTANCENAME}  Complete Tarball"

	## upload tarball to s3
	f_INFO "${INSTANCENAME}  Begin S3 Upload"
	s3cmd --no-progress put ${TS}_${INSTANCENAME}.tar.gz $DESTINATION/${TS}_${INSTANCENAME}.tar.gz
	f_INFO "${INSTANCENAME}  Complete S3 Upload"

	## check that the tarball exists on S3
	f_INFO "${INSTANCENAME}  Begin S3 FileCheck"
	if [[ `s3cmd ls $DESTINATION/${TS}_${INSTANCENAME}.tar.gz | wc -l` -eq 1 ]]; 
	then
		f_INFO "${INSTANCENAME}  Complete S3 FileCheck [SUCCESS]"
	else
		f_ERROR "${INSTANCENAME}  Complete S3 FileCheck [FAILURE]"
	fi

	## cleanup local zip file
	f_INFO "${INSTANCENAME}  Begin Removing Local Tarball"
	rm -f ${TS}_${INSTANCENAME}.tar.gz
	f_INFO "${INSTANCENAME}  Complete Removing Local Tarball"
done

## purge old backups
f_INFO "Begin S3 Cleanup of files in $PURGEDESTINATION"
s3cmd del --recursive $PURGEDESTINATION
f_INFO "Complete S3 Cleanup of files in $PURGEDESTINATION"

### TODO
### Upload only the diff since the last upload to s3 rather than entire file 
