#!/bin/bash

 case $1 in
    start)
	   cd /opt/neon/	
	   . enable_env	
	   nohup python /opt/neon/mastermind/server.py -c /opt/neon/config/neon-prod.conf --utils.logs.file /mnt/logs/neon/mastermind.log 2>/dev/null & 
	   echo $! > /var/run/mastermind.pid
	   ;;
     stop)  
       kill `cat /var/run/mastermind.pid` ;;
     *)  
       echo "usage: mastermind.sh {start|stop}" ;;
 esac
 exit 0
