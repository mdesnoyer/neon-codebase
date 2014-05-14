#!/bin/bash

 case $1 in
    start)
	   cd /opt/neon/	
	   . enable_env	
	   nohup python /opt/neon/controllers/brightcove_controller.py -c /opt/neon/config/neon-prod.conf --utils.logs.file /mnt/logs/neon/bc_controller.log 2>/dev/null & 
	   echo $! > /var/run/bcontroller.pid
	   ;;
     stop)  
       kill `cat /var/run/bcontroller.pid` ;;
     *)  
       echo "usage: bcontroller.sh {start|stop}" ;;
 esac
 exit 0
