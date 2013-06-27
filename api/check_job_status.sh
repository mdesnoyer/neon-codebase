#!/bin/bash
id=$1
if [ "$#" -eq 1 ]
	then curl http://ec2-23-22-138-110.compute-1.amazonaws.com:8081/api/jobstatus?api_key=a63728c09cda459c3caaa158f4adff49\&job_id=$id
	echo "\n"
	exit 0
fi
echo "Enter the job id \n"
