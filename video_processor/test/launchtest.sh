#!/bin/bash
model_file=$1
if [ $# -ne 1 ] ; then
	echo "sh launchtest.sh <model_file_abs_path>"
	exit
fi

#start neon api server
echo "starting neon api server"
../server.py &
spid=$!

#start test server
echo "starting test server"
./testserver.py & 
tpid=$!

#start test
sleep 5
curl localhost:8082/integrationtest?test=neon

#start clients
../client.py --model_file=$model_file --local --debug &
cpid=$!

#poll for status 
echo "poll for status"
while true; do
	code=$(curl --write-out %{http_code} --silent --output /dev/null http://localhost:8082/teststatus)
	if [ $code -eq 200 ]; then
		echo "Test Success"
		break
	fi
	if [ $code -eq 501 ]; then
		echo "Test Failed"
		break
	fi
  #if [ $code -eq 502 ]; then
#		echo "Test Failed"
#		break
#	fi

  if ! ps -p $cpid > /dev/null; then
      echo "Client died. Test failed."
      break
  fi
	sleep 10
done

#shutdown all
kill -9 $cpid $spid $tpid

