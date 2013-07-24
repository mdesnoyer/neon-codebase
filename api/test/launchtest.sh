model_dir=$1
if [ $# -ne 1 ] ; then
	echo "sh launchtest.sh <model_dir_abs_path>"
	exit
fi

#start neon api server
echo "starting neon api server"
nohup python ../server.py &
spid=$!

#start test server
echo "starting test server"
nohup python testserver.py & 
tpid=$!

#start test
sleep 5
curl localhost:8082/integrationtest?test=neon

#start clients
nohup python ../client.py --model_dir=$model_dir --local=True &
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
	sleep 10
done

#shutdown all
kill -9 $cpid $spid $tpid

