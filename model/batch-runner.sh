IP=10.0.3.125
INPUT=/data/targ/images
UNTIL=1000
for c in 10 22 50 100 250 500 1000 10000; do
  echo concurrency=$c
  model/runner.py \
    --ip $IP \
    --until $UNTIL \
    --quiet \
    --concurrency $c \
    $INPUT 2>/dev/null # Assuming the execution is perfect!
done
