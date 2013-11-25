#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

LOG_DIR=/data/neon/imdb/staging/

${THIS_DIR}/../post_new_stimuli.py \
    --stimuli_dir /data/neon/imdb/staging/stimuli_set/ \
    --mturk_dir ${THIS_DIR}/../../../mturk_thumbnails \
    --pay 0.40 \
    --assignments 30 \
    --log ${LOG_DIR}/post_stimuli.log \
    --max_dynos_allowed 8 \
    --max_hits 5 \
    > ${LOG_DIR}/post_stimuli_stdout.log 2> ${LOG_DIR}/post_stimuli_stderr.log
