#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

LOG_DIR=/data/neon/imdb/staging/

${THIS_DIR}/../post_new_stimuli.py \
    --stimuli_dir /data/neon/imdb/staging/temp_stimuli_sets/ \
    --mturk_dir ${THIS_DIR}/../../../mturk_thumbnails \
    --pay 1.00 \
    --assignments 30 \
    --log ${LOG_DIR}/post_stimuli.log \
    > ${LOG_DIR}/post_stimuli_stdout.log 2> ${LOG_DIR}/post_stimuli_stderr.log