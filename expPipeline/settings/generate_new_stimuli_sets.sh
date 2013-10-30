#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

STAGING_DIR=/data/neon/imdb/staging/
OUTPUT_DIR=${STAGING_DIR}/stimuli_set

# Find out the next index for the stimuli set to use
MAX_SET=0
for name in ${OUTPUT_DIR}/stimuli_* ; do
    bname=`basename ${name}`
    CUR_SET=`echo ${bname} | sed 's/stimuli_//g'`
    if [ "$CUR_SET" -gt "$MAX_SET" ] ; then
        MAX_SET=${CUR_SET}
    fi
done
NEXT_SET=`expr ${MAX_SET} + 1`

${THIS_DIR}/../generate_stimuli_set.py \
    --cache_dir /data/neon/cache/ \
    --start_index ${NEXT_SET} \
    --image_dir ${STAGING_DIR}/images \
    --stimuli_dir ${OUTPUT_DIR} \
    --output ${OUTPUT_DIR}/stimuli_%i \
    --image_db ${STAGING_DIR}/image.db \
    --codebook /data/neon/gist/07222013/gist_codebook.db \
    --example_urls /data/neon/imdb/staging/examples.urls \
    --new_urls ${STAGING_DIR}/requests.links \
    --log ${STAGING_DIR}/generate_stimuli.log \
    > ${STAGING_DIR}/generate_stimuli_stdout.log 2> ${STAGING_DIR}/generate_stimuli_stderr.log
