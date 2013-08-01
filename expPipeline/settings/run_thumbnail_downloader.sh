#!/bin/bash

OUTPUT_DIR=/data/neon/imdb/staging
INPUT_QUEUE=/data/neon/imdb/staging/requests.links

./thumbnail_extractor.py \
    --image_db ${OUTPUT_DIR}/image.db \
    --image_dir ${OUTPUT_DIR}/images \
    --input ${INPUT_QUEUE} \
    --failed_links ${OUTPUT_DIR}/failed_links.txt \
    --log ${OUTPUT_DIR}/thumbnail_extractor.log \
    > ${OUTPUT_DIR}/thumbnail_stdout.log 2> ${OUTPUT_DIR}/thumbnail_stderr.log