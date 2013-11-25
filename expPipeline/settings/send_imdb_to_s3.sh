#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

${THIS_DIR}/../send_imdb_to_s3.py \
    -i /data/neon/imdb/staging