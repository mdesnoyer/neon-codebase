#!/bin/bash
IN=$1
OUT=$2
if [ $# -eq 0 ]
	then
		echo "./minifier input output_fname"; exit 0
fi	

curl -s -d compilation_level=SIMPLE_OPTIMIZATIONS \
	-d output_format=text \
	-d output_info=compiled_code \
	--data-urlencode "js_code@${IN}" http://closure-compiler.appspot.com/compile \
    > $OUT	
