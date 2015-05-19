#!/bin/bash
echo "/usr/local/lib64" >/etc/ld.so.conf.d/cuda.conf
ldconfig

