#!/bin/bash
ps aux | grep "python client.py*" | awk '{print $2}' | xargs kill -9

