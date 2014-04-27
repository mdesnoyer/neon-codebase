cmake_minimum_required(VERSION 2.8)
PROJECT(test)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

######## All tests after this point #########

# Disable this test for now because the data pipeline is being changed
# significantly.
# TODO(mdesnoyer): Get a new integration test
#add_test(serving_tester
#  ${PROJECT_SOURCE_DIR}/serving_tester.py
#)