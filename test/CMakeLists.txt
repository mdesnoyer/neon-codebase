cmake_minimum_required(VERSION 2.8)
PROJECT(test)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

######## All tests after this point #########
add_test(serving_tester
  ${PROJECT_SOURCE_DIR}/serving_tester.py -c ${PROJECT_SOURCE_DIR}/local_tester.conf
)