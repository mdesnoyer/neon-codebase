cmake_minimum_required(VERSION 2.8)
PROJECT(mastermind)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

######## All tests after this point #########
add_test(core_test
  ${PROJECT_SOURCE_DIR}/test/core_test.py
)