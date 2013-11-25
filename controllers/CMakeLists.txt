cmake_minimum_required(VERSION 2.8)
PROJECT(controllers)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

######## All tests after this point #########
add_test(brightcove_controller_test
  ${PROJECT_SOURCE_DIR}/test/test_brightcove_controller.py
)
