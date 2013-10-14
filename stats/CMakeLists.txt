cmake_minimum_required(VERSION 2.8)
PROJECT(stats)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

######## All tests after this point #########
add_test(hourly_event_stats_mr_test
  ${PROJECT_SOURCE_DIR}/hourly_event_stats_mr_test.py
)