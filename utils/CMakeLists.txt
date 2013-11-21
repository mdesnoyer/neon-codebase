cmake_minimum_required(VERSION 2.8)
PROJECT(utils)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

add_library(cvutils STATIC
  cvutils.cc
)
target_link_libraries(cvutils
  opencv_highgui
  opencv_core
)

add_library(connectedcomponents STATIC
  connectedcomponents.cpp
)
target_link_libraries(connectedcomponents
  opencv_core
)

######## All tests after this point #########
add_test(options_test
  ${PROJECT_SOURCE_DIR}/test/options_test.py
)

add_test(connection_pool_test
  ${PROJECT_SOURCE_DIR}/test/connection_pool_test.py
)

add_test(ps_test
  ${PROJECT_SOURCE_DIR}/test/ps_test.py
)

add_test(statemon_test
  ${PROJECT_SOURCE_DIR}/test/statemon_test.py
)