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
