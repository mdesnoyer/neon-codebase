cmake_minimum_required(VERSION 2.8)
PROJECT(externalLibs)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

add_subdirectory(gtest-1.6.0)

if(NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  add_subdirectory(flann)
endif(NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")