cmake_minimum_required(VERSION 2.8)
PROJECT(supportServices)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

######## All tests after this point #########
add_test(services_test
  ${PROJECT_SOURCE_DIR}/tests/test_services.py
)
