cmake_minimum_required(VERSION 2.8)
PROJECT(api)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

######## All tests after this point #########
add_test(video_server_test
  ${PROJECT_SOURCE_DIR}/test/test_video_server.py
)

add_test(video_client_test
  ${PROJECT_SOURCE_DIR}/test/test_video_client.py
)
