cmake_minimum_required(VERSION 2.8)
PROJECT(model)

include(${CMAKE_SOURCE_DIR}/Common.cmake)

include_directories(${utils_SOURCE_DIR})

add_library(TextDetection
  TextDetection.cc
)
target_link_libraries(TextDetection
  opencv_core
  opencv_imgproc
  opencv_features2d
  cvutils
  connectedcomponents
)

add_executable(ProfileTextDetection
  test/ProfileTextDetection.cc
)
target_link_libraries(ProfileTextDetection
  TextDetection
  gflags
  opencv_core
  opencv_highgui
  profiler
)

######## All tests after this point #########

add_cc_test(TextDetectionTest
  test/TextDetectionTest.cc
)
target_link_libraries(TextDetectionTest
  TextDetection
  opencv_highgui
)

######## All python builds go after this point ########
enable_python()

add_py_library(TextDetectionPy
  TextDetectionPy.cc
)
target_link_libraries(TextDetectionPy
  opencv_core
  opencv_imgproc
  TextDetection
)