cmake_minimum_required(VERSION 2.8)
PROJECT(cvutils)

find_package( OpenCV REQUIRED )

include(${CMAKE_SOURCE_DIR}/Common.cmake)

include_directories(${utils_SOURCE_DIR})

add_library(TextDetection
  TextDetection.cc
)
target_link_libraries(TextDetection
  ${OpenCV_LIBS}
  cvutils
  connectedcomponents
)

add_executable(ProfileTextDetection
  test/ProfileTextDetection.cc
)
target_link_libraries(ProfileTextDetection
  ${OpenCV_LIBS}
  TextDetection
  gflags
  profiler
)

######## All tests after this point #########

add_cc_test(TextDetectionTest
  test/TextDetectionTest.cc
)
target_link_libraries(TextDetectionTest
  opencv_core
  TextDetection
  opencv_highgui
)

######## All python builds go after this point ########
enable_python()

#add_py_library(TextDetectionPy
#  TextDetectionPy.cc
#)
#target_link_libraries(TextDetectionPy
#  opencv_core
#  opencv_imgproc
#  TextDetection
#)
