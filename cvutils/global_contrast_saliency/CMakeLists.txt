cmake_minimum_required (VERSION 2.8)
project (saliency)

find_package(OpenCV REQUIRED)
file(GLOB_RECURSE saliency_SOURCES "CmLib/*.cpp")
file(GLOB_RECURSE saliency_HEADERS "CmLib/*.h" "include/*.h")
file(GLOB_RECURSE saliencyexample_SOURCES "Saliency/*.cpp")

find_package(Boost REQUIRED) # COMPONENTS regex system filesystem)
set (saliency_INCLUDE_DIRS "")
foreach (_headerFile ${saliency_HEADERS})
    get_filename_component(_dir ${_headerFile} PATH)
    list (APPEND saliency_INCLUDE_DIRS ${_dir})
endforeach()
list(REMOVE_DUPLICATES saliency_INCLUDE_DIRS)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")

include_directories(${saliency_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR} ${Boost_INCLUDE_DIRS})
add_executable (saliencyExe ${saliency_SOURCES} ${saliencyexample_SOURCES})
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/apclusterlib)
target_link_libraries(saliencyExe ${OpenCV_LIBS} ${Boost_LIBRARIES} ${CMAKE_CURRENT_SOURCE_DIR}/apclusterlib/apclusterunix64.so)
	