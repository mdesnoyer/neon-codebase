# We need pthread's
if(UNIX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -fPIC")
endif()

# Turn on C++ 11 and warning 
if(UNIX)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=gnu++0x")
endif()

# Add a define so that the code can know if we're building the pure debug mode
SET( CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -D_DEBUG" )
SET( CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG -D_DEBUG" )

# Define new build type for profiling
SET( CMAKE_CXX_FLAGS_PROFILE "${CMAKE_CXX_FLAGS_RELEASE} -g" CACHE STRING
  "Flags used by the C++ compiler during profile builds")
SET( CMAKE_C_FLAGS_PROFILE "${CMAKE_C_FLAGS_RELEASE} -g" CACHE STRING
    "Flags used by the C compiler during profile builds."
    FORCE )
SET( CMAKE_EXE_LINKER_FLAGS_PROFILE
    "${CMAKE_EXE_LINKER_FLAGS_RELEASE} -lprofiler" CACHE STRING
    "Flags used for linking binaries during profile builds."
    FORCE )
SET( CMAKE_SHARED_LINKER_FLAGS_PROFILE
    "${CMAKE_SHARED_LINKER_FLAGS_RELEASE} -lprofiler" CACHE STRING
    "Flags used by the shared libraries linker during profile builds."
    FORCE )
MARK_AS_ADVANCED(
    CMAKE_CXX_FLAGS_PROFILE
    CMAKE_C_FLAGS_PROFILE
    CMAKE_EXE_LINKER_FLAGS_PROFILE
    CMAKE_SHARED_LINKER_FLAGS_PROFILE )

# Update the documentation string of CMAKE_BUILD_TYPE for GUIs
SET( CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
    "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Profile."
    FORCE )

# Set the linking to include local because sometimes it's not set
SET(CMAKE_INCLUDE_CURRENT_DIR ON)
LINK_DIRECTORIES(/usr/local/lib)

#if(COMMAND cmake_policy)
#  cmake_policy(SET CMP0003 NEW)
#endif(COMMAND cmake_policy)

# if -D CMAKE_BUILD_TYPE=<blah> is not set, make it default.
IF((NOT DEFINED CMAKE_BUILD_TYPE) OR (CMAKE_BUILD_TYPE STREQUAL ""))
  SET(CMAKE_BUILD_TYPE Debug)
ENDIF((NOT DEFINED CMAKE_BUILD_TYPE) OR (CMAKE_BUILD_TYPE STREQUAL ""))

# By default, build shared libraries
set(BUILD_SHARED_LIBS ON)

# Set the build directories
string(REGEX REPLACE ${CMAKE_SOURCE_DIR} "" REL_PATH ${PROJECT_SOURCE_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib/${REL_PATH})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/${REL_PATH})

# For convienience, make symlinks from /bin and /lib to the most recent build
execute_process(COMMAND ${CMAKE_COMMAND} -E remove ${PROJECT_SOURCE_DIR}/bin)
execute_process(COMMAND ${CMAKE_COMMAND} -E remove ${PROJECT_SOURCE_DIR}/lib)
execute_process(COMMAND ln -s -f ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} ${PROJECT_SOURCE_DIR}/bin)
execute_process(COMMAND ln -s -f ${CMAKE_LIBRARY_OUTPUT_DIRECTORY} ${PROJECT_SOURCE_DIR}/lib)

# Turn on the ability to add tests
enable_testing()

# Convienience macro for adding a c++ test
function(add_cc_test name)
  include_directories(${gtest_SOURCE_DIR}/include)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} gtest_main gtest)
  add_test(${name}_test
    WORKING_DIR ${PROJECT_SOURCE_DIR}/test
    ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${name}
  )
endfunction(add_cc_test)

# Function that enables this library to generate python interfaces for c++ code
macro(enable_python)
  find_package(PythonLibs REQUIRED)
  include_directories(${PYTHON_INCLUDE_PATH})
  SET(PYTHON_EXECUTABLE /usr/bin/python)
  find_package(Numpy REQUIRED)
  include_directories(${PYTHON_NUMARRAY_INCLUDE_DIR})

  # Initialize the __init__.py files so that py libraries can live in lib
  #file(WRITE ${PROJECT_SOURCE_DIR}/__init__.py "from lib import *\n")
  file(WRITE ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/__init__.py "__all__ = []\n")
endmacro(enable_python)

# Convienience macro for adding python interfaces to libraries
function(add_py_library name)
  add_library(${name} ${ARGN})
  set_target_properties(${name} PROPERTIES PREFIX "")
  target_link_libraries(${name} ${PYTHON_LIBRARIES})

  # Add this library to __init__.py so that it can be imported
  file(APPEND ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/__init__.py
    "__all__.append('${name}')\n")
endfunction(add_py_library)