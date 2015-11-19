include(Common.cmake)

# Set the build directory
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)

# Install the python dependencies into that virtual environment. We
# don't set this up as a target because it must be installed so that
# the dependencies can find where the python pieces are installed
# (e.g. numpy)
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  message("You are on OSX. We can't install the python dependencies properly because some have to be compiled and compiling on a Mac is a nightmare. So, you have to manually install the dependencies in pre_requirements.txt and requriements.txt. MacPorts should help. Or you can just get a real developers' OS ...")

elseif(EXISTS $ENV{VIRTUAL_ENV})
  message("Installing python dependencies.")
  execute_process(
    COMMAND ${CMAKE_SOURCE_DIR}/.pyenv/bin/pip install -r ${CMAKE_SOURCE_DIR}/pre_requirements.txt --no-index --find-links http://s3-us-west-1.amazonaws.com/neon-dependencies/index.html
    RESULT_VARIABLE FAILED_PY_INSTALL
    )
  endif(FAILED_PY_INSTALL)
  if(NOT FAILED_PY_INSTALL)
    execute_process(
      COMMAND ${CMAKE_SOURCE_DIR}/.pyenv/bin/pip install -r ${CMAKE_SOURCE_DIR}/requirements.txt --no-index --find-links http://s3-us-west-1.amazonaws.com/neon-dependencies/index.html
    
      RESULT_VARIABLE FAILED_PY_INSTALL
      )
  if(NOT FAILED_PY_INSTALL)
    execute_process(
    COMMAND ${CMAKE_SOURCE_DIR}/.pyenv/bin/pip install -r ${CMAKE_SOURCE_DIR}/post_requirements.txt --no-index --find-links http://s3-us-west-1.amazonaws.com/neon-dependencies/index.html
    RESULT_VARIABLE FAILED_PY_INSTALL
      )
  endif(NOT FAILED_PY_INSTALL)
  if(FAILED_PY_INSTALL)
    message(FATAL_ERROR "Error installing the python dependencies. Stopping.")
  endif(FAILED_PY_INSTALL)

  # Install the python cv2 hooks. They need to be installed globally
  # for this to work
  find_file(GLOBAL_PY python
    PATHS /usr/local/bin /usr/bin /opt/local/bin
    NO_DEFAULT_PATH
    NO_CMAKE_ENVIRONMENT_PATH
    NO_CMAKE_PATH
    NO_SYSTEM_ENVIRONMENT_PATH
    NO_CMAKE_SYSTEM_PATH)
  if(GLOBAL_PY-NOTFOUND)
    message(FATAL_ERROR "Could not find the global python executable")
  endif(GLOBAL_PY-NOTFOUND)
  execute_process(COMMAND ${GLOBAL_PY} -c "import cv2; print cv2.__file__"
    OUTPUT_VARIABLE PYCV2_FILE
    RESULT_VARIABLE NOTFOUND_PYCV2
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
  if(NOTFOUND_PYCV2)
    message(FATAL_ERROR "Could not find cv2.so. Did you install OpenCV globally yet? Log: " ${PYCV2_FILE})
  endif(NOTFOUND_PYCV2)
  execute_process(
    COMMAND ${CMAKE_SOURCE_DIR}/.pyenv/bin/python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())"
    OUTPUT_VARIABLE PY_PACKAGE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
  file(COPY ${PYCV2_FILE} DESTINATION ${PY_PACKAGE_DIR} )
else(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  message(FATAL_ERROR "Not in a virtual environment. Please run 'source enable_env'")
endif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
