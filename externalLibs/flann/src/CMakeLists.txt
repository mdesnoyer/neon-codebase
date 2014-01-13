
if (BUILD_CPP_BINDINGS)
  add_subdirectory( cpp )
endif()

if (BUILD_MATLAB_BINDINGS)
    add_subdirectory( matlab )
endif()

if (BUILD_PYTHON_BINDINGS)
    add_subdirectory( python )
endif()
