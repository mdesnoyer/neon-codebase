configure_file( setup.py.tpl setup.py )

install( DIRECTORY pyflann DESTINATION share/flann/python )
install( FILES ${CMAKE_CURRENT_BINARY_DIR}/setup.py DESTINATION share/flann/python )

# python instalation
if (PYTHON_EXECUTABLE)
    #install(CODE "execute_process(
    #    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/setup.py install
    #    WORKING_DIRECTORY \"${CMAKE_CURRENT_SOURCE_DIR}\")")
  add_custom_target(pyflann_install ALL
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/setup.py install
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Installing pyflann"
  )
  if (BUILD_C_BINDINGS)
    add_dependencies(pyflann_install
      flann
    )
  endif(BUILD_C_BINDINGS)
endif(PYTHON_EXECUTABLE)
