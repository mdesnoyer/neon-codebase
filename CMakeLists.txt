cmake_minimum_required(VERSION 2.8)
PROJECT(neon)

include(Top.cmake)

# TODO: Right now, you have to include directories that are depended
# on first because otherwise their export variables aren't set
# properly. Need to figure out how to fix this.
add_subdirectory(externalLibs)
add_subdirectory(utils)
add_subdirectory(model)
add_subdirectory(stats)
add_subdirectory(mastermind)
add_subdirectory(clickTracker)
add_subdirectory(controllers)
add_subdirectory(cmsdb)
add_subdirectory(cmsapi)
add_subdirectory(api)
add_subdirectory(imageservingplatform/neon_isp)

include(ExternalProject)
externalproject_add(Nginx
    PREFIX ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/Nginx
    SOURCE_DIR ${PROJECT_SOURCE_DIR}/imageservingplatform/nginx-1.6.2/
    UPDATE_COMMAND ""  # Stop it from updating because we want a static repo
    CONFIGURE_COMMAND
    ${PROJECT_SOURCE_DIR}/imageservingplatform/nginx-1.6.2/configure
    --add-module=../neon_isp/ --with-http_realip_module
    --prefix=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    BUILD_COMMAND make
    BUILD_IN_SOURCE 1
)

#After the ExternalProject_Add call for Nginx 
#ExternalProject_Add_Step(Nginx forceconfigure
#    COMMAND ${CMAKE_COMMAND} -E echo "Force configure of nginx"
#    DEPENDEES update
#    DEPENDERS configure
#    ALWAYS 1
#    ))
