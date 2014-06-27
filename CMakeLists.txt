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
add_subdirectory(supportServices)
add_subdirectory(api)
add_subdirectory(imageservingplatform/neon_isp)
#add_subdirectory(testinclude(ExternalProject)


externalproject_add(Nginx
    SOURCE_DIR ${PROJECT_SOURCE_DIR}/imageservingplatform/nginx-1.4.7/
    CONFIGURE_COMMAND ${PROJECT_SOURCE_DIR}/imageservingplatform/nginx-1.4.7/configure --add-module=../neon_isp/ --with-http_realip_module
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
