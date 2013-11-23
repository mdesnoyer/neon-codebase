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
