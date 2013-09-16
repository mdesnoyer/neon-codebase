cmake_minimum_required(VERSION 2.8)
PROJECT(neon)

include(Top.cmake)

add_subdirectory(model)
add_subdirectory(externalLibs)
add_subdirectory(utils)