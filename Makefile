BUILD_TYPE ?= Debug

CMAKE_FLAGS= -D CMAKE_BUILD_TYPE=$(BUILD_TYPE) 

# Create the build directory and invoke cmake
all:
	@mkdir -p build/${BUILD_TYPE}
	cd build/${BUILD_TYPE} && CXX=g++ CC=gcc cmake $(CMAKE_FLAGS) ../..
	cd build/${BUILD_TYPE} && make

# Convienience targets for different build types
release:
	make BUILD_TYPE=Release
debug:
	make BUILD_TYPE=Debug
profile:
	make BUILD_TYPE=Profile
RelWithDebInfo:
	make BUILD_TYPE=RelWithDebInfo

clean:
	-cd build/${BUILD_TYPE} && make clean
	rm externalLibs/flann/lib/libflann*
	rm -rf build bin lib

test: all
	cd build/${BUILD_TYPE} && make -k $@
	nosetests --exe

