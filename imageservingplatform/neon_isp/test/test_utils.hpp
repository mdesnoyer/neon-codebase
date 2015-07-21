#ifndef TEST_UTILS_HPP
#define TEST_UTILS_HPP 

#include <stdio.h> 
#include <stdlib.h> 
#include <string.h>
#include <unistd.h>
#include <libgen.h>

#include <iostream> 
#include <sstream> 
#include <fstream> 
#include <boost/scoped_ptr.hpp>

class TestUtils {
    public : 
        static std::string readTestFile(std::string fileName); 
}; 

#endif
