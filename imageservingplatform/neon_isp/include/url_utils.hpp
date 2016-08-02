#ifndef UTILITY_HPP
#define UTILITY_HPP 

#include <iostream>
#include <sstream>
#include <string>  
#include <string.h>

namespace url_utils 
{ 
    std::string GenerateUrl(const std::string, const std::string, int, int, const char * queryString=""); 
}

#endif 
