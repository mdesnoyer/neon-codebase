#include "include/url_utils.hpp" 

using namespace std; 

namespace url_utils
{ 
     string 
     GenerateUrl(const string baseUrl, const string tid, int height, int width) 
     { 
         ostringstream oss(""); 
         oss << baseUrl << "neontn" << tid << "_w" << width << "_h" << height << ".jpg"; 
         return oss.str(); 
     }  
}
