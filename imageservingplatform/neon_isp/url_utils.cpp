#include "include/url_utils.hpp" 

using namespace std; 

namespace url_utils
{ 
     string* 
     GenerateUrl(const string baseUrl, const string tid, int height, int width) 
     { 
         ostringstream oss("");
         oss << baseUrl;  
         if (*baseUrl.rbegin() != '/') 
             oss << "/"; 
         oss << "neontn" << tid << "_w" << width << "_h" << height << ".jpg"; 
         return new string(oss.str()); 
     }  
}
