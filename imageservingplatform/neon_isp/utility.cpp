#include "include/utility.hpp" 

using namespace std; 

namespace utility 
{ 
     string 
     generateUrl(string baseUrl, string tid, int height, int width) 
     { 
         ostringstream oss(""); 
         oss << baseUrl << "neontn" << tid << "_w" << width << "_h" << height << ".jpg"; 
         return oss.str(); 
     }  
}
