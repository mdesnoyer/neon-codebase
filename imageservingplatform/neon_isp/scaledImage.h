#ifndef _NEON_SCALE_IMAGE__
#define _NEON_SCALE_IMAGE__

#include <string>
#include "rapidjson/document.h"


class ScaledImage  {
    
    
public:
    
    ScaledImage();
    ~ScaledImage();
    
    
    void Init(const rapidjson::Value& img);
    void Shutdown();
 
    int GetHeight() const;
    int GetWidth () const;
    const char * GetUrl(int & size) const;
    const std::string & GetUrlString() const;
    
protected:
    
    int height;
    int width;
    std::string url;
};



#endif


