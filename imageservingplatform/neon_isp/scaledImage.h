/* 
 * Scaled Image class used to represent the Image object
 * in the json array of the mastermind file
 *
 * */

#ifndef _NEON_SCALE_IMAGE__
#define _NEON_SCALE_IMAGE__

#include <string>
#include "rapidjson/document.h"


class ScaledImage  {
    
public:
    ScaledImage();
    ~ScaledImage();
    
    int Init(const rapidjson::Value& img);
    void Shutdown();
 
    int GetHeight() const;
    int GetWidth () const;
    const char * GetUrl(int & size) const;
    const std::string & GetUrlString() const;
    static bool ApproxEqual(int a, int b, int window);
    
    bool needsUrlGenerated; 

protected:
    bool initialized;
    int height;
    int width;
    std::string url;
};



#endif


