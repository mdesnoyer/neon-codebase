/* 
 * Scaled Image class used to represent the Image object
 * in the json array of the mastermind file
 *
 * */

#ifndef _NEON_SCALE_IMAGE__
#define _NEON_SCALE_IMAGE__

#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include "rapidjson/document.h"
#include "limits.h"


class ScaledImage  {
    
public:
    ScaledImage();
    ~ScaledImage();
    
    int Init(const rapidjson::Value& img);
    void Shutdown();
 
    int GetHeight() const;
    int GetWidth () const;
    static bool ApproxEqual(int a, int b, int window);
    static bool ApproxEqualAspectRatio(int widthone, int heightone, int widthtwo, int heighttwo);
    int FindApproxAspectRatio(int width, int height) const; 
    static int FindBestSizeMatchImage(int width, int height, const boost::ptr_vector<ScaledImage>& imgs); 
    std::string * scoped_url() const;

protected:
    bool initialized;
    int height;
    int width;

private: 
    boost::scoped_ptr<std::string> scoped_url_; 
};



#endif


