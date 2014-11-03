#include <iostream>
#include "neonException.h"
#include "scaledImage.h"
#include "neon_stats.h"



ScaledImage::ScaledImage()
{
    height = 0;
    width = 0;
}


ScaledImage::~ScaledImage()
{
    height = 0;
    width = 0;
}


void
ScaledImage::Init(const rapidjson::Value& img)
{
    
    /*
     *  Image height
     */
    if(img.HasMember("h") == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        throw new NeonException("ScaledImage::Init: no height key found");
    }

    if(img["h"].IsInt() == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        throw new NeonException("ScaledImage::Init: height value isnt a json int");
    }

    height = img["h"].GetInt();
    

    /*
     *  Image width
     */
    if(img.HasMember("w") == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        throw new NeonException("ScaledImage::Init: no width key found");
    }

    if(img["w"].IsInt() == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        throw new NeonException("ScaledImage::Init: width value isnt a json int");
    }

    width = img["w"].GetInt();

    
    /*
     *  Image url
     */
    if(img.HasMember("url") == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        throw new NeonException("ScaledImage::Init: no url key found");
    }

    if(img["url"].IsString() == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        throw new NeonException("ScaledImage::Init: url value isnt a json string");
    }

    url = img["url"].GetString();
}


void
ScaledImage::Shutdown()
{
    // nothing to do
}


int
ScaledImage::GetHeight() const
{
    return height;
}


int
ScaledImage::GetWidth () const
{
    return width;
}


const char *
ScaledImage::GetUrl(int & size) const
{
    size = url.size();
    return url.c_str();
}


const std::string &
ScaledImage::GetUrlString() const
{
    return url;
}






