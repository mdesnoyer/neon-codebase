#include <iostream>
#include <sstream>
#include "neonException.h"
#include "scaledImage.h"
#include "neon_stats.h"

ScaledImage::ScaledImage()
{
    height = 0;
    width = 0;
    initialized = false;
    needsUrlGenerated = false; 
}

ScaledImage::~ScaledImage()
{
    height = 0;
    width = 0;
    initialized = false;
}

// check if a & b approx equal i.e in the range of the window size specified 
bool
ScaledImage::ApproxEqual(int a, int b, int window){
    if (abs(a - b) <= window)
        return true;
    else
        return false;
}

int
ScaledImage::Init(const rapidjson::Value& img)
{
    if(initialized == true) {
        neon_stats[NEON_SCALED_IMAGE_INVALID_INIT]++;
        return -1;
    }
    
    /*
     *  Image height
     */
    if(img.HasMember("h") == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        return -1;
    }

    if(img["h"].IsInt() == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        return -1;
    }

    height = img["h"].GetInt();
    

    /*
     *  Image width
     */
    if(img.HasMember("w") == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        return -1;
    }

    if(img["w"].IsInt() == false) {
        neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
        return -1;
    }

    width = img["w"].GetInt();

    
    /*
     *  Image url
     */
    if (this->needsUrlGenerated == false) { 
        if(img.HasMember("url") == false) {
	    neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
	    return -1;
	}

	if(img["url"].IsString() == false) {
	    neon_stats[NEON_SCALED_IMAGE_PARSE_ERROR]++;
	    return -1;
	}
	url = strdup(img["url"].GetString());
    } 
    
    initialized = true;
    return 0;
}

void
ScaledImage::Shutdown()
{
    if(initialized == false) { 
        neon_stats[NEON_SCALED_IMAGE_INVALID_SHUTDOWN]++;
        return;
    }

    initialized = false;
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

const char *
ScaledImage::GenerateUrl(std::string baseUrl, std::string tid, int h, int w) const
{
    std::ostringstream ss(""); 
    ss << baseUrl << "/" << tid << "_" << h << "_" << w << ".jpg"; 
    return ss.str().c_str(); 
}
