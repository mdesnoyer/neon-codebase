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

bool
ScaledImage::ApproxEqualAspectRatio(int width, int height, double aspectRatio)
{
    if (height == 0) { 
        return false; 
    } 
    if (((double)width / (double)height) == aspectRatio) {
        return true; 
    } 
    else { 
        return false; 
    }
}

int 
ScaledImage::FindApproxAspectRatio(int width, int height, const boost::ptr_vector<ScaledImage>& imgs) 
{ 
    int image_index = -1;
    unsigned numOfImages = imgs.size();
     
    if(numOfImages == 0) {
        return image_index;
    }
    if (height == 0) { 
        return image_index; 
    } 
    double desiredAspectRatio = (double)width / (double)height; 
    long int min_matching_area = LONG_MAX; 
    for(unsigned i=0; i < numOfImages; i++) {
        int image_height = imgs[i].GetHeight();
        int image_width = imgs[i].GetWidth();
        if (ScaledImage::ApproxEqualAspectRatio(
             image_width, 
             image_height, 
             desiredAspectRatio)) { 
            long int matching_area = image_height*image_width; 
            if (matching_area < min_matching_area) { 
                image_index = i;
                min_matching_area = matching_area;  
            }
        }
    } 
    return image_index; 
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
    if (img.HasMember("url") && img["url"].IsString()) { 
        scoped_url_.reset(new std::string(img["url"].GetString())); 
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

std::string * 
ScaledImage::scoped_url() const
{
    return scoped_url_.get();
}
