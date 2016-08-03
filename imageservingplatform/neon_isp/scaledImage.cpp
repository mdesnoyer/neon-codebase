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
ScaledImage::ApproxEqualAspectRatio(int widthone, int heightone, int widthtwo, int heighttwo)
{
    if (heightone == 0 || heighttwo == 0) { 
        return false; 
    }
    if (abs(widthone*heighttwo-heightone*widthtwo) < (heighttwo * 3)) { 
        return true; 
    } 
    else { 
        return false; 
    }
}

int 
ScaledImage::FindBestSizeMatchImage(int width, int height, const boost::ptr_vector<ScaledImage>& imgs) 
{
    int pixel_window = 6;  
    int exact_match_index = -1;
    int near_match_index = -1; 
    int aspect_match_index = -1;
    int final_index = -1;
      
    unsigned numOfImages = imgs.size();
     
    if(numOfImages == 0) {
        return final_index;
    }
    if (height == 0) { 
        return final_index; 
    }
 
    int min_matching_width = INT_MAX; 
    for(unsigned i=0; i < numOfImages; i++) {
        int image_height = imgs[i].GetHeight();
        int image_width = imgs[i].GetWidth();
        if( image_height ==  height && image_width == width ) {
            exact_match_index = i; 
        }
        if (ScaledImage::ApproxEqual(image_height, height, pixel_window) && 
             ScaledImage::ApproxEqual(image_width, width, pixel_window)) { 
            near_match_index = i;
        } 
        if (ScaledImage::ApproxEqualAspectRatio(
             image_width, 
             image_height, 
             width, 
             height)) { 
            int matching_width_diff = abs(width-image_width);
            if (matching_width_diff < min_matching_width) { 
                aspect_match_index = i;
                min_matching_width = matching_width_diff;  
            }
        }
    }

    // if we found an exaxt match return it, 
    //  next see if we have a really close match, 
    //  finally see if we have an aspect ratio match

    if (exact_match_index > -1) {
        final_index = exact_match_index; 
    }
    else if (near_match_index > -1) { 
        final_index = near_match_index; 
    } 
    else if (aspect_match_index > -1) { 
        final_index = aspect_match_index; 
    } 
    return final_index; 
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
