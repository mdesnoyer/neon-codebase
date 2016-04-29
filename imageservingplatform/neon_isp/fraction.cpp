/*
 * Fraction class
 *
 * Fraction data present in the mastermind json file
 * */
#include <iostream>
#include <sstream>
#include "neonException.h"
#include "fraction.h"
#include "neon_stats.h"

#define SMALLEST_FRACTION 0.001

Fraction::Fraction() { }

Fraction::~Fraction() { }

int
Fraction::Init(double floor, const rapidjson::Value& frac)
{
    try {
        int ret = InitSafe(floor, frac);
        // success
        if (ret == 0)
            return 0;
    }

    catch (...) {
    }

    return -1;
}


int
Fraction::InitSafe(double floor, const rapidjson::Value& frac)
{
    /*
     *  Percentage of being selected at random
     */
    if(frac.HasMember("pct") == false) {
        neon_stats[NEON_FRACTION_PARSE_ERROR]++;
        return -1; 
    }

    if(frac["pct"].IsDouble() == false) {
        neon_stats[NEON_FRACTION_PARSE_ERROR]++;
        return -1;;
    }

    pct = frac["pct"].GetDouble();
    
    // Check if pct is < the smallest decimal acceptable
    if (pct < SMALLEST_FRACTION)
        pct = 0.0;

    threshold = floor + pct;
    
    // Thumbnail ID
    if (frac.HasMember("tid") && frac["tid"].IsString()) {
        tid_.reset(new std::string(frac["tid"].GetString())); 
    }
    else { 
        neon_stats[NEON_FRACTION_PARSE_ERROR]++;
        return -1; 
    } 

    if(tid()->empty()) {
       neon_stats[NGINX_OUT_OF_MEMORY]++;
       return -1;
    }

    // Default URL
    if (frac.HasMember("default_url"))  {
        default_url_.reset(new std::string(frac["default_url"].GetString())); 
    }
    else if (frac.HasMember("base_url") && frac["base_url"].IsString()) { 
        base_url_ = frac["base_url"].GetString();
        if (base_url_.empty()) { 
           neon_stats[NGINX_OUT_OF_MEMORY]++;
           return -1;
        }

        // TODO Kevin when we delete the old code above, move the default url logic outta here
        if (frac.HasMember("default_size") == false) { 
            neon_stats[NEON_FRACTION_PARSE_ERROR]++;
            return -1; 
        } 
        const rapidjson::Value& defaultSize = frac["default_size"]; 
        if (defaultSize["h"].IsInt() && defaultSize["w"].IsInt()) {  
            default_url_.reset(new std::string(url_utils::GenerateUrl(base_url_, *tid(), frac["default_size"]["h"].GetInt(), frac["default_size"]["w"].GetInt()))); 
        }
        else { 
            neon_stats[NEON_FRACTION_PARSE_ERROR]++;
            return -1; 
        }
    } 
    else { 
        neon_stats[NEON_FRACTION_PARSE_ERROR]++;
        return -1;
    }
    
    if (frac.HasMember("imgs")) { 
        const rapidjson::Value& imgs = frac["imgs"];  
        return ProcessImages(imgs); 
    } 
    else if (frac.HasMember("img_sizes")) { 
        const rapidjson::Value& img_sizes = frac["img_sizes"];  
        return ProcessImages(img_sizes); 
    } 

    return 0;
}

int 
Fraction::ProcessImages(const rapidjson::Value & imgs) 
{ 
    if(imgs.IsArray() == false) {
        neon_stats[NEON_DEFAULT_THUMBNAIL_PARSE_ERROR]++;
        return -1;
    }

    rapidjson::SizeType numOfImages = imgs.Size();

    if(numOfImages == 0) {
        neon_stats[NEON_FRACTION_INVALID]++;
        return -1;
    }
    
    for(rapidjson::SizeType i=0; i < numOfImages; i++) {
        
        ScaledImage * img = new ScaledImage();

        images_.push_back(img); 
        
        // get image from the json array
        const rapidjson::Value& elem = imgs[i];
        
        // init the image instance with json elem
        int ret = img->Init(elem);
            
        if(ret != 0) {
            neon_stats[NEON_FRACTION_INVALID]++;
            return -1;
        }
    }
    return 0; 
}

// Iterate throgugh the images to find the appropriate image for a given
// height & width
const ScaledImage*
Fraction::GetScaledImage(int height, int width) const
{
    int image_index = ScaledImage::FindBestSizeMatchImage(width, height, images_);
    if (image_index > -1)
        return &images_[image_index];
    // no fit found
    return 0;
}

void
Fraction::SetPct(double pct){ 
    this->pct = pct;
}

double
Fraction::GetThreshold() const
{
    return threshold;
}


double
Fraction::GetPct() const
{
    return pct;
}

std::string * 
Fraction::default_url() const
{
    return default_url_.get();
}

std::string * 
Fraction::tid() const
{
    return tid_.get();
}

const std::string& 
Fraction::base_url() const 
{ 
    return base_url_; 
}
