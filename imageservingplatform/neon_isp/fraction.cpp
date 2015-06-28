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

Fraction::Fraction()
{
   defaultURL = 0;
   tid = 0;  
   initialized = false;
}


Fraction::~Fraction()
{
     defaultURL = 0;
     tid = 0;
     initialized = false;
}

int
Fraction::Init(double floor, const rapidjson::Value& frac)
{
    try {
        int ret = InitSafe(floor, frac);

        // success
        if (ret == 0)
            return 0;
    }

    // on any error, clean up 
    catch (NeonException * e) {
    }
    catch (std::bad_alloc e) {
    }
    catch (...) {
    }
    
    Dealloc();
    return -1;
}


int
Fraction::InitSafe(double floor, const rapidjson::Value& frac)
{
    if(initialized == true) {
        neon_stats[NEON_FRACTION_INVALID_INIT]++;
        return -1;
    }

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
    if (frac.HasMember("tid") == false) {
        neon_stats[NEON_FRACTION_PARSE_ERROR]++;
        return -1; 
    }

    tid = 0;
    tid = strdup(frac["tid"].GetString());

    if(tid == 0) {
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
            default_url_.reset(new std::string(utility::generateUrl(base_url_, tid, frac["default_size"]["h"].GetInt(), frac["default_size"]["w"].GetInt()))); 
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

    initialized = true;
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
        // store immediately so that Shutdown can delete it in case of error
        images.push_back(img);
        
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

void
Fraction::Shutdown()
{
    if(initialized == false) {
        neon_stats[NEON_FRACTION_INVALID_SHUTDOWN]++;
        return;
    }
    
    Dealloc();
    initialized = false;
}


void
Fraction::Dealloc()  {

    for(std::vector<ScaledImage*>::iterator it = images.begin(); it != images.end(); it ++)
    {
        ScaledImage * img = (*it);
        (*it) = 0;

        if(img == NULL) {
            neon_stats[NEON_SCALED_IMAGE_SHUTDOWN_NULL_POINTER]++;
            continue;
        }

        img->Shutdown();
        delete img;
        img = 0;
    }   

    if(defaultURL != 0)
        free((void *)defaultURL);
    if(tid != 0)
        free((void *)tid);
}

// Iterate throgugh the images to find the appropriate image for a given
// height & width
// TODO Kevin this needs to be combined with DefaultThumbnail::GetScaledImage
ScaledImage*
Fraction::GetScaledImage(int height, int width) const{

    static const int pixelRange = 6; 

    // go through all images again and pick the first approximate fit
    unsigned numOfImages = images.size();

    if(numOfImages == 0)
        return 0; 

    // try to find an exact fit
    for(unsigned i=0; i < numOfImages; i++){
        if(images[i]->GetHeight() == height &&
           images[i]->GetWidth() == width)
             return images[i];
    }

    // otherwise try to find pick an approximate fit
    for(unsigned i=0; i < numOfImages; i++){
        if(ScaledImage::ApproxEqual(images[i]->GetHeight(), height, pixelRange) &&
           ScaledImage::ApproxEqual(images[i]->GetWidth(), width, pixelRange)) { 
            return images[i];
        } 
    }
    
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

const char *
Fraction::GetThumbnailID() const
{
    return tid;
}

const std::string& 
Fraction::base_url() const 
{ 
    return base_url_; 
}
