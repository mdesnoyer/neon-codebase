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

    // Default URL
    if (frac.HasMember("default_url"))  {
        defaultURL = 0;
        defaultURL = strdup(frac["default_url"].GetString());
    
        if(defaultURL == 0) {
           neon_stats[NGINX_OUT_OF_MEMORY]++;
           return -1;
        }
    }
    else if (frac.HasMember("base_url") && frac["base_url"].IsString()) { 
        baseUrl = frac["base_url"].GetString();
        if (baseUrl.empty()) { 
           neon_stats[NGINX_OUT_OF_MEMORY]++;
           return -1;
        }
        //we don't core dump let's generate a default url 
        defaultURL = strdup(this->GenerateDefaultUrl(frac).c_str());  
    } 
    else { 
        neon_stats[NEON_FRACTION_PARSE_ERROR]++;
        return -1;
    }

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
    
    /*
     *  Images array
     */
    if (frac.HasMember("imgs")) { 
        const rapidjson::Value& imgs = frac["imgs"];  
        return ProcessImages(imgs, false); 
    } 
    else if (frac.HasMember("img_sizes")) { 
        const rapidjson::Value& img_sizes = frac["img_sizes"];  
        return ProcessImages(img_sizes, true); 
    } 
    
    initialized = true;
    return 0;
}


// TODO Kevin refactor this/ with the identically named defaultThumbnail function, these things do the same thing
int 
Fraction::ProcessImages(const rapidjson::Value & imgs, bool needsUrlGenerated) 
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
        img->needsUrlGenerated = needsUrlGenerated;    
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

const char *
Fraction::GetDefaultURL() const
{
    return defaultURL;
}

const char *
Fraction::GetThumbnailID() const
{
    return tid;
}

std::string 
Fraction::GetBaseUrl() const{ 
    return baseUrl; 
}

// TODO Kevin exceptions, also this should be combined with the scaledImage::GenerateUrl function 
std::string
Fraction::GenerateDefaultUrl(const rapidjson::Value& frac) {
    std::ostringstream ss(""); 
    int height = frac["default_size"]["h"].GetInt();  
    int width = frac["default_size"]["w"].GetInt(); 
    std::string baseUrl = frac["base_url"].GetString(); 
    std::string tid = frac["tid"].GetString(); 
    
    ss << baseUrl << "/" << tid << "_" << height << "_" << width << ".jpg"; 
       
    return ss.str();
} 

