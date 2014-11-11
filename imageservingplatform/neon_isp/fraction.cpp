/*
 * Fraction class
 *
 * Fraction data present in the mastermind json file
 * */


#include <iostream>
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
    if (frac.HasMember("default_url") == false)  {
        neon_stats[NEON_FRACTION_PARSE_ERROR]++;
        return -1;
    }

    defaultURL = strdup(frac["default_url"].GetString()); 

    // Thumbnail ID
    if (frac.HasMember("tid") == false) {
        neon_stats[NEON_FRACTION_PARSE_ERROR]++;
        return -1; 
    }

    tid = strdup(frac["tid"].GetString());

    /*
     *  Images array
     */
    const rapidjson::Value& imageArray = frac["imgs"];
    
    if(imageArray.IsArray() == false) {
        neon_stats[NEON_FRACTION_PARSE_ERROR]++;
        return -1;
    }

    rapidjson::SizeType numOfImages = imageArray.Size();
    
    if(numOfImages == 0) {
        neon_stats[NEON_FRACTION_INVALID]++;
        return -1;
    }

    images.reserve(numOfImages);
    
    // parse all images
    for(rapidjson::SizeType i=0; i < numOfImages; i++) {
        
        ScaledImage * img = new ScaledImage();
        
        // store immediately so that Shutdown can delete it in case of error
        images.push_back(img);
        
        // get image from the json array
        const rapidjson::Value& elem = imageArray[i];
        
        // init the image instance with json elem
        int ret = img->Init(elem);
            
        if(ret != 0) {
            neon_stats[NEON_FRACTION_INVALID]++;
            return -1;
        }
    }

    initialized = true;
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


// check if a & b approx equal i.e in the range of the window size specified 
bool
Fraction::ApproxEqual(int a, int b, int window){
    if (abs(a - b) <= window)
        return true;
    else
        return false;
}

// Iterate throgugh the images to find the appropriate image for a given
// height & width

ScaledImage*
Fraction::GetScaledImage(int height, int width) const{

    for(unsigned i=0; i < images.size(); i++){
        
        if(ApproxEqual(images[i]->GetHeight(), height, 6) &&
           ApproxEqual(images[i]->GetWidth(), width, 6))
            return images[i];
    }
    
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

