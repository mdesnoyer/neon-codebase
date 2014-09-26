/*
 * Fraction class
 *
 * Fraction data present in the mastermind json file
 * */


#include <iostream>
#include "neonException.h"
#include "fraction.h"
#define SMALLEST_FRACTION 0.001

Fraction::Fraction()
{
   defaultURL = 0;
   tid = 0;    
}


Fraction::~Fraction()
{
    //std::cout << "\nFraction destructor";
}


void
Fraction::Init(double floor, const rapidjson::Value& frac)
{
    /*
     *  Percentage of being selected at random
     */
    if(frac.HasMember("pct") == false)
        throw new NeonException("Fraction::Init: no percentage key/value found");
    
    if(frac["pct"].IsDouble() == false)
        throw new NeonException("Fraction::Init: percentage value isnt a double type");
    
    pct = frac["pct"].GetDouble();
    
    // Check if pct is < the smallest decimal acceptable
    if (pct < SMALLEST_FRACTION)
        pct = 0.0;

    threshold = floor + pct;

    // Default URL
    if (frac.HasMember("default_url") == false) 
        throw new NeonException("Fraction::Init: no default_url key found");
    
    defaultURL = strdup(frac["default_url"].GetString()); 

    // Thumbnail ID
    if (frac.HasMember("tid") == false) 
        throw new NeonException("Fraction::Init: no tid key found");
    
    tid = strdup(frac["tid"].GetString());

    /*
     *  Images array
     */
    const rapidjson::Value& imageArray = frac["imgs"];
    
    if(imageArray.IsArray() == false)
        throw new NeonException("Fraction::Init: imgs element not in array json format");
    
    rapidjson::SizeType numOfImages = imageArray.Size();
    
    if(numOfImages == 0)
        throw new NeonException("Fraction::Init: no images found");
    
    images.reserve(numOfImages);
    
    // parse all images
    for(rapidjson::SizeType i=0; i < numOfImages; i++) {
        
        ScaledImage * img = new ScaledImage();
        
        // store immediately so that Shutdown can delete it in case of error
        images.push_back(img);
        
        // get image from the json array
        const rapidjson::Value& elem = imageArray[i];
        
        // init the image instance with json elem
        img->Init(elem);
    }
}


void
Fraction::Shutdown()
{
    for(std::vector<ScaledImage*>::iterator it = images.begin(); it != images.end(); it ++)
    {
        (*it)->Shutdown();
        delete (*it);
    }   

   free((void *)defaultURL);
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
        
        if(ApproxEqual(images[i]->GetHeight(), height, 2) &&
           ApproxEqual(images[i]->GetWidth(), width, 2))
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

