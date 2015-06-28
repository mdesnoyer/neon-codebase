#include <stdlib.h>
#include <iostream>
#include <limits.h>
#include "neon_stats.h"
#include "defaultThumbnail.h"
#include "neonException.h"
#include "mastermind.h"

DefaultThumbnail::DefaultThumbnail()
{
}


DefaultThumbnail::~DefaultThumbnail()
{
}


int
DefaultThumbnail::Init(const rapidjson::Document & document) {

    try {
        int ret = InitSafe(document);
        
        // success
        if(ret == 0)
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
DefaultThumbnail::InitSafe(const rapidjson::Document & document)
{
    /*
     *  account id
     */
    if(document.HasMember("aid") == false) {
        neon_stats[NEON_DEFAULT_THUMBNAIL_PARSE_ERROR]++;
        return -1;
    }

    if(document["aid"].IsString() == false) {
        neon_stats[NEON_DEFAULT_THUMBNAIL_PARSE_ERROR]++;
        return -1;
    }

    accountId = document["aid"].GetString();


    /*
     *  default url
     */
    if(document.HasMember("default_url") == false) {
        neon_stats[NEON_DEFAULT_THUMBNAIL_PARSE_ERROR]++;
        return -1;
    }

    if(document["default_url"].IsString() == false) {
        neon_stats[NEON_DEFAULT_THUMBNAIL_PARSE_ERROR]++;
        return -1;
    }

    default_url = document["default_url"].GetString();


    /*
     *  Scaled Images
     */
    // the array must exist
    if(document.HasMember("imgs")) {
        const rapidjson::Value& imgs = document["imgs"];
        return ProcessImages(imgs); 
    }
    else if(document.HasMember("img_sizes"))  {
        const rapidjson::Value& img_sizes = document["img_sizes"];  
        return ProcessImages(img_sizes); 
    }
    else { 
        neon_stats[NEON_DEFAULT_THUMBNAIL_PARSE_ERROR]++;
        return -1;
    }     

    return 0;
}


/**************************************************************
Function   : ProcessImages
Purpose    : handles both the imgs and img_sizes Values from 
             mastermind json file 
Parameters : imgs - an array of images from the mastermind file 
             on a directive. should have h/w/and possible 
             url  
             needsUrlGenerated - if we have an img that does not 
             have a url, send this in as true and one will be 
             generated 
RV         : 0 if successful -1 if not 
***************************************************************/ 

int 
DefaultThumbnail::ProcessImages(const rapidjson::Value & imgs) 
{ 
    if(imgs.IsArray() == false) {
        neon_stats[NEON_DEFAULT_THUMBNAIL_PARSE_ERROR]++;
        return -1;
    }

    rapidjson::SizeType numOfImages = imgs.Size();

    // the array may legally be empty, so we are done with init here
    if(numOfImages == 0) {
        return 0;
    }

    images.reserve(numOfImages);

    for(rapidjson::SizeType i=0; i < numOfImages; i++) {

        ScaledImage * img = new ScaledImage();
        // store in vector first, if any error it is deletable from Shutdown()
        images.push_back(img);

        // obtain the specific json fraction
        const rapidjson::Value& imageDocument  = imgs[i];

        // init the fraction, passing along the floor value
        int ret = img->Init(imageDocument);

        if(ret != 0) {
            neon_stats[NEON_DEFAULT_THUMBNAIL_INVALID]++;
            return -1;
        }
    }
    return 0; 
}



void
DefaultThumbnail::Shutdown()
{
    accountId = "";
    default_url = "";
    Dealloc();
}


void 
DefaultThumbnail::Dealloc()
{
    for(std::vector<ScaledImage*>::iterator it = images.begin(); 
            it != images.end(); it ++)
    {
        ScaledImage * img = (*it);

        if(img == NULL) {
            neon_stats[NEON_DEFAULT_THUMBNAIL_SHUTDOWN_NULL_POINTER]++;
            continue;
        }

        img->Shutdown();
        delete img;
        img = 0;
    }
}


const char *
DefaultThumbnail::GetAccountId() const
{
    return accountId.c_str();
}


const std::string &
DefaultThumbnail::GetAccountIdRef() const
{
    return accountId;
}


// TODO Kevin this needs to be combined with Fraction::GetScaledImage
// TODO get rid of images->GetUrl, replace it with the call to scoped_url  
const char *
DefaultThumbnail::GetScaledImage(int height, int width, int & url_size) const{
    
    static const int pixelRange = 6;

    // iterate through our scaled images to find a size match
    unsigned numOfImgs = images.size();

    // try to find a perfect size match
    for(unsigned i=0; i < numOfImgs; i++){

        if( images[i]->GetHeight() ==  height &&
            images[i]->GetWidth() == width ) {

            // a match, url_size is set here
            const char * url = images[i]->GetUrl(url_size);
            neon_stats[NEON_DEFAULT_IMAGE_PERFECT_FIT]++;
            return url;
        }
    }

    // try to find an approximate size
    for(unsigned i=0; i < numOfImgs; i++){
        
        if( ScaledImage::ApproxEqual(images[i]->GetHeight(), height, pixelRange) &&
            ScaledImage::ApproxEqual(images[i]->GetWidth(), width, pixelRange)) {

            // a match, url_size is set here
            const char * url = images[i]->GetUrl(url_size);
            neon_stats[NEON_DEFAULT_IMAGE_APPROX_FIT]++;
            return url;
        }
    }
    
    // otherwise return default url
    url_size = default_url.size();
    return default_url.c_str();
}


bool
DefaultThumbnail::operator==(const DefaultThumbnail &other) const {

    return  accountId == other.GetAccountIdRef();
};


