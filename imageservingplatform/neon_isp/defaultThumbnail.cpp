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
    if(document.HasMember("imgs") == false) {
        neon_stats[NEON_DEFAULT_THUMBNAIL_PARSE_ERROR]++;
        return -1;
    }
    
    const rapidjson::Value& imgs = document["imgs"];

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
            neon_stats[NEON_SCALED_IMAGE_SHUTDOWN_NULL_POINTER]++;
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


const char *
DefaultThumbnail::GetScaledImage(int height, int width, int & url_size) const{
    
    static const int pixelRange = 6;

    // iterate through our scaled images to find a size match
    unsigned numOfImgs = images.size();
    
    for(unsigned i=0; i < numOfImgs; i++){
        
        if( ScaledImage::ApproxEqual(images[i]->GetHeight(), height, pixelRange) &&
            ScaledImage::ApproxEqual(images[i]->GetWidth(), width, pixelRange)) {

            // a match, url_size is set here
            const char * url = images[i]->GetUrl(url_size);
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


