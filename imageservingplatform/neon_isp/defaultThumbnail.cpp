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

    default_url_ = document["default_url"].GetString();


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

    for(rapidjson::SizeType i=0; i < numOfImages; i++) {

        ScaledImage * img = new ScaledImage();
        // store in vector first, if any error it is deletable from Shutdown()
        images_.push_back(img);

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
    default_url_ = "";
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

const std::string &
DefaultThumbnail::default_url() const
{
    return default_url_;
}

const ScaledImage*
DefaultThumbnail::GetScaledImage(int height, int width) const
{
    int image_index = ScaledImage::FindBestSizeMatchImage(width, height, images_);
    if (image_index > -1)
        return &images_[image_index];

    // otherwise return NULL, and leave it up to the caller to do what they want with it
    return NULL; 
}

bool
DefaultThumbnail::operator==(const DefaultThumbnail &other) const {

    return  accountId == other.GetAccountIdRef();
};


