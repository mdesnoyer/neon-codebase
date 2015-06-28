#ifndef _NEON_DEFAULT_THUMBNAIL__
#define _NEON_DEFAULT_THUMBNAIL__

#include <vector>
#include "scaledImage.h"
#include "neon_constants.h"
#include "rapidjson/document.h"


/*
 *  This class represents an account-wide default thumbnail for the 
 *  cases where a video directive might not exists.
 */
class DefaultThumbnail  {
    
    
public:
    
    DefaultThumbnail();
    DefaultThumbnail(const DefaultThumbnail &  p);
    ~DefaultThumbnail();
    
    int Init(const rapidjson::Document & document);
    
    void Shutdown();
    
    const char * GetAccountId() const;
    
    const std::string & GetAccountIdRef() const;

    const char * GetScaledImage(int height, int width, int & url_size) const;

    bool operator == (const DefaultThumbnail &other) const;

protected:
    
    int InitSafe(const rapidjson::Document & document);
    int ProcessImages(const rapidjson::Value &);
    void Dealloc();
    
    std::string accountId;
    
    std::string default_url;

    std::vector<ScaledImage*> images;
};


#endif

