/*
 * Directive class 
 *
 * This is class that reprents each of the directives from the
 * mastermind file. All directives are stored in the directiveHashtable
 * */

#ifndef _NEON_DEFAULT_THUMBNAIL__
#define _NEON_DEFAULT_THUMBNAIL__

#include <vector>
#include "scaledImage.h"
#include "neon_constants.h"
#include "rapidjson/document.h"


/*
 *  Default Thumbnail type which decribes what is the default image to serve for an ccount 
 *  in case no directive are defined for it.
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
    void Dealloc();
    
    std::string accountId;
    
    std::string default_url;

    std::vector<ScaledImage*> images;
};


#endif

