#ifndef _NEON_FRACTION__
#define _NEON_FRACTION__

#include <boost/scoped_ptr.hpp>
#include <string>
#include <vector>
#include "rapidjson/document.h"
#include "scaledImage.h"
#include "include/utility.hpp" 

class Fraction  {
    
    
public:
    
    Fraction();
    ~Fraction();
    
    int Init(double floor, const rapidjson::Value& fa);
    void Shutdown();
    
    void   SetPct(double);
    double GetPct() const;
    double GetThreshold() const;
    std::string * default_url() const;
    const char * GetThumbnailID() const;
    const std::string & base_url() const; 
    ScaledImage* GetScaledImage(int height, int width) const;
    
protected:
    
    int InitSafe(double floor, const rapidjson::Value& fa);
    int ProcessImages(const rapidjson::Value &);
    void Dealloc();
    
    bool initialized;
    double threshold;
    double pct;
    const char * defaultURL;
    const char * tid;
    std::vector<ScaledImage*> images;

private: 
    std::string base_url_; 
    boost::scoped_ptr<std::string> default_url_; 
};



#endif


