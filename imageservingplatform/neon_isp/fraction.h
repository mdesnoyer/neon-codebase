#ifndef _NEON_FRACTION__
#define _NEON_FRACTION__

#include <string>
#include <vector>
#include "rapidjson/document.h"
#include "scaledImage.h"

class Fraction  {
    
    
public:
    
    Fraction();
    ~Fraction();
    
    int Init(double floor, const rapidjson::Value& fa);
    void Shutdown();
    
    void   SetPct(double);
    double GetPct() const;
    double GetThreshold() const;
    const char * GetDefaultURL() const;
    const char * GetThumbnailID() const;
    std::string GetBaseUrl() const; 

    ScaledImage* GetScaledImage(int height, int width) const;
    
protected:
    
    int InitSafe(double floor, const rapidjson::Value& fa);
    int ProcessImages(const rapidjson::Value &, bool);
    void Dealloc();
    
    bool initialized;
    double threshold;
    double pct;
    const char * defaultURL;
    const char * tid;
    std::string  baseUrl; 
    
    std::vector<ScaledImage*> images;

private: 
    std::string GenerateDefaultUrl(const rapidjson::Value&); 
};



#endif


