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
    
    
    void Init(double floor, const rapidjson::Value& fa);
    void Shutdown();
    
    double GetPct() const;
    double GetThreshold() const;
    const char * GetDefaultURL() const;
    const char * GetThumbnailID() const;

    ScaledImage* GetScaledImage(int height, int width) const;
    
protected:
    
    double threshold;
    double pct;
    const char * defaultURL;
    const char * tid;
    
    std::vector<ScaledImage*> images;
};



#endif


