#ifndef _NEON_FRACTION__
#define _NEON_FRACTION__

#include <boost/scoped_ptr.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <string>
#include <vector>
#include "rapidjson/document.h"
#include "scaledImage.h"
#include "include/url_utils.hpp"
#include "limits.h" 

class Fraction  {
    
    
public:
    
    Fraction();
    ~Fraction();
    
    int Init(double floor, const rapidjson::Value& fa);
    void   SetPct(double);
    double GetPct() const;
    double GetThreshold() const;
    std::string * default_url() const;
    std::string * tid() const;
    const std::string & base_url() const; 
    const ScaledImage* GetScaledImage(int height, int width) const;
    
protected:
    
    int InitSafe(double floor, const rapidjson::Value& fa);
    int ProcessImages(const rapidjson::Value &);
    double threshold;
    double pct;

private: 
    std::string base_url_; 
    boost::scoped_ptr<std::string> default_url_; 
    boost::scoped_ptr<std::string> tid_; 
    boost::ptr_vector<ScaledImage> images_; 
};



#endif


