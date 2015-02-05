#ifndef _NEON_VIDEO_COUNTER__
#define _NEON_VIDEO_COUNTER__


#include <vector>
#include "fraction.h"
#include "neon_constants.h"
#include "rapidjson/document.h"



class VideoCounter  {


public:

    VideoCounter();
    VideoCounter(const VideoCounter &  v);
    ~VideoCounter();

    int Init(std::string & id);

    void Shutdown();

    std::string &  GetVideoId() const;

    bool operator == (const VideoCounter & other) const;

protected:

    std::string  videoId;
    unsigned long long counter;
};


#endif
