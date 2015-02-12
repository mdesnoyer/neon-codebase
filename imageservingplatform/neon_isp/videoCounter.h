#ifndef _NEON_VIDEO_COUNTER__
#define _NEON_VIDEO_COUNTER__


#include <vector>
#include "fraction.h"
#include "neon_constants.h"
#include "rapidjson/document.h"
#include "string"


class VideoCounter  {


public:

    VideoCounter();
    VideoCounter(const VideoCounter &  v);
    ~VideoCounter();

    int Init(const char * vid);

    void Shutdown();

    const std::string &  GetId() const;

    void SetId(const char * vid);

    unsigned long long GetCounter() const;

    bool operator == (const VideoCounter & other) const;

    void Increment();

    static VideoCounter * Create();

    static void Destroy(VideoCounter * a);


protected:

    std::string  videoId;
    unsigned long long counter;
};


#endif
