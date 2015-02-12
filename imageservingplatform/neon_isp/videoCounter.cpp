#include <stdlib.h>
#include <iostream>
#include <limits.h>
#include "neon_stats.h"
#include "videoCounter.h"
#include "neonException.h"
#include "mastermind.h"



VideoCounter::VideoCounter()
{
}


VideoCounter::~VideoCounter()
{
}


int
VideoCounter::Init(const char * vid) 
{
    videoId = vid;
    return 0;
}


void
VideoCounter::Shutdown()
{
}


const std::string &  
VideoCounter::GetId() const 
{
    return videoId;
}


void 
VideoCounter::SetId(const char * vid)
{
    videoId = vid;
}


unsigned long long 
VideoCounter::GetCounter() const
{
    return counter;
}


void 
VideoCounter::Increment()
{
    counter++;
}



bool
VideoCounter::operator==(const VideoCounter &other) const {

    return videoId  == other.GetId();
};


VideoCounter * 
VideoCounter::Create()
{
    VideoCounter * v = new VideoCounter();
    return v;
}


void 
VideoCounter::Destroy(VideoCounter * v)
{
    delete v;
}



