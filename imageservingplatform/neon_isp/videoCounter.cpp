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
VideoCounter::Init(const rapidjson::Document & document) 
{

    return 0;
}


void
VideoCounter::Shutdown()
{
}


std::string &
VideoCounter::GetVideoId() const
{
    return videoId();
}


bool
VideoCounter::operator==(const VideoCounter &other) const {

    return videoId  == other.GetVideoId();
};

