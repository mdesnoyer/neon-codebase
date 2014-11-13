#include <stdlib.h>
#include <iostream>
#include <limits.h>
#include "neon_stats.h"
#include "directive.h"
#include "neonException.h"
#include "mastermind.h"
extern "C" {
    #include "neon_utils.h"
}
const double EXPECTED_PCT = 1.0; 

/*
 *   Directive
 */

Directive::Directive()
{
    sla = 0;
}


Directive::~Directive()
{
    sla = 0; 
}


int
Directive::Init(const rapidjson::Document & document) {

    try {
        int ret = InitSafe(document);
        
        // success
        if(ret == 0)
            return 0;
    }

    // on any error, clean up 
    catch (NeonException * e) {
    
    }
    catch (std::bad_alloc e) {
    
    }
    catch (...) {
    }

    Dealloc();
    return -1;
}


int
Directive::InitSafe(const rapidjson::Document & document)
{
    /*
     *  account id
     */
    if(document.HasMember("aid") == false) {
        neon_stats[NEON_DIRECTIVE_PARSE_ERROR]++;
        return -1;
    }

    if(document["aid"].IsString() == false) {
        neon_stats[NEON_DIRECTIVE_PARSE_ERROR]++;
        return -1;
    }

    accountId = document["aid"].GetString();

    /*
     *  video id
     */
    if(document.HasMember("vid") == false)  {
        neon_stats[NEON_DIRECTIVE_PARSE_ERROR]++;
        return -1;
    }

    if(document["vid"].IsString() == false) {
        neon_stats[NEON_DIRECTIVE_PARSE_ERROR]++;
        return -1;
    }

    videoId = document["vid"].GetString();

    // Convert to External(platform) video id, if _ not present return
    // std::string::npos, which wraps around to 0, when +1 'ed :)
    videoId = videoId.substr(videoId.find("_") + 1, videoId.length());

    /*
     *  SLA time
     */
    if(document.HasMember("sla") == false) {
        neon_stats[NEON_DIRECTIVE_PARSE_ERROR]++;
        return -1;
    }

    if(document["sla"].IsString() == false) {
        neon_stats[NEON_DIRECTIVE_PARSE_ERROR]++;
        return -1;
    }

    const char * t = document["sla"].GetString();
    sla = Mastermind::ConvertUTC(t);

    /*
     *  Fractions
     */
    const rapidjson::Value& fract = document["fractions"];

    if(fract.IsArray() == false) {
        neon_stats[NEON_DIRECTIVE_PARSE_ERROR]++;
        return -1;
    }

    rapidjson::SizeType numOfFractions = fract.Size();

    if(numOfFractions == 0) {
        neon_stats[NEON_DIRECTIVE_INVALID]++;
        return -1;
    }

    double pctFloor = 0.0;

    fractions.reserve(numOfFractions);

    for(rapidjson::SizeType i=0; i < numOfFractions; i++) {

        Fraction * f = new Fraction();

        // store in vector first, if any error it is deletable from Shutdown()
        // order is important here
        fractions.push_back(f);

        // obtain the specific json fraction
        const rapidjson::Value& fa = fract[i];

        // init the fraction, passing along the floor value
        int ret = f->Init(pctFloor, fa);

        if(ret != 0) {
            neon_stats[NEON_DIRECTIVE_INVALID]++;
            return -1;
        }

        // this is the floor value of the next one
        pctFloor = f->GetThreshold();
    }

    // Check total fraction adds up to 1. Else normalize pcts 
    //
    if (pctFloor != EXPECTED_PCT){
        for(unsigned int i=0; i<fractions.size(); i++){
            double pcnt = fractions[i]->GetPct();
            pcnt = pcnt / pctFloor;
            fractions[i]->SetPct(pcnt);
        } 
    }
    
    return 0;
}


void
Directive::Shutdown()
{
    Dealloc();
}


void 
Directive::Dealloc()
{
    for(std::vector<Fraction*>::iterator it = fractions.begin(); 
            it != fractions.end(); it ++)
    {
        Fraction * f = (*it);

        if(f == NULL) {
            neon_stats[NEON_FRACTION_SHUTDOWN_NULL_POINTER]++;
            continue;
        }

        f->Shutdown();
        delete f;
        f = 0;
    }
}


const char *
Directive::GetAccountId() const
{
    return accountId.c_str();
}


const std::string &
Directive::GetAccountIdRef() const
{
    return accountId;
}


const char *
Directive::GetVideoId() const
{
    return videoId.c_str();
}


const std::string &
Directive::GetVideoIdRef() const
{
    return videoId;
}

/*
 * For a given bucketId, select the fraction  
 *
 * */

const Fraction *
Directive::GetFraction(unsigned char * bucketId, int bucketIdLen) const
{
    //GDB: print *(fractions._M_impl._M_start)@fractions.size()
    if(fractions.size() == 0)
        return 0;

    unsigned int index = 0;    
    std::vector<double> cumulative_pcts; 
    std::vector<double> individual_pcts; 
    double total_pcnt = 0;
    for(unsigned int i=0; i<fractions.size(); i++){
        double pcnt = fractions[i]->GetPct();
        total_pcnt += pcnt;    
        individual_pcts.push_back(pcnt);
        cumulative_pcts.push_back(total_pcnt);
    }
    char * endptr = NULL;
    // BucketId is HEX 
    double bId = (double) strtol((const char *)bucketId, &endptr, 16); 

    // check if bId is actually 0 or just a junk string
    if (bId == 0){
       if (bucketId[0] != '0')
           bId = -1;
    }

    if(bId < 0 or bucketIdLen <= 0){
        // If bucketId is empty, the user isnt' part of AB test yet 
        // Pick the fraction with max pcnt
        index = std::distance(individual_pcts.begin(), 
                                std::max_element(individual_pcts.begin(), 
                                individual_pcts.end()));

    }else{
        // Pick the AB test bucket
        unsigned int i;
        for(i=0 ; i< cumulative_pcts.size(); i++){
            if (bId < (cumulative_pcts[i] * N_ABTEST_BUCKETS))
                break;    
        }
        index = i;
    }

    return fractions[index];
}

bool
Directive::operator==(const Directive &other) const {

    return GetKey() == other.GetKey();
};

std::string
Directive::GetKey() const{
    
    std::string composite;
    composite.append(accountId);
    composite.append(videoId);
    return composite;
}
