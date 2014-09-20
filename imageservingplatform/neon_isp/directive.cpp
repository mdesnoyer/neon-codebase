#include <stdlib.h>
#include <iostream>
#include <limits.h>
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
    // left empty
}


Directive::~Directive()
{
    //std::cout << "\nDirective destructor";
}


void
Directive::Init(const rapidjson::Document & document)
{
    /*
     *  account id
     */
    if(document.HasMember("aid") == false)
        throw new NeonException("Directive::Init: no account id key found");

    if(document["aid"].IsString() == false)
        throw new NeonException("Directive::Init: account id value isnt a string");

    accountId = document["aid"].GetString();

    /*
     *  video id
     */
    if(document.HasMember("vid") == false)
        throw new NeonException("Directive::Init: no video id key found");

    if(document["vid"].IsString() == false)
        throw new NeonException("Directive::Init: video id value isnt a string");

    videoId = document["vid"].GetString();

    /*
     *  SLA time
     */
    if(document.HasMember("sla") == false)
        throw new NeonException("Directive::Init: no SLA key found");

    if(document["sla"].IsString() == false)
        throw new NeonException("Directive::Init: SLA value isnt a string");

    const char * t = document["sla"].GetString();
    sla = Mastermind::ConvertUTC(t);

    /*
     *  Fractions
     */
    const rapidjson::Value& fract = document["fractions"];

    if(fract.IsArray() == false)
        throw new NeonException("Directive::Init: fractions not in array json format");

    rapidjson::SizeType numOfFractions = fract.Size();

    if(numOfFractions == 0)
        throw new NeonException("Directive::Init: no fractions in directive");

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
        f->Init(pctFloor, fa);

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
}


void
Directive::Shutdown()
{
    for(std::vector<Fraction*>::iterator it = fractions.begin(); 
            it != fractions.end(); it ++)
    {
        (*it)->Shutdown();
        delete (*it);
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
