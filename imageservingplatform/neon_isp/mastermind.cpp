/*
 *  Mastermind class : Mastermind Data structure
 *
 *  */

#include <errno.h>
#include <iostream>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include "neonException.h"
#include "mastermind.h"
#include "neon_mastermind.h"
#include "neon_stats.h"
#include "include/utility.hpp"

const std::string Mastermind::typeKey            = "type";
const std::string Mastermind::typeDirective      = "dir";
const std::string Mastermind::typePublisher      = "pub";
const std::string Mastermind::typeDefaultThumbnail      = "default_thumb";

char Mastermind::lineBuffer[MaxLineBufferSize];

// Formatter for Cloudinary Image URLs (Cloudinary insert .jpg after upload)
const char * cloudinary_image_format = "http://res.cloudinary.com/neon-labs/image/upload/w_%d,h_%d/neontn%s_w0_h0.jpg.jpg";


Mastermind::Mastermind()
{
    publisherTable = 0;
    directiveTable = 0;
    defaultThumbnailTable = 0;
    expiry = 0;
    parseFile = 0;
    initialized = false;
}


Mastermind::~Mastermind()
{
    publisherTable = 0;
    directiveTable = 0;
    defaultThumbnailTable = 0;
    parseFile = 0;
    initialized = false;
}



time_t
Mastermind::GetUTC(const char * line)
{
    // line must begin with
    if(strncmp(line, "expiry=", 7) != 0)
        throw "Mastermind::GetUTC: file expiry key is invalid";
    
    // jump to date string
    line += 7;
    
    time_t gmt = 0;
    gmt = ConvertUTC(line);
    
    return gmt;
}


time_t
Mastermind::ConvertUTC(const char * line)
{
    // ISO-8601  "2014-03-27T23:20:00Z"
    char buffer[UTCStrSize + 1];
    
    // null ptr
    if(line == 0)
        return 0;
    
    // check size
    if(strlen(line) < UTCStrSize)
        return 0;
    
    memset(buffer, 0, UTCStrSize + 1);
    memcpy(buffer, line, UTCStrSize);
    
    struct tm when;

    memset(&when, 0, sizeof(struct tm));
    
    // this function takes care of proper conversion to a struct tm, with proper
    // year and month numbers
    char * ret = strptime(buffer, "%Y-%m-%dT%H:%M:%SZ", &when);

    if(ret == 0)
        return 0;
    
    time_t result = 0;
    result = mktime(&when);

    return result;
}


time_t
Mastermind::GetFileExpiry(const char * const filepath)
{
    errno = 0;
    FILE * file = fopen(filepath, "r");
    
    time_t gmt = GetFileExpiry(file);
    
    errno = 0;
    fclose(file);
    
    return gmt;
}


time_t
Mastermind::GetFileExpiry(FILE * file)
{
    char buffer[32];
    char * line = 0;
    long position = 0;
    
    // note where is the file position currently
    position = ftell(file);
    
    // position first byte to read expiry
    errno = 0;
    fseek(file, 0L, SEEK_SET);
    
    // get the expiry line
    line = fgets(buffer, 32, file);
    
    // set file position to where it was
    errno = 0;
    fseek(file, position, SEEK_SET);
    
    // must begin
    if(strncmp(line, "expiry=", 7) != 0)
        return 0;
    
    // jump to date string
    line += 7;
    
    time_t gmt = 0;
    
    gmt = GetUTC(line);
    
    return gmt;
}


double
Mastermind::randZeroToOne()
{
    return rand() / (RAND_MAX + 1.);
}


time_t
Mastermind::GetExpiry()
{
    return expiry;
}


Mastermind::EINIT_ERRORS 
Mastermind::Init() {
   
   if(initialized == true) {
        neon_stats[NEON_MASTERMIND_INVALID_INIT]++;
        return EINIT_FATAL_ERROR;  
   }
   
    try {
        publisherTable = new PublisherHashtable();
        directiveTable = new DirectiveHashtable();
        defaultThumbnailTable = new DefaultThumbnailHashtable();
        publisherTable->Init(100);
        directiveTable->Init(100);
        defaultThumbnailTable->Init(100);
    }
    catch(...) {
        neon_stats[NEON_MASTERMIND_INVALID_INIT]++;
        return EINIT_FATAL_ERROR;
    }

    initialized = true;
    return EINIT_SUCCESS;
}


Mastermind::EINIT_ERRORS
Mastermind::Init(const char * mastermindFile, 
                 time_t previousMastermindExpiry,
                 char * error_message,
                 unsigned error_message_size)
{
    if(initialized == true) {
        return EINIT_FATAL_ERROR;
    }
   
    try {
        EINIT_ERRORS ret = InitSafe(mastermindFile, 
                                    previousMastermindExpiry,
                                    error_message,
                                    error_message_size);
        
        if(ret == EINIT_SUCCESS || EINIT_PARTIAL_SUCCESS)  
            return ret;
    }
    catch (...) {
        snprintf(error_message, error_message_size, "%s", 
                 "Mastermind::Init: failed: uncaught exception");
    }

    if(parseFile != 0) {
        fclose(parseFile);
        parseFile = 0;
    }

    Dealloc();
    return EINIT_FATAL_ERROR;
}


Mastermind::EINIT_ERRORS
Mastermind::InitSafe(const char * mastermindFile, 
                     time_t previousMastermindExpiry,
                     char * error_message,
                     unsigned error_message_size)
{

    if(initialized == true) {
        neon_stats[NEON_MASTERMIND_INVALID_INIT]++;
        return EINIT_FATAL_ERROR;
    }

    parseFile = 0;
    int lineNumber = 1;

    // terminate this error buffer
    error_message[0] = 0;

    if(mastermindFile == 0) {
        snprintf(error_message, error_message_size, "%s",
                "Mastermind::Init: mastermind file name ptr is null");
        return EINIT_FATAL_ERROR;
    }

    errno = 0;
    parseFile = 0;
    parseFile = fopen(mastermindFile, "r");
    
    if(parseFile == 0) {
        snprintf(error_message, error_message_size, 
                 "Mastermind::Init: cannot open mastermind file named %s: errno %d",
                 mastermindFile, errno);
        return EINIT_FATAL_ERROR;
    }
    
    /*
     *  check expiry
     */
    char * line = fgets(lineBuffer, MaxLineBufferSize, parseFile);
    
    if(line == 0) {
        snprintf(error_message, error_message_size, "%s",
                "Mastermind::Init: expiry line missing in mastermind file");
        neon_stats[MASTERMIND_PARSE_FAIL]++;
        return EINIT_FATAL_ERROR;
    }

    lineNumber++;
    
    // parse expiry line
    expiry = GetUTC(line);
    
    if(expiry == 0) {
        snprintf(error_message, error_message_size, 
               "Mastermind::Init: expiry parse error in mastermind file: line %s", line);
         neon_stats[MASTERMIND_PARSE_FAIL]++;
        return EINIT_FATAL_ERROR;
    }

    if(expiry <= previousMastermindExpiry) {
        snprintf(error_message, error_message_size, 
           "Mastermind::Init: expiry in new mastermind file is lower or the same as the "
           "current mastermind in memory: new file expiry %d, current mastermind expiry %d",
           (int)expiry, (int)previousMastermindExpiry);
        return EINIT_FATAL_ERROR;
    }

    // allocate all tables
    publisherTable = new PublisherHashtable();
    directiveTable = new DirectiveHashtable();
    defaultThumbnailTable = new DefaultThumbnailHashtable();

    publisherTable->Init(100);
    directiveTable->Init(100);
    defaultThumbnailTable->Init(100);

    /*
     *  Parse all entries (publishers, account default image, video directives).  
     *  Each entry is a self-contained json document and may be rejected individually
     *  if malformed.  If there are rejected entries the return code is EINIT_PARTIAL_SUCCESS
     *  Ocurrences of entry rejection should be promptly investigated. Check stats counters
     *  for details and velocity of errors.
     *
     *  For performance reasons we only do a best-effort logging of the first occurence of an 
     *  entry rejection. This is done to prevent bogging down isp in the case of a mastermind
     *  file with potentially millions of repetitive entry issues.  
     */

    // used to count rejected entries below
    unsigned entries_rejected = 0; 
    
    while(1)
    {
        // put a terminating char at end of read buffer.  If overwritten while reading a line
        // from file this will indicate that the buffer is too small
        lineBuffer[MaxLineBufferSize-1] = 0;
        
        // get a line frorm file
        line = fgets(lineBuffer, MaxLineBufferSize, parseFile);
        lineNumber++;
        
        // error, end of file reached but no end marker detected before
        if(line == 0) {
            snprintf(error_message, error_message_size, "%s",
                    "Mastermind::Init: new mastermind file is missing its end marker, "
                    "the file is incomplete ");
            neon_stats[MASTERMIND_PARSE_FAIL]++;
            return EINIT_FATAL_ERROR;
        }

        // check if this is the last line of the file, the marker "end"
        if(line[0] == 'e')
            break;

        // skip commented or empty lines 
        if(line[0] == '#' || line[0] == '\n')
            continue;

        // init json parser
        rapidjson::Document document;
        document.Parse<0>(line);
        
        // if json parsing error
        if(document.IsObject() == false) {
    
            neon_stats[MASTERMIND_ENTRY_REJECTED]++;

            // terminating null value in buffer was overwritten by file read, the
            // read buffer may be too small
            if(lineBuffer[MaxLineBufferSize-1] != 0) {
                // log error is no previous error message
                if(error_message[0] == 0) {
                    snprintf(error_message, error_message_size, "Mastermind::Init: directive at "
                    "line number %d in mastermind file is bigger than parse buffer, json parse fail", 
                    lineNumber);
                    entries_rejected++;
                    continue;
                }
            }

            // log error is no previous error message
            if(error_message[0] == 0) {
                snprintf(error_message, error_message_size, "Mastermind::Init: directive at "
                       "line number %d in mastermind file isn't a well-formed json document", 
                        lineNumber);
            }
            
            entries_rejected++;
            continue;
        }
        
        // if missing type key
        if(document.HasMember("type") == false) {
            // log error is no previous error message
            if(error_message[0] == 0)
                snprintf(error_message, error_message_size, "Mastermind::Init: directive at "
                    "line number %d in mastermind file doesn't contain a \"type\" "
                    "key and value", lineNumber);
           entries_rejected++;
           continue;
        }

        std::string type = document["type"].GetString();
        
        // a publisher record
        if(type == typePublisher) {
            publisherTable->AddPublisher(document);
        }
        // a video directive 
        else if(type == typeDirective) {
            directiveTable->AddDirective(document);
        }
        // an account-wide default thumbnail
        else if(type == typeDefaultThumbnail) {
            defaultThumbnailTable->Add(document);
        }            

        // unrecognized entry type
        else {
            // log error is no previous error message
            if(error_message[0] == 0)
                snprintf(error_message, error_message_size, "Mastermind::Init: directive at "
                    "line number %d in mastermind file is of an unrecognized "
                    "\"type\": %s ", lineNumber, type.c_str());
            entries_rejected++;
            continue;
        }
    }
    
    // check the end of file marker "end" is correct
    if(line[0]      != 'e'      ||
       line[1]      != 'n'      ||
       line[2]      != 'd') {
        snprintf(error_message, error_message_size, "Mastermind::Init: file mastermind has incorrect "
                                "end marker, line in file is: %s", line);
        neon_stats[MASTERMIND_PARSE_FAIL]++;
        return EINIT_FATAL_ERROR;
    }

    // make sure this is the last line by trying to read another
    line = fgets(lineBuffer, MaxLineBufferSize, parseFile);
    
    // more stuff was read after the end marker, which is invalid
    if(line != 0) {
        snprintf(error_message, error_message_size, "%s", "Mastermind::Init: file mastermind has extraneous data following "
                                "end marker \"end\", which is invalid");
        neon_stats[MASTERMIND_PARSE_FAIL]++;
        return EINIT_FATAL_ERROR;
    }

    // check that we got non-zero number of publishers and directives
    if(publisherTable->GetSize() == 0 && directiveTable->GetSize() == 0) {
        snprintf(error_message, error_message_size, "Mastermind::Init:  publishers and "
            "directives tables are empty after loading mastermind file.  Number of "
            "rejected entries is %d", entries_rejected);
        return EINIT_FATAL_ERROR;
    }
   
    int ret = fclose(parseFile);
   
    // unable to close file
    if(ret != 0) {
        // add a counter here
    }

    parseFile = 0;
    initialized = true;

    // some entries were rejected, so partial success
    if(entries_rejected > 0) {
        neon_stats[MASTERMIND_PARSE_PARTIAL]++;
        return EINIT_PARTIAL_SUCCESS;
    }
    // complete success
    else {
        neon_stats[MASTERMIND_PARSE_SUCCESS]++;
        return EINIT_SUCCESS;
    }
}


/*
 * Get Neon Account ID mapping for a given publisherId
 *
 * @return : * to const char 
 * *
 */

const char *
Mastermind::GetAccountId(const char * publisherId, int & size){
    
    Publisher * pub = 0;
    pub = publisherTable->Find(publisherId);
    
    if(pub == 0)
        return 0;
    
    const std::string & accountId = pub->GetAccountIdRef();
    
    size = accountId.size();
    
    return accountId.c_str();
}

/*
 * Get Image URL for a given request
 * @input : Neon Account ID, Video ID, IP Address of client, image height, width
 *
 * @return : URL of the image (const char *)
 * */

void
Mastermind::GetImageUrl(const char * account_id, 
                        const char * video_id, 
                        unsigned char * bucketId,
                        int bucketIdLen,
                        int height, 
                        int width, 
                        int & size,
                        std::string & image_url){
    
    string accountId = account_id;
    string videoId = video_id;

    const Directive * directive = 0;
    directive = directiveTable->Find(accountId, videoId);
    // if no directive are found for this vid then try to return 
    // the default thumb for this account
    if(directive == 0) {
        const DefaultThumbnail * def = defaultThumbnailTable->Find(accountId); 
        // if nothing more can be done
        if(def == 0) {
            neon_stats[NEON_INVALID_VIDEO_ID] ++;
            return;
        }
        image_url = def->GetScaledImage(height, width, size);
        return;  
    }

    const Fraction * fraction = directive->GetFraction(bucketId, bucketIdLen);
    
    if (fraction == 0) {
        return;
    }
   
    // If either or both height or width are empty, then serve the default image URL
    if (height == -1 || width == -1){
        //image_url = string(*fraction->default_url());
        image_url = *fraction->default_url();
        size = image_url.size();
    }
    else { 
        const ScaledImage * image = fraction->GetScaledImage(height, width);
        // Didn't get a "pre-sized" image so send default URL 
        if (image == 0){
            image_url = *fraction->default_url();
            size = image_url.size();
            // NOTE: IGN Doesn't want to use cloudinary, hence we'll return a
            // default URL for non-standard sizes
            // re-enable when needed

            //char buffer[1024]; // sufficiently large buffer for the URL
            //const char * tid = fraction->GetThumbnailID();
            //sprintf(buffer, cloudinary_image_format, width, height, tid);
            //const char * url = strdup(buffer);
            //size = strlen(url);
            //return url;
        }
        if (image->scoped_url() != 0) {
            image_url = *image->scoped_url(); 
            size = image_url.size(); 
        } 
        else { 
            image_url = utility::generateUrl(fraction->base_url(), (std::string)fraction->GetThumbnailID(), image->GetHeight(), image->GetWidth()); 
            size = image_url.size(); 
        }
    }  
}

/*
 * Get Thumbnail ID for a given video id
 * */
const char *
Mastermind::GetThumbnailID(const char * c_accountId, 
                            const char * c_videoId,
                            unsigned char * bucketId,
                            int bucketIdLen,
                            int &size){
    
    string accountId = c_accountId;
    string videoId = c_videoId;

    const Directive * directive = 0;
    directive = directiveTable->Find(accountId, videoId);
    
    if(directive == 0){
        neon_stats[NEON_INVALID_VIDEO_ID] ++;
        return 0;
    }
    
    const Fraction * fraction = directive->GetFraction(bucketId, bucketIdLen);

    if (fraction == 0){
        //neon_log_error("Fraction for the directive is NULL");
        return 0;
    }

    const char * tid = fraction->GetThumbnailID();
    size = strlen(tid);    
    return tid;
} 

/*
 * Mastermind shutdown function
 *
 * Clean up the publisher & directive tables
 * */

void
Mastermind::Shutdown(){
   
    if(initialized == false) {
        //neon_stats[NEON_MASTERMIND_INVALID_SHUTDOWN] ++;
        return;
    }

    Dealloc();
    initialized = false;
}

void 
Mastermind::Dealloc() {

    if(publisherTable != 0) {
        publisherTable->Shutdown();
        delete publisherTable;
        publisherTable = 0;
    }

    if(directiveTable != 0) {
        directiveTable->Shutdown();
        delete directiveTable;
        directiveTable = 0;
    }

    if(defaultThumbnailTable != 0) {
        defaultThumbnailTable->Shutdown();
        delete defaultThumbnailTable;
        defaultThumbnailTable = 0;
    }
}





