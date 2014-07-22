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


const std::string Mastermind::typeKey            = "type";
const std::string Mastermind::typeDirective      = "dir";
const std::string Mastermind::typePublisher      = "pub";

char Mastermind::lineBuffer[MaxLineBufferSize];


Mastermind::Mastermind()
{
    
    publisherTable.Init(100);
    directiveTable.Init(100);
    expiry = 0;
}


Mastermind::~Mastermind()
{
    std::cout << "\nMastermind destruct" << endl;
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

    memset(&when, sizeof(struct tm), 1);
    
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


void
Mastermind::InitStatic()
{
    srand(time(0));
}


void
Mastermind::Init(const char * mastermindFile, time_t previousMastermindExpiry)
{
    int lineNumber = 1;

    if(mastermindFile == 0)
        throw new NeonException("Mastermind::Init: mastermind file name is null");
    
    errno = 0;
    FILE * f = fopen(mastermindFile, "r");
    
    if(f == 0) {
        throw new NeonException("Mastermind::Init: cannot open mastermind file name "
        "\"%s\" for reading, errno = %d", mastermindFile, errno);
    }
    
    /*
     *  check trailer -- do we need to add one?
     */
    
    
    
    /*
     *  check expiry
     */
    
    char * line = fgets(lineBuffer, MaxLineBufferSize, f);
    
    if(line == 0)
        throw new NeonException("Mastermind::Init: expiry line missing in mastermind file");
    
    lineNumber++;
    
    // parse expiry here
    expiry = GetUTC(line);
    
    if(expiry == 0)
        throw new NeonException("Mastermind::Init: expiry parse error in mastermind file: line %s", line);
    
    if(expiry <= previousMastermindExpiry)
        throw new NeonException("Mastermind::Init: expiry in new mastermind file is lower or the same as the "
                                "current mastermind in memory: new file expiry %d, current mastermind expiry "
                                " %d", expiry, previousMastermindExpiry);
    /*
     *  get publishers and directives
     */
    
    while(1)
    {
        // put a terminating char at end of read buffer.  If overwritten while reading a line
        // from file this will indicate that the buffer is too small
        lineBuffer[MaxLineBufferSize-1] = 0;
        
        // get a line frorm file
        line = fgets(lineBuffer, MaxLineBufferSize, f);
        lineNumber++;
        
        // error, end of file but no end marker detected before
        if(line == 0)
            throw new NeonException("Mastermind::Init: new mastermind file is missing its end marker, "
                                    "the file may be incomplete ");
        
        // check if this is the last line of the file, the marker "end"
        if(line[0] == 'e')
            break;
        
        // init json parser
        rapidjson::Document document;
        document.Parse<0>(line);
        
        // if parsing error
        if(document.IsObject() == false) {
         
            // terminating null value in buffer was overwritten by file read, the
            // read buffer may be too small
            if(lineBuffer[MaxLineBufferSize-1] != 0)
                throw new NeonException("Mastermind::Init: line number %d in "
                    "mastermind file is bigger than buffer, json parse fail", lineNumber);
            
            throw new NeonException("Mastermind::Init: line number %d in "
            "mastermind file isn't a well-formed json document", lineNumber);
        }
        
        // if missing type key
        if(document.HasMember("type") == false)
            throw new NeonException("Mastermind::Init: line number %d in "
            "mastermind file isn't a well-formed json containing a \"type\" "
            "key and value, either a publisher or a directive", lineNumber);
        
        // must be either a publisher "pub" or directive "dir"
        std::string type = document["type"].GetString();
        
        // a publisher entry
        if(type == typePublisher) {
            publisherTable.AddPublisher(document);
        }
        
        // a directive entry
        else if(type == typeDirective) {
            directiveTable.AddDirective(document);
        }
        
        // unrecognized type
        else {
            throw new NeonException("Mastermind::Init: line number %d in "
            "mastermind file isn't a well-formed json containing a recognized "
            "\"type\" value, either \"pub\" or \"dir\"", lineNumber);
        }
    }
    
    // check the end of file marker "end" is correct
    if(line[0]      != 'e'      ||
       line[1]      != 'n'      ||
       line[2]      != 'd')
        throw new NeonException("Mastermind::Init: file mastermind has incorrect "
                                "end marker, line in file is: %s", line);
    
    // make sure this is the last line by trying to read another
    line = fgets(lineBuffer, MaxLineBufferSize, f);
    
    // more stuff was read after the end marker, which is invalid
    if(line != 0)
        throw new NeonException("Mastermind::Init: file mastermind has extraneous data following "
                                "end marker \"end\", which should is invalid");
    
    // check that we got non-zero number of publishers and directives
    if(publisherTable.GetSize() == 0 && directiveTable.GetSize() == 0)
        throw new NeonException("Mastermind::Init: zero publishers and "
                                "directives read from mastermind file");
        
    int ret = fclose(f);
    
    if(ret != 0)
        throw new NeonException("Mastermind::Init: cannot close mastermind file");
}


/*
 * Get Neon Account ID mapping for a given publisherId
 *
 * @return : * to const char 
 * *
 */

const char *
Mastermind::GetAccountId(const char * publisherId, int & size)
{
    Publisher * pub = 0;
    pub = publisherTable.Find(publisherId);
    
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

const char *
Mastermind::GetImageUrl(const char * account_id, const char * video_id, 
        unsigned char * ipAddress, int ip_len, int height, 
        int width, int & size)
{
    string accountId = account_id;
    string videoId = video_id;

    const Directive * directive = 0;
    directive = directiveTable.Find(accountId, videoId);
    
    if(directive == 0)
        return 0;
   
    // Hash string to be a combination of ipAddress & video id
    // TODO: Use bucket ID to change
    unsigned char hashstring[256]; // max size = 15 + sizeof(vid)
    int offset = 0;
    memcpy(hashstring + offset, ipAddress, ip_len);
    offset += ip_len;
    if(offset + strlen(video_id) < 256){
        memcpy(hashstring + offset, video_id, strlen(video_id));
        offset += strlen(video_id);
    }

    const Fraction * fraction = directive->GetFraction(hashstring, offset);
    if (fraction == 0){
        //neon_log_error("Fraction for the directive is NULL");
        return 0;
    }
   
    // If height or width doesn't match serve the default image URL
    if (height == -1 || width == -1)
        return fraction->GetDefaultURL();

    const ScaledImage * image = fraction->GetScaledImage(height, width);
    
    if (image == 0)
        return 0;

    return image->GetUrl(size);
}

/*
 * Get Thumbnail ID for a given video id
 * */
const char *
Mastermind::GetThumbnailID(const char * account_id, const char * video_id,
        unsigned char * ipAddress, int ip_len, int &size)
{
    string accountId = account_id;
    string videoId = video_id;

    const Directive * directive = 0;
    directive = directiveTable.Find(accountId, videoId);
    
    if(directive == 0)
        return 0;
    
    // Hash string to be a combination of ipAddress & video id
    unsigned char hashstring[256]; // max size = 15 + sizeof(vid)
    int offset = 0;
    memcpy(hashstring + offset, ipAddress, ip_len);
    offset += ip_len;
    if(offset + strlen(video_id) < 256){
        memcpy(hashstring + offset, video_id, strlen(video_id));
        offset += strlen(video_id);
    }

    const Fraction * fraction = directive->GetFraction(hashstring, offset);
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
Mastermind::Shutdown()
{
    publisherTable.Shutdown();
    directiveTable.Shutdown();
}
