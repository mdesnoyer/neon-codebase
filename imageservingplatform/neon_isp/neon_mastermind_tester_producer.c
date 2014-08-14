#include <time.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>





char * publisherFormat = "{\"type\": \"pub\", \"pid\": \"pub%d\", \"aid\" : \"acc%d\" }\n";
char publisherBuffer[1024];

void
printPublisher(FILE * f, int num)
{
    int i=0;
    
    for(; i < num; i++)
    {
        int total = sprintf(publisherBuffer, publisherFormat, i, i);
        fwrite(publisherBuffer, 1, total, f);
    }
}



char * directiveFormat =

"{ "
"    \"type\": \"dir\",  "
"    \"aid\":\"acc%d\", "
"    \"vid\":\"vid%d\", "
"    \"sla\":\"%s\", "
"    \"fractions\":  "
"    [        "
"         {    "
"             \"pct\": 0.7,  "
"             \"tid\": \"thumb1\",  "
"             \"default_url\": \"http://default_image_url.jpg\",  "
"             \"imgs\":   "
"             [     "
"                  {  "
"                       \"h\":500, "
"                       \"w\":600,  "
"                       \"url\":\"http://neon-image-cdn.s3.amazonaws.com/pixel.jpg\""
"                  },  "
"                  {   "
"                       \"h\":700,  "
"                       \"w\":800,  "
"                       \"url\":\"http://neon/thumb2_700_800.jpg\"  "
"                  }    "
"             ]   "
"         },    "
"         {    "
"             \"pct\": 0.2, "
"             \"tid\": \"thumb2\",  "
"             \"default_url\": \"http://default_image_url.jpg\",  "
"             \"imgs\":  "
"             [   "
"                  {   "
"                       \"h\":500,  "
"                       \"w\":600,  "
"                       \"url\":\"http://neont2/thumb1_500_600.jpg\"  "
"                  },  "
"                  {  "
"                       \"h\":300,  "
"                       \"w\":400,  "
"                       \"url\":\"http://neont2/thumb2_300_400.jpg\"  "
"                  }  "
"             ]   "
"         },   "
"         {    "
"             \"pct\": 0.1, "
"             \"tid\": \"thumb3\",  "
"             \"default_url\": \"http://default_image_url.jpg\",  "
"             \"imgs\":  "
"             [   "
"                  {   "
"                       \"h\":500,  "
"                       \"w\":600,  "
"                       \"url\":\"http://neont3/thumb1_500_600.jpg\"  "
"                  },  "
"                  {  "
"                       \"h\":300,  "
"                       \"w\":400,  "
"                       \"url\":\"http://neont3/thumb2_300_400.jpg\"  "
"                  }  "
"             ]   "
"         }   "
"     ] "
"}\n"
;


char directiveBuffer[5000];

void
printDirective(FILE * f, int num, const char * expiry)
{
    int i=0;
    for(; i < num; i++)
    {
        int total = sprintf(directiveBuffer, directiveFormat, i, i, expiry);
        fwrite(directiveBuffer, 1, total, f);
    }
}


char * trailerFormat = "end";

void
printTrailer(FILE * f)
{
    fwrite(trailerFormat, 1, strlen(trailerFormat), f);
}






int
main(int argc, char ** argv)
{
    char * expiryBuffer = 0;
    char * filepath = "./mastermind";
    int expiry = 60;
    time_t now = 0;
    struct tm * gmt = 0;
    
    if(argc != 5) {
        printf("\nneon_mastermind_tester produces a test mastermind file at every");
        printf("\nexpiry interval in seconds specified in the directory specified");
        printf("\n\nby default it produces a mastermind file in current dir every 60 seconds\n");
        printf("\nusage:  neon_mastermind_tester <expiry interval> <filepath> <numOfPublishers> <numOfDirectives>\n\n");
        return 1;
    }
    
    
    expiry = atoi(argv[1]);
    filepath = argv[2];
    int publishers = atoi(argv[3]);
    int directives = atoi(argv[4]);
    
    
    printf("\nexpiry will be %d seconds", expiry);
    printf("\nfilepath is %s\n", filepath);
    expiryBuffer = (char*) malloc(256);
    
    while(1)
    {
        now = time(0);
        now += expiry;
        gmt = gmtime(&now);
        
        memset(expiryBuffer, 0, 256);
        
        snprintf(expiryBuffer, 256, "expiry=%d-%02d-%02dT%02d:%02d:%02dZ", gmt->tm_year+1900, gmt->tm_mon+1, gmt->tm_mday, gmt->tm_hour, gmt->tm_min, gmt->tm_sec);
        
        gmt = 0;
        
        // write expiry header
        FILE * f = fopen(filepath, "w");
        fwrite(expiryBuffer, 1, strlen(expiryBuffer), f);
        
        const char newline[] = "\n";
        fwrite(newline, 1, 1, f);
        
        // write publishers
        printPublisher(f, publishers);
        
        // write directives
        printDirective(f, directives, expiryBuffer);
        
        // write trailer
        printTrailer(f);
    
        fclose(f);
        sleep(expiry);
    }
}



