#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <iostream>
#include <string>
#include <stdlib.h>





char * publisherFormat = "{\"pid\": \"pub%d\", \"aid\" : \"acc%d\" }\n";
char publisherBuffer[1024];

int
printPublisher(int argc, char ** argv)
{

    if(argc != 3) {
        std::cout << "\n\nusage:  publisherTEstFile <num of entries> <output filename>";
        return 1;
    }
    
    int num = atoi(argv[1]);
    
    FILE * f = fopen(argv[2], "w");
    
    if(f == 0) {
        std::cout << "\n\ncannot open file";
        return 1;
    }
        
    
    for(int i=0; i < num; i++)
    {
        int total = sprintf(buffer, format, i, i);
        fwrite(buffer, 1, total, f);
    }
    
    
}