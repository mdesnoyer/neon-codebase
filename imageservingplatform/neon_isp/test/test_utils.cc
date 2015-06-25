#include "test_utils.hpp"

using namespace std; 

string 
TestUtils::readTestFile(std::string fileName) { 
    boost::scoped_ptr<char> curPath(strdup(__FILE__));
    string curDir = dirname(curPath.get());
    string goodDirective = curDir + "/testFiles/" + fileName; /*noUrlsGoodDirective.json"*/;
    ifstream readMe(goodDirective); 
    stringstream ss; 
    ss << readMe.rdbuf(); 
    return ss.str(); 
}
