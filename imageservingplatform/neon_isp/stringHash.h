#ifndef _BASE_STRINGHASH_H
#define _BASE_STRINGHASH_H

#include <string>
#include <hash_fun.h>

namespace __gnu_cxx {

template<>
struct hash<std::string> {
  std::size_t operator()(const std::string& s) const {
    return hasher_(s.c_str());
  }
  hash<const char*> hasher_;
};

template<>
struct hash<const std::string> {
  std::size_t operator()(const std::string& s) const {
    return hasher_(s.c_str());
  }
  hash<const char*> hasher_;
};

}  // namespace

#endif  // _BASE_STRINGHASH_H
