#ifndef _INDEX_HH
#define _INDEX_HH
#include <cstring>
#include <string>

struct Index
{
  std::string name;

  Index(std::string name) : name(name) {}
  ~Index() {}
};

#endif    // _INDEX_HH
