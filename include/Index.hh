#ifndef _INDEX_HH
#define _INDEX_HH
#include <cstring>
#include <string>

struct Index
{
  Index(const char* name) { this->name = name; }
  ~Index() { delete this->name; }

  const char* name;
};

#endif    // _INDEX_HH
