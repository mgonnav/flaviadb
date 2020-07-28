#ifndef _INDEX_HH
#define _INDEX_HH
#include <string>
#include <cstring>

struct Index
{
  Index(const char* name) { this->name = name; }
  ~Index() { delete this->name; }

  const char* name;
};

#endif    // _INDEX_HH

