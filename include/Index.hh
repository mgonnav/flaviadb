#pragma once

#include <cstring>
#include <string>

struct Index
{
  std::string name;

  Index(std::string name) : name(name) {}
  ~Index() {}
};
