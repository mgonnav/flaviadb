#pragma once

#include <string>

struct Index
{
  std::string name;

  explicit Index(std::string name) : name(name) {}
  ~Index() {}
};
