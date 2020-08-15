#pragma once
#include <vector>

struct Register {
  std::vector<std::string> data;

  Register(std::vector<std::string> values)
    : data{values}
  {}
  ~Register();
};