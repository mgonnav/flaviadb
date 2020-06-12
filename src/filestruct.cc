#include "filestruct.hh"
#include <cstring>
#include <fstream>
#include <stdio.h>
#include <sys/stat.h>
#include <vector>

#define FLAVIADB_TEST_DB "/home/mgonnav/.flaviadb/test/"

namespace ftools
{
bool dirExists(const char* const path)
{
  struct stat info;

  int statRC = stat(path, &info);
  if (statRC != 0)
  {
    if (errno == ENOENT)    // something along the path does not exist
      return 0;
    if (errno == ENOTDIR)    // something in path prefix is not a dir
      return 0;
    return -1;
  }

  return (info.st_mode & S_IFDIR) ? 1 : 0;
}

bool fileExists(const char* const path)
{
  struct stat info;
  // 0 -> found | 1 -> not found
  return !stat(path, &info);
}

char* getString(std::vector<const char*> strings)
{
  int total_size = 0;
  for (const char* c : strings)
    total_size += strlen(c);

  char* str = new char[total_size + 1];
  strcpy(str, strings[0]);
  for (size_t i = 1; i < strings.size(); i++)
    strcat(str, strings[i]);

  return str;
}

char* getTablePath(const char* tableName)
{
  return getString({FLAVIADB_TEST_DB, tableName, "/"});
}

char* getRegistersPath(const char* tableName)
{
  return getString({FLAVIADB_TEST_DB, tableName, "/registers/"});
}

char* getMetadataPath(const char* tableName)
{
  return getString({FLAVIADB_TEST_DB, tableName, "/metadata.dat"});
}

std::string getCurrentTimeAsString()
{
  time_t rawtime;
  struct tm* timeinfo;
  char buffer[80];

  time(&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(buffer, sizeof(buffer), "%d%m%Y%H%M%S", timeinfo);
  std::string str(buffer);

  return str;
}

char* getRegPath(const char* current_regs_path, int salt)
{
  char* reg_path = new char[80];

  strcpy(reg_path, current_regs_path);
  strcat(reg_path, getCurrentTimeAsString().c_str());
  strcat(reg_path, std::to_string(salt).c_str());
  strcat(reg_path, ".sqlito");

  return reg_path;
}
}    // namespace ftools
