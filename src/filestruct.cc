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

char* getIndexesPath(const char* tableName)
{
  return getString({FLAVIADB_TEST_DB, tableName, "/indexes/"});
}

char* getMetadataPath(const char* tableName)
{
  return getString({FLAVIADB_TEST_DB, tableName, "/metadata.dat"});
}

char* getRegCountPath(const char* table_name)
{
  return getString({FLAVIADB_TEST_DB, table_name, "/reg_count.dat"});
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

char* getNewRegPath(const char* table_name)
{
  char* reg_path = new char[80];
  std::string reg_count_file_path =
      getString({FLAVIADB_TEST_DB, table_name, "/reg_count.dat"});
  std::ifstream rCount(reg_count_file_path);
  if (!rCount.is_open())
  {
    fprintf(stderr, "ERROR: Couldn't read %s.\n", reg_count_file_path.c_str());
    return nullptr;
  }

  int count;
  std::string data;
  getline(rCount, data);
  rCount.close();
  count = stoi(data);

  strcpy(reg_path, getRegistersPath(table_name));
  strcat(reg_path, std::to_string(count).c_str());
  strcat(reg_path, ".sqlito");

  std::ofstream wCount(reg_count_file_path);
  wCount << ++count;

  return reg_path;
}
}    // namespace ftools
