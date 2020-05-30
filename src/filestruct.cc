#include "filestruct.hh"
#include <fstream>
#include <cstring>
#include <stdio.h>
#include <vector>
#include <sys/stat.h>

#define FLAVIADB_TEST_DB "/home/mgonnav/.flaviadb/test/"

bool dirExists(const char* const path)
{
  struct stat info;

  int statRC = stat(path, &info);
  if (statRC != 0)
  {
    if (errno == ENOENT) // something along the path does not exist
      return 0;
    if (errno == ENOTDIR) // something in path prefix is not a dir
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
  // if (exists == 0)
  //   return 1;
  // else // -1
  //   return 0;
}

char* getString( std::vector<const char*> strings )
{
  int total_size = 0;
  for (const char* c : strings)
    total_size += strlen(c);

  char* str = new char[total_size + 1];
  strcpy(str, strings[0]);
  for (int i = 1; i < strings.size(); i++)
    strcat(str, strings[i]);

  return str;
}

char* getTablePath(const char *tableName)
{
  return getString( {FLAVIADB_TEST_DB, tableName, "/"} );
}

char* getRegistersPath(const char *tableName)
{
  return getString( {FLAVIADB_TEST_DB, tableName, "/registers/"} );
}

char* getMetadataPath(const char *tableName)
{
  return getString( {FLAVIADB_TEST_DB, tableName, "/metadata.dat"} );
}