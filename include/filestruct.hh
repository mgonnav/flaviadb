#ifndef _FILE_TOOLS
#define _FILE_TOOLS

#include "flaviadb_paths.hh"
#include <cstring>
#include <fstream>
#include <stdio.h>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace ftools
{
bool dirExists(const char* const path);

bool fileExists(const char* const path);

char* getString(std::vector<const char*> strings);

char* getTablePath(const char* tableName);

char* getRegistersPath(const char* tableName);

char* getIndexesPath(const char* tableName);

char* getMetadataPath(const char* tableName);

char* getRegCountPath(const char* table_name);

std::string getCurrentTimeAsString();

char* getNewRegPath(const char* table_name);

int getRegCount(const char* table_name);
}    // namespace ftools

#endif    // _FILE_TOOLS
