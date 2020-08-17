#pragma once

#include "flaviadb_definitions.hh"
#include <cstring>
#include <fstream>
#include <stdio.h>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace ftools
{
bool dirExists(std::string const& path);

bool fileExists(std::string const& path);

void createFolder(std::string const& folder_name);

std::string getTablePath(std::string const& tableName);

std::string getRegistersPath(std::string const& tableName);

std::string getIndexesPath(std::string const& tableName);

std::string getMetadataPath(std::string const& tableName);

std::string getRegCountPath(std::string const& table_name);

std::string getNewRegPath(std::string const& table_name);

int getRegCount(std::string const& table_name);
}
