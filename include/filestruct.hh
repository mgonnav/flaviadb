#ifndef _FILE_TOOLS
#define _FILE_TOOLS

#include <string>

namespace ftools
{
bool dirExists(const char* const path);
bool fileExists(const char* const path);
char* getTablePath(const char* tableName);
char* getRegistersPath(const char* tableName);
char* getMetadataPath(const char* tableName);
std::string getCurrentTimeAsString();
char* getRegPath(const char* current_regs_path, int salt = 0);
}    // namespace ftools

#endif    // _FILE_TOOLS
