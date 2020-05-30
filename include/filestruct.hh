#ifndef _FILE_STRUCT
#define _FILE_STRUCT

bool dirExists (const char* const path);
bool fileExists (const char* const path);
char* getTablePath (const char *tableName);
char* getRegistersPath (const char *tableName);
char* getMetadataPath (const char *tableName);

#endif // _FILE_STRUCT