#include "filestruct.hh"

namespace ftools
{
bool dirExists(std::string const& path)
{
  struct stat info;

  int statRC = stat(path.c_str(), &info);
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

bool fileExists(std::string const& path)
{
  struct stat info;
  // 0 -> found | 1 -> not found
  return !stat(path.c_str(), &info);
}

void createFolder(std::string const& folder_name)
{
  if (mkdir(folder_name.c_str(), S_IRWXU) != 0)
    throw std::invalid_argument(
        "ERROR: Couldn't create folder" + std::string(folder_name) + ".\n");
}

std::string getTablePath(std::string const& tableName)
{
  return FLAVIADB_TEST_DB + tableName + "/";
}

std::string getRegistersPath(std::string const& tableName)
{
  return FLAVIADB_TEST_DB + tableName + "/registers.sqlito";
}

std::string getIndexesPath(std::string const& tableName)
{
  return FLAVIADB_TEST_DB + tableName + "/indexes/";
}

std::string getMetadataPath(std::string const& tableName)
{
  return FLAVIADB_TEST_DB + tableName + "/metadata.dat";
}

std::string getRegCountPath(std::string const& tableName)
{
  return FLAVIADB_TEST_DB + tableName + "/reg_count.dat";
}

std::string getNewRegPath(std::string const& tableName)
{
  std::string reg_count_file_path = FLAVIADB_TEST_DB + tableName + "/reg_count.dat";
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

  std::string reg_path = getRegistersPath(tableName) + std::to_string(++count) + ".sqlito";

  std::ofstream wCount(reg_count_file_path);
  wCount << count;

  return reg_path;
}

int getRegCount(std::string const& tableName)
{
  std::string reg_count_file_path = FLAVIADB_TEST_DB + tableName + "/reg_count.dat";
  std::ifstream rCount(reg_count_file_path);
  if (!rCount.is_open())
  {
    fprintf(stderr, "ERROR: Couldn't read %s.\n", reg_count_file_path.c_str());
    return -1;
  }

  int count;
  std::string data;
  getline(rCount, data);
  rCount.close();
  count = stoi(data);

  return count;
}
}
