#pragma once

#include "DBException.hh"
#include "Index.hh"
#include "filestruct.hh"
#include <algorithm>    // find
#include <filesystem>
#include <fstream>
#include <hsql/SQLParser.h>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <vector>

#define DATE_FORMAT "%d-%m-%Y"
typedef std::vector<std::string> RegisterData;

struct Table
{
  Table(std::string name);
  Table(std::string name, std::vector<hsql::ColumnDefinition*>* cols);
  ~Table();

  std::string name;
  std::string path;
  std::string regs_path;
  std::string metadata_path;
  std::string indexes_path;
  std::unique_ptr<std::list<std::pair<std::string, RegisterData>>> registers;
  std::vector<hsql::ColumnDefinition*>* columns;
  std::vector<Index*>* indexes;
  int reg_size;
  int reg_count;

private:
  bool load_metadata();

  std::ifstream metadata_file;
  void loadPaths(std::string const& name);
  void checkTableExists();
  void loadIndexes();
  void loadStoredRegisters();
  void createTableFolders();
  int calculateRegSize();
  void openMetadataFile();
  void loadMetadataHeader();
  void loadTableName();
  void loadColumnCount();
  void loadRegSize();
  void loadColumnData(hsql::ColumnDefinition*& column);
  hsql::ColumnType getColumnType();

  friend class Processor;
};
