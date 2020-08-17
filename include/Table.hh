#ifndef TABLE_HH
#define TABLE_HH

#include "DBException.hh"
#include "Index.hh"
#include "Register.hh"
#include "Table.hh"
#include "filestruct.hh"
#include <algorithm>    // unique
#include <cstring>
#include <ctime>    // strptime
#include <filesystem>
#include <fstream>
#include <hsql/SQLParser.h>
#include <iomanip>    // Better print formatting
#include <iostream>
#include <map>
#include <set>
#include <sys/stat.h>
#include <vector>

#define DATE_FORMAT "%d-%m-%Y"

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
  std::map<std::string, Register*>* registers;
  std::vector<hsql::ColumnDefinition*>* columns;
  std::vector<Index*>* indexes;
  int reg_size;
  int reg_count;

  bool insert_record(const hsql::InsertStatement* stmt);
  bool show_records(const hsql::SelectStatement* stmt) const;
  bool update_records(const hsql::UpdateStatement* stmt);
  bool delete_records(const hsql::DeleteStatement* stmt);
  bool drop_table();
  bool create_index(std::string column);

private:
  bool load_metadata();

  std::ifstream metadata_file;
  void loadPaths();
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
};

#endif    // TABLE_HH
