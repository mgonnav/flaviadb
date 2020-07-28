#ifndef TABLE_HH
#define TABLE_HH

#include "Index.hh"
#include <hsql/SQLParser.h>
#include <vector>

struct Table
{
  Table(const char* name);
  Table(const char* name, std::vector<hsql::ColumnDefinition*>* cols);
  ~Table();

  const char* path;
  const char* regs_path;
  const char* metadata_path;
  const char* indexes_path;
  const char* name;
  std::vector<hsql::ColumnDefinition*>* columns;
  std::vector<Index*>* indexes;
  int reg_size;

  bool insert_record(const hsql::InsertStatement* stmt);
  bool show_records(const hsql::SelectStatement* stmt);
  bool update_records(const hsql::UpdateStatement* stmt);
  bool delete_records(const hsql::DeleteStatement* stmt);
  bool drop_table();
  bool create_index(const char* column);

private:
  bool load_metadata();
};

#endif    // TABLE_HH
