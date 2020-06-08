#ifndef TABLE_HH
#define TABLE_HH

#include <hsql/SQLParser.h>
#include <vector>
// #include "Index.hh"

struct Table {
  Table (const char* name);
  Table (const char* name, std::vector< hsql::ColumnDefinition* >* cols);

  const char* path;
  const char* regs_path;
  const char* metadata_path;
  const char* name;
  std::vector< hsql::ColumnDefinition* >* columns;
  int reg_size;

  bool insert_record( const hsql::InsertStatement* stmt );
  bool show_records( const hsql::SelectStatement* stmt );
  bool update_records( const hsql::UpdateStatement* stmt );
  bool delete_records( const hsql::DeleteStatement* stmt );
  // std::vector< Index* >* indexes;
  private:
  bool load_metadata();
};

#endif // TABLE_HH
