#ifndef TABLE_HH
#define TABLE_HH

#include <hsql/SQLParser.h>
// #include "Index.hh"

struct Table {
  Table (const char* name, std::vector< hsql::ColumnDefinition* >* cols);

  const char* path;
  const char* regs_path;
  const char* metadata_path;
  const char* name;
  std::vector< hsql::ColumnDefinition* >* cols;
  int reg_size;

  // bool add_record( std::vector<Expr*>* );
  // std::vector< Index* >* indexes;
};

#endif // TABLE_HH