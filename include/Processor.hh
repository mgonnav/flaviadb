#pragma once

#include "Table.hh"
#include "Where.hh"
#include "filestruct.hh"
#include "printutils.hh"
#include <filesystem>

class Processor
{
  Processor();
  ~Processor();

  // std::vector< std::unique_ptr<Table> > tables;
public:
  static bool insert_record(const hsql::InsertStatement* stmt,
                            std::unique_ptr<Table> const& table);
  static bool show_records(const hsql::SelectStatement* stmt,
                           std::unique_ptr<Table> const& table);
  static bool update_records(const hsql::UpdateStatement* stmt,
                             std::unique_ptr<Table> const& table);
  static bool delete_records(const hsql::DeleteStatement* stmt,
                             std::unique_ptr<Table> const& table);
  static bool drop_table(std::unique_ptr<Table> const& table);
  static bool create_index(std::string column,
                           std::unique_ptr<Table> const& table);
};
