#pragma once

#include "DBException.hh"
#include "Table.hh"
#include "flaviadb_definitions.hh"
#include <hsql/SQLParser.h>

class Where
{
protected:
  hsql::OperatorType opType;
  int comparison_result;

public:
  static Where* get(hsql::Expr* const& where_clause, hsql::DataType data_type);
  bool compare_helper();
  virtual bool compare(std::string data) = 0;
};

class WhereInt : public Where
{
  int value;

public:
  WhereInt(hsql::Expr* const& where_clause);
  bool compare(std::string data);
};

class WhereChar : public Where
{
  std::string value;

public:
  WhereChar(hsql::Expr* const& where_clause);
  bool compare(std::string data);
};

class WhereDate : public Where
{
  std::string date;
  short int compareToDate(std::string date);

public:
  WhereDate(hsql::Expr* const& where_clause);
  bool compare(std::string data);
};

bool valid_where(const hsql::Expr* where, int* where_column_pos,
                 hsql::DataType* column_data_type,
                 std::unique_ptr<Table> const& table);
