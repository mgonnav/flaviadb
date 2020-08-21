#include "Where.hh"

Where* Where::get(hsql::Expr* const& where_clause, hsql::DataType data_type)
{
  switch (data_type)
  {
  case hsql::DataType::INT:
    return new WhereInt(where_clause);
  case hsql::DataType::CHAR:
    return new WhereChar(where_clause);
  case hsql::DataType::DATE:
    return new WhereDate(where_clause);
  default:
    return nullptr;
  }
}

bool Where::compare_helper()
{
  switch (opType)
  {
  case hsql::kOpEquals:
    return comparison_result == 0;
  case hsql::kOpNotEquals:
    return comparison_result != 0;
  case hsql::kOpLess:
    return comparison_result == -1;
  case hsql::kOpLessEq:
    return comparison_result == -1 || comparison_result == 0;
  case hsql::kOpGreater:
    return comparison_result == 1;
  case hsql::kOpGreaterEq:
    return comparison_result == 0 || comparison_result == 1;
  default:
    return 0;
  }
}

WhereInt::WhereInt(hsql::Expr* const& where_clause)
{

  this->opType = where_clause->opType;
  this->value = where_clause->expr2->ival;
}

bool WhereInt::compare(std::string data)
{
  comparison_result = stoi(data) - value;
  if (comparison_result < 0)
    comparison_result = -1;
  else if (comparison_result > 0)
    comparison_result = 1;

  return compare_helper();
}

WhereChar::WhereChar(hsql::Expr* const& where_clause)
{
  this->opType = where_clause->opType;
  this->value = where_clause->expr2->name;
}

bool WhereChar::compare(std::string data)
{
  comparison_result = value.compare(data);
  if (comparison_result < 0)
    comparison_result = -1;
  else if (comparison_result > 0)
    comparison_result = 1;

  return compare_helper();
}

WhereDate::WhereDate(hsql::Expr* const& where_clause)
{
  this->opType = where_clause->opType;
  this->date = where_clause->expr2->name;
}

short int WhereDate::compareToDate(std::string otherDate)
{
  struct tm data_time = {0};
  struct tm expr_time = {0};

  strptime(otherDate.c_str(), DATE_FORMAT, &data_time);
  strptime(date.c_str(), DATE_FORMAT, &expr_time);

  double diff = difftime(mktime(&data_time), mktime(&expr_time));

  if (diff < 0)
    return -1;
  if (diff == 0)
    return 0;
  return 1;
}

bool WhereDate::compare(std::string data)
{
  comparison_result = compareToDate(data);

  return compare_helper();
}

bool valid_where(const hsql::Expr* where, int* where_column_pos,
                 hsql::DataType* column_data_type, std::unique_ptr<Table> const& table)
{
  if (where == nullptr)
    return 1;

  // Check WHERE clause correctness
  // Check left hand expression is a ColumnRef
  if (where->expr->type != hsql::kExprColumnRef)
  {
    fprintf(stderr, "ERROR: Left hand expression of WHERE clause must be a "
                    "column reference.\n");
    return 0;
  }

  // Check if operator is =, !=, <, <=, >, >=
  if (where->opType < 10 || where->opType > 15)
  {
    fprintf(stderr, "ERROR: Unknown operator.\n");
    return 0;
  }

  // Check right hand expression is a string or int literal
  if (where->expr2->type != hsql::kExprLiteralInt &&
      where->expr2->type != hsql::kExprLiteralString)
  {
    fprintf(stderr, "ERROR: Unknown right hand expression.\n");
    return 0;
  }

  // Check if column to be tested exists
  bool field_exists = 0;
  for (auto& col : *table->columns)
    if (strcmp(where->expr->name, col->name) == 0)
    {
      field_exists = 1;
      *column_data_type = col->type.data_type;
      *where_column_pos = &col - &table->columns->at(0);
      break;
    }

  if (!field_exists)
    throw DBException{COLUMN_NOT_IN_TABLE, table->name, where->expr->name};

  // Check column data type and literal's data type match
  if ((where->expr2->type == hsql::kExprLiteralString &&
       *column_data_type != hsql::DataType::CHAR &&
       *column_data_type != hsql::DataType::DATE) ||
      (where->expr2->type == hsql::kExprLiteralInt &&
       *column_data_type != hsql::DataType::INT))
    throw DBException{INVALID_DATA_TYPE, table->name, where->expr->name};

  if (*column_data_type == hsql::DataType::DATE)
  {
    if (strlen(where->expr2->name) > 10)
      throw DBException{INVALID_DATE, table->name, where->expr2->name};

    struct tm tm = {0};
    if (!strptime(where->expr2->name, DATE_FORMAT, &tm))
      throw DBException{INVALID_DATE, table->name, where->expr->name};
  }

  return 1;
}
