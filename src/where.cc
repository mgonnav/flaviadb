#include "where.hh"

int compare_date(std::string data, const char* expr)
{
  struct tm data_time = {0};
  struct tm expr_time = {0};

  strptime(data.c_str(), DATE_FORMAT, &data_time);
  strptime(expr, DATE_FORMAT, &expr_time);

  double diff = difftime(mktime(&data_time), mktime(&expr_time));

  if (diff < 0)
    return -1;
  if (diff == 0)
    return 0;
  return 1;
}

bool compare_helper(int comparison_result, hsql::OperatorType opType)
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

bool compare_where(hsql::ColumnType* column_type, std::string data,
                   const hsql::Expr* where)
{
  if (where == nullptr)
    return 1;

  int comparison;

  switch (column_type->data_type)
  {
  case hsql::DataType::CHAR:
    comparison = strcmp(data.c_str(), where->expr2->name);
    break;
  case hsql::DataType::DATE:
    comparison = compare_date(data, where->expr2->name);
    break;
  case hsql::DataType::INT:
    comparison = stoi(data) - where->expr2->ival;
    if (comparison < 0)
      comparison = -1;
    else if (comparison > 0)
      comparison = 1;
    break;
  default:
    comparison = 0;
    break;
  }

  return compare_helper(comparison, where->opType);
}

bool valid_where(const hsql::Expr* where, int* where_column_pos,
                 hsql::ColumnType** column_type, const Table* table)
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
  if (where->opType < 10 && where->opType > 15)
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
      *column_type = &col->type;
      *where_column_pos = &col - &table->columns->at(0);
      break;
    }

  if (!field_exists)
    throw DBException{COLUMN_NOT_IN_TABLE, table->name, where->expr->name};

  // Check column data type and literal's data type match
  if ((where->expr2->type == hsql::kExprLiteralString &&
       (*column_type)->data_type != hsql::DataType::CHAR &&
       (*column_type)->data_type != hsql::DataType::DATE) ||
      (where->expr2->type == hsql::kExprLiteralInt &&
       (*column_type)->data_type != hsql::DataType::INT))
    throw DBException{INVALID_DATA_TYPE, table->name, where->expr->name};

  if ((*column_type)->data_type == hsql::DataType::DATE)
  {
    if (strlen(where->expr2->name) > 10)
      throw DBException{INVALID_DATE, table->name, where->expr2->name};

    struct tm tm = {0};
    if (!strptime(where->expr2->name, DATE_FORMAT, &tm))
      throw DBException{INVALID_DATE, table->name, where->expr->name};
  }

  return 1;
}
