#ifndef _WHERE
#define _WHERE

#include "Table.hh"
#include "hsql/SQLParser.h"
#include <cstring>

#define DATE_FORMAT "%d-%m-%Y"

int compare_date(const char* data, const char* expr);
bool compare_helper(short int comparison_result, hsql::OperatorType opType);
bool compare_where(hsql::ColumnType* column_type, std::string data,
                   const hsql::Expr* where);
bool valid_where(const hsql::Expr* where, int* column_pos,
                 hsql::ColumnType** column_type, const Table* table);

#endif    // _WHERE
