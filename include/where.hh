#pragma once

#include "Table.hh"
#include "hsql/SQLParser.h"
#include <cstring>

int compare_date(std::string data, const char* expr);
bool compare_helper(int comparison_result, hsql::OperatorType opType);
bool compare_where(hsql::ColumnType* column_type, std::string data,
                   const hsql::Expr* where);
bool valid_where(const hsql::Expr* where, int* column_pos,
                 hsql::ColumnType** column_type, const Table* table);
