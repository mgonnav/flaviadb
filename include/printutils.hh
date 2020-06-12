#include "Table.hh"
#include <hsql/SQLParser.h>
#include <iomanip>
#include <iostream>
#include <vector>

namespace printUtils
{
void print_welcome_message();

void print_row(const std::vector<std::string>* row,
               const std::vector<size_t>* fields_width, std::string separator);

void print_select_result(std::vector<hsql::ColumnDefinition*>* columns,
                         std::vector<std::vector<std::string>>* regs_data,
                         std::vector<size_t>* fields_width);

void print_select_result(std::vector<hsql::Expr*>* fields,
                         std::vector<std::vector<std::string>>* regs_data,
                         std::vector<size_t>* fields_width);

void print_table_desc(Table* table);
}    // namespace printUtils
