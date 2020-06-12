#include "printutils.hh"
#include <hsql/SQLParser.h>
#include <cstring>

namespace printUtils {
  void print_welcome_message()
  {
    std::cout << "\e[1mWelcome to the FlaviaDB monitor. Commands end with ;.\e[0m\n";
    std::cout << "\e[1mUse standard SQL statements.\n";
    std::cout << "\e[1mServer version: 0.0.1-FlaviaDB-0ubuntu0.19.10.1 Ubuntu 19.10\e[0m\n\n";

    std::cout << "\e[1mCopyright (c) 2019, FlaviaDB Corporation.\e[0m\n\n";
  }

  std::string centered(std::string str, size_t width)
  {
    int spaces = 0;
    spaces = ( width - str.size() ) / 2;
    if ( spaces > 0 ) 
      str = std::string(spaces, ' ') + str;
    return str;
  }

  void print_row( const std::vector< std::string >* row, const std::vector< size_t >* fields_width, std::string separator="|")
  {
    std::cout << std::left << std::setw(1) << "|";
    for (size_t i = 0; i < row->size(); i++)
    {
      std::cout << std::setw( fields_width->at(i) ) << centered(row->at(i), fields_width->at(i));
      std::cout << ((i == row->size()-1) ? "|" : separator);
    }
    std::cout << "\n";
  }

  void print_select_result(std::vector< hsql::ColumnDefinition* >* columns, std::vector< std::vector< std::string > >* regs_data, std::vector< size_t >* fields_width)
  {
    // header[0] has the first row with the column names
    std::vector< std::vector<std::string> > header;
    header.push_back(std::vector<std::string>());
    for (auto col : *columns)
      header[0].push_back(col->name);

    // header[1] has the second row with '-' times the width of the field
    header.push_back(std::vector<std::string>());
    for (size_t i = 0; i < fields_width->size(); i++)
      header[1].push_back( std::string(fields_width->at(i), '-') );

    std::cout << "\n";
    print_row(&header[0], fields_width);
    print_row(&header[1], fields_width, "+");
    for (auto row : *regs_data)
      print_row(&row, fields_width);
  }

  void print_select_result(std::vector< hsql::Expr* >* fields, std::vector< std::vector< std::string > >* regs_data, std::vector< size_t >* fields_width)
  {
    // header[0] has the first row with the column names
    std::vector< std::vector<std::string> > header;
    header.push_back(std::vector<std::string>());
    for (auto col : *fields)
      header[0].push_back(col->name);

    // header[1] has the second row with '-' times the width of the field
    header.push_back(std::vector<std::string>());
    for (size_t i = 0; i < fields_width->size(); i++)
      header[1].push_back( std::string(fields_width->at(i), '-') );

    std::cout << "\n";
    print_row(&header[0], fields_width);
    print_row(&header[1], fields_width, "+");
    for (auto& row : *regs_data)
      print_row(&row, fields_width);
  }

  char* dataType_to_char(hsql::ColumnType type)
  {
    char* type_char = new char[10];
    switch (type.data_type)
    {
      case hsql::DataType::CHAR:
        strcpy(type_char, "CHAR(");
        strcat(type_char, std::to_string(type.length).c_str());
        strcat(type_char, ")");
        break;
      case hsql::DataType::INT:
        strcpy(type_char, "INT");
        break;
      case hsql::DataType::DATE:
        strcpy(type_char, "DATE");
        break;
    }

    return type_char;
  }

  void print_table_desc(Table* table)
  {
    std::vector< size_t > fields_width;
    fields_width.push_back(8); // Size of "Column" + 2
    fields_width.push_back(6); // Size of "Type" + 2
    
    // Find max width for first column
    for (const auto& col : *table->columns)
      if (fields_width[0] <= strlen(col->name))
        fields_width[0] = strlen(col->name) + 2;

    // Find max width for second column
    for (const auto& col : *table->columns)
      if (fields_width[1] <= strlen( dataType_to_char(col->type) ))
        fields_width[1] = strlen( dataType_to_char(col->type) ) + 2;

    // header[0] has the first row with the column names
    std::vector< std::vector<std::string> > header;
    header.push_back( std::vector<std::string>() );
    header[0].push_back("Column");
    header[0].push_back("Type");

    // header[1] has the second row with '-' times the width of the field 
    header.push_back( std::vector<std::string>() );
    for (size_t i = 0; i < 2; i++)
      header[1].push_back( std::string(fields_width[i], '-') );

    std::cout << "\n";
    print_row(&header[0], &fields_width);
    print_row(&header[1], &fields_width, "+");

    std::vector< std::vector< std::string > > cols_info;
    for (const auto& col : *table->columns)
    {
      cols_info.push_back( std::vector<std::string>() );
      cols_info.back().push_back( std::string(col->name) );
      cols_info.back().push_back( std::string( dataType_to_char(col->type) ));
      print_row( &cols_info.back(), &fields_width);
    }
    std::cout << "\n";
  }
}
