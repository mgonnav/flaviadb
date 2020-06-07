#include "Table.hh"
#include <hsql/SQLParser.h>
#include <sys/stat.h>
#include <cstring>
#include <fstream>
#include <ctime>          // strftime, strptime
#include <iomanip>        // Better print formatting
#include <algorithm>      // unique
#include <set>
#include "filestruct.hh"
#include <iostream> // TESTING

namespace ft = ftools;

Table::Table(const char* name)
{
  this->name = name;
  this->path = ft::getTablePath(this->name);
  this->regs_path = ft::getRegistersPath(this->name);

  if ( !ft::dirExists( this->path ) )
  {
    // fprintf(stderr, "Table %s doesn't exist!", this->name);
    throw std::invalid_argument("Table doesn't exist");
  }

  this->metadata_path = ft::getMetadataPath( this->name );
  if ( !this->load_metadata() )
    throw std::invalid_argument("Error while loading metadata");
}

Table::Table(const char* name, std::vector< hsql::ColumnDefinition* >* cols)
{
  this->name = name;
  this->path = ft::getTablePath(this->name);
  this->regs_path = ft::getRegistersPath(this->name);

  if ( mkdir(this->path, S_IRWXU) != 0 )
  {
    fprintf(stderr, "ERROR: Couldn't create table\n");
    return;
  }
  if ( mkdir(this->regs_path, S_IRWXU) != 0 )
  {
    fprintf(stderr, "ERROR: Couldn't create registers folder\n");
    return;
  }

  this->columns = cols;
  reg_size = 0;
  for (const hsql::ColumnDefinition* col : *this->columns)
  {
    if      ( col->type.data_type == hsql::DataType::INT )
      reg_size += 4;
    else if ( col->type.data_type == hsql::DataType::CHAR )
      reg_size += col->type.length;
    else if ( col->type.data_type == hsql::DataType::DATE )
      reg_size += 8;
  }

  // Create medata.dat file for table
  // and fill it with table's name & cols info
  this->metadata_path = ft::getMetadataPath(this->name);
  std::ofstream wMetadata (this->metadata_path);
  wMetadata << this->name << "\t" << this->columns->size() << "\t" << this->reg_size << "\n";
  for (const hsql::ColumnDefinition* col : *this->columns)
    wMetadata << col->name << "\t" << (int)col->type.data_type << "\t" << col->type.length << "\t" << col->nullable << "\n";
  wMetadata.close();

  fprintf(stdout, "Table %s was created successfully\n", this->name); 
}

bool Table::load_metadata()
{
  std::ifstream rMetadata ( this->metadata_path );
  if ( !rMetadata )
  {
     fprintf(stderr, "ERROR: metadata.dat doesn't exist\n");
     return 0;
  }

  std::string data;
  // Read first line's data
  // Read table name
  getline(rMetadata, data, '\t');
  if (this->name != data)
  {
    fprintf(stderr, "ERROR: Metadata's stored table name doesn't match requested table\n");
    return 0; }
  // Read total # of columns in table
  getline(rMetadata, data, '\t');
  int total_cols = atoi(data.c_str());
  // Read reg_size
  getline(rMetadata, data, '\n'); this->reg_size = atoi(data.c_str()); 
  this->columns = new std::vector< hsql::ColumnDefinition* >;
  while ( total_cols-- )
  {
    // Read columns' data
    // Read column name
    getline(rMetadata, data, '\t');
    char* col_name = new char[data.size()];
    strcpy(col_name, (char*)data.c_str());

    // Read column type
    getline(rMetadata, data, '\t');
    int col_enum = atoi(data.c_str());

    // Read column length, e.g., CHAR(20) where 20 is the length
    getline(rMetadata, data, '\t');
    int col_length = atoi(data.c_str());

    // Create ColumnType object from previous data
    hsql::ColumnType col_type ((hsql::DataType)col_enum, col_length);

    // Read column nullability
    getline(rMetadata, data, '\n');
    bool col_nullable = (bool)(data.c_str());

    // Add column to cols vector of Table
    this->columns->push_back( new hsql::ColumnDefinition(col_name, col_type, col_nullable) );
  }

  //std::cout << "Insert Table: \n";
  //for (int i = 0; i < this->columns->size(); i++)
  //  std::cout << "Col #" << i << " " << this->columns->at(i)->name << " " << this->columns->at(i)->type.data_type << " " << this->columns->at(i)->type.length << " "<< this->columns->at(i)->nullable << "\n";

  return 1;
}

bool Table::insert_record( const hsql::InsertStatement* stmt )
{
  if ( stmt->columns != nullptr )
  {
    return 1;
  }
  else
  {
    if ( stmt->values->size() < this->columns->size() )
    {
      fprintf(stderr, "ERROR: If you are ignoring some values, you need to provide the columns where stated values will be inserted\n");
      return 0;
    }
    if ( stmt->values->size() > this->columns->size() )
    {
      fprintf(stderr, "ERROR: Received more values than columns in table.\n");
      return 0;
    }

    // Create filename
    char* reg_file = ft::getRegPath(this->regs_path);

    std::ofstream new_reg ( reg_file );
    for (size_t i = 0; i < stmt->values->size(); i++)
    {
      hsql::Expr* value = stmt->values->at(i);
      hsql::ColumnDefinition* column = this->columns->at(i);
      if ( value->type == hsql::ExprType::kExprLiteralString )
      {
        if      ( column->type.data_type == hsql::DataType::CHAR )
        {
          if ( strlen(value->name) <= column->type.length )
            new_reg << value->name;
          else
          {
            fprintf(stderr, "ERROR: CHAR value inserted for column %s is too big. Max length is %ld.\n", column->name, column->type.length);
            new_reg.close();
            remove(reg_file);
            return 0;
          }
        }
        else if ( column->type.data_type == hsql::DataType::DATE )
        {
          struct tm tm;
          if ( strptime(value->name, "%d-%m-%Y", &tm ) )
            new_reg << value->name;
          else
          {
            fprintf(stderr, "ERROR: '%s' isn't correctly formatted. Please use dd-mm-YYYY format.\n", value->name);
            new_reg.close();
            remove(reg_file);
            return 0;
          }
        }
        else
        {
          fprintf(stderr, "ERROR: %s.%s data type doesn't match received value's data type.\n", this->name, column->name);
          new_reg.close();
          remove(reg_file);
          return 0;
        }
      }
      else if ( value->type == hsql::ExprType::kExprLiteralInt 
             && column->type.data_type == hsql::DataType::INT )
      {
        new_reg << value->ival;
      }
      else {
        fprintf(stderr, "ERROR: %s.%s data type doesn't match received value's data type.\n", this->name, column->name);
        new_reg.close();
        remove(reg_file);
        return 0;
      }
      new_reg << "\t";
    }
    new_reg.close();
  } // TODO: REFACTOR ERRORS

  //for (auto i = 0; i < stmt->values->size(); i++)
  //  std::cout << "expr #" << i << " type: " << stmt->values->at(i)->type << "\n";

  return 1;
}

#include <filesystem>
#include "printutils.hh"
namespace fs = std::filesystem;
namespace pu = printUtils;

bool Table::show_records( const hsql::SelectStatement* stmt )
{
  //std::cout << "FromTable: " << stmt->fromTable->name << "\n";
  //for (const auto& st : *stmt->selectList)
  //  std::cout << "expr: " << st->type << "\n";

  if ( stmt->selectList->size() == 1
      && stmt->selectList->at(0)->type == hsql::ExprType::kExprStar )
  {
    // REFACTOR WHOLE THING
    std::vector< size_t > fields_width (this->columns->size(), 0);
    for (auto i = 0; i < this->columns->size(); i++)
      fields_width[i] = strlen(this->columns->at(i)->name) + 2;
    std::vector< std::vector< std::string > > regs_data;

    for (const auto& reg : fs::directory_iterator(this->regs_path))
    {
      // Load data in file to reg_data
      std::ifstream data_file ( reg.path() );
      std::vector<std::string> reg_data;
      
      // Load each piece of data into reg_data
      // and change max field_width if necessary
      std::string data;
      for (auto i = 0; i < this->columns->size(); i++)
      {
        getline(data_file, data, '\t');
        reg_data.push_back(data);
        if (fields_width[i] <= data.size())
          fields_width[i] = data.size() + 2;
      }

      regs_data.push_back(reg_data);
    }

    pu::print_select_result(this->columns, &regs_data, &fields_width);
    return 1;
  }

  // SELECT specific columns
  // CHECK ALL REQUESTED ARE DIFFERENT FROM *,
  // THERE AREN'T ANY DUPLICATES &
  // ALL FIELDS EXIST
  std::set< std::string > tmp;
  std::vector< size_t > fields_width;
  std::vector< int > requested_columns_order;
  for (auto& field : *stmt->selectList)
  {
    // If field is *, error 
    if (field->type == hsql::ExprType::kExprStar)
    {
      fprintf(stderr, "ERROR: Can't use * along with other fields.\n");
      return 0;
    }
    
    // If field is a duplicate, error
    tmp.insert( std::string(field->name) );
    if ( tmp.size() != (&field - &stmt->selectList->at(0))+1 )
    {
      fprintf(stderr, "ERROR: Duplicate fields detected in query.\n");
      return 0;
    }

    // Assert requested field exists. If it does, add needed data
    // to fields_width & requested_columns_order
    bool field_exists = 0;
    for (auto& col : *this->columns)
    {
      if ( strcmp(field->name, col->name) == 0 ) // If strings are equal
      {
        field_exists = 1;
        fields_width.push_back( strlen(col->name) + 2 );
        requested_columns_order.push_back(&col - &this->columns->at(0));
        break;
      }
    }

    if (!field_exists)
    {
      fprintf(stderr, "ERROR: Column %s not found in table %s.\n", field->name, this->name);
      return 0;
    }
  }

  // COLLECT DATA FROM ALL REGS
  std::vector< std::vector< std::string > > regs_data;
  for (const auto& reg : fs::directory_iterator(this->regs_path))
  {
    // Load data in file to reg_data
    std::ifstream data_file ( reg.path() );
    std::vector<std::string> reg_data(stmt->selectList->size(), "");

    // Load piece of data
    std::string data;
    for (auto i = 0; i < this->columns->size(); i++)
    {
      getline(data_file, data, '\t');

      // Find index of current field in the requested order
      auto field_pos = std::find(requested_columns_order.begin(),
                                 requested_columns_order.end(),
                                 i);

      // If field was requested, add it to reg_data
      if ( field_pos != requested_columns_order.end() )
      {
        auto current_req_field = std::distance(requested_columns_order.begin(), field_pos);
        reg_data[current_req_field] = data;
        // If data witdth is larger than current field_width,
        // make it the new field_width
        if ( fields_width[ current_req_field ] <= data.size() )
          fields_width[ current_req_field ] = data.size() + 2;
      } 
    }
    regs_data.push_back(reg_data);
  }

  pu::print_select_result(stmt->selectList, &regs_data, &fields_width);

  return 1;
}
