#include "Table.hh"
#include "filestruct.hh"
#include "where.hh"
#include <algorithm>    // unique
#include <cstring>
#include <ctime>    // strptime
#include <fstream>
#include <hsql/SQLParser.h>
#include <iomanip>    // Better print formatting
#include <iostream>
#include <set>
#include <sys/stat.h>

#define DATE_FORMAT "%d-%m-%Y"
namespace ft = ftools;

Table::Table(const char* name)
{
  this->name = name;
  this->path = ft::getTablePath(this->name);
  this->regs_path = ft::getRegistersPath(this->name);

  if (!ft::dirExists(this->path))
    throw std::invalid_argument("Table " + std::string(this->name) +
                                " doesn't exist.\n");

  this->metadata_path = ft::getMetadataPath(this->name);
  if (!this->load_metadata())
    throw std::invalid_argument("ERROR while loading metadata for table " +
                                std::string(this->name) + ".\n");
}

Table::Table(const char* name, std::vector<hsql::ColumnDefinition*>* cols)
{
  this->name = name;
  this->path = ft::getTablePath(this->name);
  this->regs_path = ft::getRegistersPath(this->name);

  if (mkdir(this->path, S_IRWXU) != 0)
    throw std::invalid_argument("ERROR: Couldn't create table " +
                                std::string(this->name) + ".\n");

  if (mkdir(this->regs_path, S_IRWXU) != 0)
    throw std::invalid_argument(
        "ERROR: Couldn't create registers folder for table " +
        std::string(this->name) + ".\n");

  this->columns = cols;
  reg_size = 0;
  for (const hsql::ColumnDefinition* col : *this->columns)
  {
    if (col->type.data_type == hsql::DataType::INT)
      reg_size += 4;
    else if (col->type.data_type == hsql::DataType::CHAR)
      reg_size += col->type.length;
    else if (col->type.data_type == hsql::DataType::DATE)
      reg_size += 8;
  }

  // Create medata.dat file for table
  // and fill it with table's name & cols info
  this->metadata_path = ft::getMetadataPath(this->name);
  std::ofstream wMetadata(this->metadata_path);
  wMetadata << this->name << "\t" << this->columns->size() << "\t"
            << this->reg_size << "\n";
  for (const hsql::ColumnDefinition* col : *this->columns)
    wMetadata << col->name << "\t" << (int)col->type.data_type << "\t"
              << col->type.length << "\t" << col->nullable << "\n";
  wMetadata.close();

  std::cout << "Table " << this->name << " was created successfully.\n";
}

bool Table::load_metadata()
{
  std::ifstream rMetadata(this->metadata_path);
  if (!rMetadata)
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
    fprintf(
        stderr,
        "ERROR: Metadata's stored table name doesn't match requested table\n");
    return 0;
  }
  // Read total # of columns in table
  getline(rMetadata, data, '\t');
  int total_cols = atoi(data.c_str());
  // Read reg_size
  getline(rMetadata, data, '\n');
  this->reg_size = atoi(data.c_str());
  this->columns = new std::vector<hsql::ColumnDefinition*>;
  while (total_cols--)
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
    hsql::ColumnType col_type((hsql::DataType)col_enum, col_length);

    // Read column nullability
    getline(rMetadata, data, '\n');
    bool col_nullable = (bool)(data.c_str());

    // Add column to cols vector of Table
    this->columns->push_back(
        new hsql::ColumnDefinition(col_name, col_type, col_nullable));
  }

  // std::cout << "Insert Table: \n";
  // for (int i = 0; i < this->columns->size(); i++)
  //  std::cout << "Col #" << i << " " << this->columns->at(i)->name << " " <<
  //  this->columns->at(i)->type.data_type << " " <<
  //  this->columns->at(i)->type.length << " "<< this->columns->at(i)->nullable
  //  << "\n";

  return 1;
}

bool Table::insert_record(const hsql::InsertStatement* stmt)
{
  if (stmt->columns != nullptr)
  {
    return 1;    // TODO: Insert on specific columns
  }
  else
  {
    if (stmt->values->size() < this->columns->size())
    {
      fprintf(stderr,
              "ERROR: If you are ignoring some values, you need to provide the "
              "columns where stated values will be inserted\n");
      return 0;
    }
    if (stmt->values->size() > this->columns->size())
    {
      fprintf(stderr, "ERROR: Received more values than columns in table.\n");
      return 0;
    }

    // Create filename
    char* reg_file = ft::getRegPath(this->regs_path);

    std::ofstream new_reg(reg_file);
    for (size_t i = 0; i < stmt->values->size(); i++)
    {
      hsql::Expr* value = stmt->values->at(i);
      hsql::ColumnDefinition* column = this->columns->at(i);
      if (value->type == hsql::kExprLiteralString)
      {
        if (column->type.data_type == hsql::DataType::CHAR)
        {
          if (strlen(value->name) <= column->type.length)
            new_reg << value->name;
          else
          {
            fprintf(stderr,
                    "ERROR: CHAR value inserted for column %s is too big. Max "
                    "length is %ld.\n",
                    column->name, column->type.length);
            new_reg.close();
            remove(reg_file);
            return 0;
          }
        }
        else if (column->type.data_type == hsql::DataType::DATE)
        {
          if (strlen(value->name) > 10)
          {
            fprintf(stderr,
                    "ERROR: '%s' isn't a valid date or isn't a date. Max YEAR "
                    "value supported is 9999.\n",
                    value->name);
            new_reg.close();
            remove(reg_file);
            return 0;
          }

          struct tm tm = {0};
          if (strptime(value->name, DATE_FORMAT, &tm))
            new_reg << value->name;
          else
          {
            fprintf(stderr,
                    "ERROR: '%s' isn't correctly formatted or isn't a date. "
                    "Please use %s format.\n",
                    value->name, DATE_FORMAT);
            new_reg.close();
            remove(reg_file);
            return 0;
          }
        }
        else
        {
          fprintf(stderr,
                  "ERROR: %s.%s data type doesn't match received value's data "
                  "type.\n",
                  this->name, column->name);
          new_reg.close();
          remove(reg_file);
          return 0;
        }
      }
      else if (value->type == hsql::kExprLiteralInt &&
               column->type.data_type == hsql::DataType::INT)
      {
        new_reg << value->ival;
      }
      else
      {
        fprintf(stderr,
                "ERROR: %s.%s data type doesn't match received value's data "
                "type.\n",
                this->name, column->name);
        new_reg.close();
        remove(reg_file);
        return 0;
      }
      new_reg << "\t";
    }
    new_reg.close();
  }    // TODO: REFACTOR ERRORS

  std::cout << "Inserted 1 row.\n";
  return 1;
}

#include "printutils.hh"
#include <filesystem>
namespace fs = std::filesystem;
namespace pu = printUtils;

bool Table::show_records(const hsql::SelectStatement* stmt)
{
  // Check WHERE clause correctness
  int where_column_pos;
  hsql::ColumnType* column_type;
  if (!valid_where(stmt->whereClause, &where_column_pos, &column_type, this))
    return 0;

  std::set<std::string> tmp;
  std::vector<size_t> fields_width;
  std::vector<int> requested_columns_order;

  if (stmt->selectList->size() == 1 &&
      stmt->selectList->at(0)->type == hsql::kExprStar)
  {
    delete stmt->selectList->back();
    stmt->selectList->pop_back();
    for (auto i = 0; i < this->columns->size(); i++)
    {
      stmt->selectList->push_back(new hsql::Expr(hsql::kExprColumnRef));
      stmt->selectList->at(i)->name = this->columns->at(i)->name;
      fields_width.push_back(strlen(this->columns->at(i)->name) + 2);
      requested_columns_order.push_back(i);
    }
  }
  else
  {
    // SELECT specific columns
    // CHECK ALL REQUESTED ARE DIFFERENT FROM *,
    // THERE AREN'T ANY DUPLICATES &
    // ALL FIELDS EXIST
    for (auto& field : *stmt->selectList)
    {
      // If field is *, error
      if (field->type == hsql::kExprStar)
      {
        fprintf(stderr, "ERROR: Can't use * along with other fields.\n");
        return 0;
      }

      // If field is a duplicate, error
      tmp.insert(std::string(field->name));
      if (tmp.size() != (&field - &stmt->selectList->at(0)) + 1)
      {
        fprintf(stderr, "ERROR: Duplicate fields detected in query.\n");
        return 0;
      }

      // Assert requested field exists. If it does, add needed data
      // to fields_width & requested_columns_order
      bool field_exists = 0;
      for (auto& col : *this->columns)
      {
        if (strcmp(field->name, col->name) == 0)    // If strings are equal
        {
          field_exists = 1;
          fields_width.push_back(strlen(col->name) + 2);
          requested_columns_order.push_back(&col - &this->columns->at(0));
          break;
        }
      }

      if (!field_exists)
      {
        fprintf(stderr, "ERROR: Column %s not found in table %s.\n",
                field->name, this->name);
        return 0;
      }
    }
  }

  // COLLECT DATA FROM ALL REGS
  std::vector<std::vector<std::string>> regs_data;
  for (const auto& reg : fs::directory_iterator(this->regs_path))
  {
    // Load data in file to reg_data
    std::ifstream data_file(reg.path());
    std::vector<std::string> reg_data(stmt->selectList->size(), "");

    // Load piece of data
    std::string data;
    bool satisfies_where = 1;
    for (auto i = 0; i < this->columns->size(); i++)
    {
      getline(data_file, data, '\t');

      // Find index of current field in the requested order
      auto field_pos = std::find(requested_columns_order.begin(),
                                 requested_columns_order.end(), i);

      // When we reach desired column, set flag
      if (i == where_column_pos && stmt->whereClause != nullptr)
        if (!compare_where(column_type, data, stmt->whereClause))
        {
          satisfies_where = 0;
          break;
        }

      // If field was requested, add it to reg_data
      if (field_pos != requested_columns_order.end())
      {
        auto current_req_field =
            std::distance(requested_columns_order.begin(), field_pos);
        reg_data[current_req_field] = data;
        // If data witdth is larger than current field_width,
        // make it the new field_width
        if (fields_width[current_req_field] < data.size() + 2)
          fields_width[current_req_field] = data.size() + 2;
      }
    }

    if (satisfies_where) regs_data.push_back(reg_data);
  }

  pu::print_select_result(stmt->selectList, &regs_data, &fields_width);
  std::cout << "Returned " << regs_data.size() << " rows.\n";
  return 1;
}

bool Table::update_records(const hsql::UpdateStatement* stmt)
{
  // Check WHERE clause correctness
  int where_column_pos;
  hsql::ColumnType* column_type;
  if (!valid_where(stmt->where, &where_column_pos, &column_type, this))
    return 0;

  // Check UPDATE SET column exists
  bool column_exists = 0;
  int update_column_pos;
  hsql::ColumnDefinition* update_column;
  for (const auto& col : *this->columns)
    if (strcmp(col->name, stmt->updates->at(0)->column) == 0)
    {
      column_exists = 1;
      update_column = col;
      update_column_pos = &col - &this->columns->at(0);
    }

  if (!column_exists)
  {
    fprintf(stderr, "ERROR: Column %s doesn't exist in table %s.\n",
            stmt->updates->at(0)->column, this->name);
    return 0;
  }

  // Check assign value is correct
  if (((update_column->type.data_type == hsql::DataType::CHAR ||
        update_column->type.data_type == hsql::DataType::DATE) &&
       stmt->updates->at(0)->value->type != hsql::kExprLiteralString) ||
      (update_column->type.data_type == hsql::DataType::INT &&
       stmt->updates->at(0)->value->type != hsql::kExprLiteralInt))
  {
    fprintf(stderr, "ERROR: Update column and value's types don't match.\n");
    return 0;
  }

  std::vector<std::string> regs_to_update_path;
  std::vector<std::vector<std::string>> regs_data;
  // LOAD ALL REGS
  for (const auto& reg : fs::directory_iterator(this->regs_path))
  {
    // Load data in file to reg_data
    std::ifstream data_file(reg.path());
    std::vector<std::string> reg_data;

    // Load each piece of data into reg_data
    std::string data;
    for (auto i = 0; i < this->columns->size(); i++)
    {
      getline(data_file, data, '\t');
      reg_data.push_back(data);
    }

    // WHERE CLAUSE
    if (compare_where(column_type, reg_data[where_column_pos], stmt->where))
    {
      regs_to_update_path.push_back(reg.path());
      if (stmt->updates->at(0)->value->type == hsql::kExprLiteralString)
        reg_data[update_column_pos] = stmt->updates->at(0)->value->name;
      else
        reg_data[update_column_pos] =
            std::to_string(stmt->updates->at(0)->value->ival);

      regs_data.push_back(reg_data);
    }
  }

  for (const auto& path : regs_to_update_path)
    remove(path.c_str());

  int x = 0;
  // Reinsert regs with updated values
  for (const auto& reg : regs_data)
  {
    // Create filename
    char* reg_file = ft::getRegPath(this->regs_path, x++);

    std::ofstream updated_reg(reg_file);
    for (const auto& data : reg)
      updated_reg << data << "\t";
    updated_reg.close();
  }

  std::cout << "Updated " << regs_to_update_path.size() << " rows.\n";
  return 1;
}
// TODO: ONLY ALLOW 1 COLUMN TO BE AFFECTED AT THE TIME BY UPDATE

bool Table::delete_records(const hsql::DeleteStatement* stmt)
{
  // Check WHERE clause correctness
  int where_column_pos;
  hsql::ColumnType* column_type;
  if (!valid_where(stmt->expr, &where_column_pos, &column_type, this))
  {
    return 0;
  }

  std::vector<std::string> regs_to_delete_path;
  // LOAD ALL REGS
  for (const auto& reg : fs::directory_iterator(this->regs_path))
  {
    // Load data in file to reg_data
    std::ifstream data_file(reg.path());
    std::vector<std::string> reg_data;

    // Load each piece of data into reg_data
    // and change max field_width if necessary
    std::string data;
    for (auto i = 0; i < this->columns->size(); i++)
    {
      getline(data_file, data, '\t');
      reg_data.push_back(data);
    }

    // WHERE CLAUSE
    if (compare_where(column_type, reg_data[where_column_pos], stmt->expr))
      regs_to_delete_path.push_back(reg.path());
  }

  for (const auto& path : regs_to_delete_path)
    remove(path.c_str());

  std::cout << "Deleted " << regs_to_delete_path.size() << " rows.\n";
  return 1;
}
