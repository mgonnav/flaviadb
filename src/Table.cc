#include "printutils.hh"
#include "where.hh"

namespace ft = ftools;
namespace fs = std::filesystem;
namespace pu = printUtils;

Table::Table(std::string name)
{
  this->name = name;
  loadPaths();
  checkTableExists();
  load_metadata();
  this->registers = new std::map<std::string, Register*>();

  this->reg_count = ft::getRegCount(this->name);

  this->indexes = new std::vector<Index*>;
  loadIndexes();
  loadStoredRegisters();
}

void Table::loadPaths()
{
  this->path = ft::getTablePath(this->name);
  this->regs_path = ft::getRegistersPath(this->name);
  this->indexes_path = ft::getIndexesPath(this->name);
  this->metadata_path = ft::getMetadataPath(this->name);
}

void Table::checkTableExists()
{
  if (!ft::dirExists(this->path))
    throw DBException{NON_EXISTING_TABLE, this->name};
}

void Table::loadIndexes()
{
  for (const auto& reg : fs::directory_iterator(this->indexes_path))
  {
    std::string idx_name = reg.path().filename();
    indexes->push_back(new Index(idx_name));
  }
}

void Table::loadStoredRegisters()
{
  for (const auto& reg : fs::directory_iterator(this->regs_path))
  {
    // Load data in file to reg_data
    std::ifstream data_file(reg.path());
    std::vector<std::string> reg_data;

    // Load each piece of data into reg_data
    std::string data;
    for (size_t i = 0; i < this->columns->size(); i++)
    {
      getline(data_file, data, '\t');
      reg_data.push_back(data);
    }

    this->registers->insert(std::make_pair<std::string, Register*>(
        reg.path().filename().string(), new Register(reg_data)));
  }
}

Table::Table(std::string name, std::vector<hsql::ColumnDefinition*>* cols)
{
  this->name = name;
  loadPaths();
  this->indexes = new std::vector<Index*>;

  createTableFolders();

  this->columns = cols;
  this->reg_size = calculateRegSize();
  this->registers = new std::map<std::string, Register*>();

  // Create medata.dat file for table
  // and fill it with table's name & cols info
  std::ofstream wMetadata(this->metadata_path);
  wMetadata << this->name << "\t" << this->columns->size() << "\t"
            << this->reg_size << "\n";
  for (const hsql::ColumnDefinition* col : *this->columns)
    wMetadata << col->name << "\t" << (int)col->type.data_type << "\t"
              << col->type.length << "\t" << col->nullable << "\n";
  wMetadata.close();

  std::ofstream wCount(ft::getRegCountPath(this->name));
  wCount << 0;    // 0 regs when table is created
  wCount.close();

  std::cout << "Table " << this->name << " was created successfully.\n";
}

void Table::createTableFolders()
{
  ft::createFolder(this->path);
  ft::createFolder(this->regs_path);
  ft::createFolder(this->indexes_path);
}

int Table::calculateRegSize()
{
  int reg_size = 0;
  for (const auto& col : *this->columns)
  {
    hsql::DataType data_type = col->type.data_type;
    if (data_type == hsql::DataType::INT)
      reg_size += 4;
    else if (data_type == hsql::DataType::CHAR)
      reg_size += col->type.length;
    else if (data_type == hsql::DataType::DATE)
      reg_size += 8;
  }

  return reg_size;
}

Table::~Table()
{
  delete this->registers;
  delete this->columns;
  delete this->indexes;
}

bool Table::load_metadata()
{
  openMetadataFile();
  loadMetadataHeader();
  for (auto& column : *this->columns)
    loadColumnData(column);

  return 1;
}

void Table::openMetadataFile()
{
  this->metadata_file.open(this->metadata_path);
  if (!this->metadata_file.is_open())
    throw DBException{UNREADABLE_METADATA};
}

void Table::loadMetadataHeader()
{
  loadTableName();
  loadColumnCount();
  loadRegSize();
}

void Table::loadTableName()
{
  std::string metadata_table_name;
  getline(this->metadata_file, metadata_table_name, '\t');

  if (this->name != metadata_table_name)
    throw DBException{DIFFERENT_METADATA_NAME};
}

void Table::loadColumnCount()
{
  std::string column_count;
  getline(this->metadata_file, column_count, '\t');
  this->columns = new std::vector<hsql::ColumnDefinition*>(stoi(column_count));
}

void Table::loadRegSize()
{
  std::string reg_size;
  getline(this->metadata_file, reg_size, '\n');
  this->reg_size = stoi(reg_size);
}

void Table::loadColumnData(hsql::ColumnDefinition*& column)
{
  std::string data;

  getline(this->metadata_file, data, '\t');
  char* col_name = new char[data.size()];
  strcpy(col_name, data.c_str());

  hsql::ColumnType col_type = getColumnType();

  getline(this->metadata_file, data, '\n');
  bool col_nullable = stoi(data);

  column = new hsql::ColumnDefinition(col_name, col_type, col_nullable);
}

hsql::ColumnType Table::getColumnType()
{
  std::string data;

  getline(this->metadata_file, data, '\t');
  int col_enum = stoi(data);

  getline(this->metadata_file, data, '\t');
  int col_length = stoi(data);

  hsql::ColumnType col_type((hsql::DataType)col_enum, col_length);
  return col_type;
}

bool Table::insert_record(const hsql::InsertStatement* stmt)
{
  std::string reg_file;
  if (stmt->columns != nullptr)
  {
    return 1;    // TODO: Insert on specific columns
  }
  else
  {
    if (stmt->values->size() < this->columns->size())
      throw DBException{MISSING_VALUES};
    if (stmt->values->size() > this->columns->size())
      throw DBException{TOO_MANY_VALUES};

    // Create filename
    reg_file = ft::getNewRegPath(this->name);

    std::ofstream new_reg(reg_file);
    std::vector<std::string> new_reg_data;
    for (size_t i = 0; i < stmt->values->size(); i++)
    {
      hsql::Expr* value = stmt->values->at(i);
      hsql::ColumnDefinition* column = this->columns->at(i);
      if (value->type == hsql::kExprLiteralString)
      {
        if (column->type.data_type == hsql::DataType::CHAR)
        {
          if (strlen(value->name) <= column->type.length)
          {
            new_reg_data.push_back(value->name);
            new_reg << value->name;
          }
          else
          {
            new_reg.close();
            remove(reg_file.c_str());
            throw DBException{CHAR_TOO_BIG, this->name, column->name};
          }
        }
        else if (column->type.data_type == hsql::DataType::DATE)
        {
          if (strlen(value->name) > 10)
          {
            new_reg.close();
            remove(reg_file.c_str());
            throw DBException{INVALID_DATE, this->name, value->name};
          }

          struct tm tm = {0};
          if (strptime(value->name, DATE_FORMAT, &tm))
          {
            new_reg_data.push_back(value->name);
            new_reg << value->name;
          }
          else
          {
            new_reg.close();
            remove(reg_file.c_str());
            throw DBException(INVALID_DATE, this->name, value->name);
          }
        }
        else
        {
          new_reg.close();
          remove(reg_file.c_str());
          throw DBException{INVALID_DATA_TYPE, this->name, column->name};
        }
      }
      else if (value->type == hsql::kExprLiteralInt &&
               column->type.data_type == hsql::DataType::INT)
      {
        new_reg_data.push_back(std::to_string(value->ival));
        new_reg << value->ival;
      }
      else
      {
        new_reg.close();
        remove(reg_file.c_str());
        throw DBException{INVALID_DATA_TYPE, this->name, column->name};
      }
      new_reg << "\t";
    }
    this->reg_count++;
    this->registers->insert(std::make_pair<std::string, Register*>(
        std::to_string(this->reg_count) + ".sqlito",
        new Register(new_reg_data)));
    new_reg.close();
  }

  std::ifstream file_to_index(reg_file);
  std::vector<std::string> reg_data();

  // Load piece of data
  std::string data;
  for (const auto& col : *this->columns)
  {
    getline(file_to_index, data, '\t');

    if (col->type.data_type == hsql::DataType::INT)
    {
      for (const auto& index : *this->indexes)
      {
        if (index->name == std::string(col->name))
        {
          std::string idx_folder =
              this->indexes_path + index->name + "/" + data + "/";

          mkdir(idx_folder.c_str(), S_IRWXU);

          std::ofstream indexed_file(
              idx_folder + std::to_string(this->reg_count) + ".sqlito");
          indexed_file.close();
        }
      }
    }
  }

  std::cout << "Inserted 1 row.\n";
  return 1;
}

bool Table::show_records(const hsql::SelectStatement* stmt) const
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
    for (size_t i = 0; i < this->columns->size(); i++)
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
        throw DBException(STAR_NOT_ALONE);

      // If field is a duplicate, error
      tmp.insert(std::string(field->name));
      if (tmp.size() != (&field - &stmt->selectList->at(0)) + 1)
        throw DBException(REPEATED_FIELDS);

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
        throw DBException{COLUMN_NOT_IN_TABLE, this->name, field->name};
    }
  }

  if (stmt->whereClause != nullptr && this->indexes->size() > 0)
  {
    for (const auto& index : *this->indexes)
    {
      if (index->name == std::string(stmt->whereClause->expr->name))
      {
        std::string indexed_data =
            this->indexes_path + std::string(index->name) + "/" +
            std::to_string(stmt->whereClause->expr2->ival);
        if (ft::dirExists(indexed_data))
        {
          // COLLECT DATA FROM ALL REGS
          std::vector<std::vector<std::string>> regs_data;
          for (const auto& reg : fs::directory_iterator(indexed_data))
          {
            // Load data in file to reg_data
            std::ifstream data_file(this->regs_path +
                                    reg.path().filename().string());
            std::vector<std::string> reg_data(stmt->selectList->size(), "");

            // Load piece of data
            std::string data;
            for (size_t i = 0; i < this->columns->size(); i++)
            {
              getline(data_file, data, '\t');

              // Find index of current field in the requested order
              auto field_pos = std::find(requested_columns_order.begin(),
                                         requested_columns_order.end(), i);

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

            regs_data.push_back(reg_data);
          }

          pu::print_select_result(stmt->selectList, &regs_data, &fields_width);
          std::cout << "Returned " << regs_data.size()
                    << " rows using indexed search.\n";
          return 1;
        }
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
    for (size_t i = 0; i < this->columns->size(); i++)
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

    if (satisfies_where)
      regs_data.push_back(reg_data);
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
    throw DBException{COLUMN_NOT_IN_TABLE, this->name, stmt->updates->at(0)->column};

  // Check assign value is correct
  if (((update_column->type.data_type == hsql::DataType::CHAR ||
        update_column->type.data_type == hsql::DataType::DATE) &&
       stmt->updates->at(0)->value->type != hsql::kExprLiteralString) ||
      (update_column->type.data_type == hsql::DataType::INT &&
       stmt->updates->at(0)->value->type != hsql::kExprLiteralInt))
    throw DBException{INVALID_DATA_TYPE, this->name, stmt->updates->at(0)->column};

  Index* updated_index = nullptr;
  for (const auto& index : *this->indexes)
    if (index->name == std::string(update_column->name))
      updated_index = index;

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
    for (size_t i = 0; i < this->columns->size(); i++)
    {
      getline(data_file, data, '\t');
      reg_data.push_back(data);
    }

    // WHERE CLAUSE
    if (compare_where(column_type, reg_data[where_column_pos], stmt->where))
    {
      auto reg_to_update = this->registers->at(reg.path().filename());

      regs_to_update_path.push_back(reg.path());
      std::string old_value = reg_data[update_column_pos];
      if (stmt->updates->at(0)->value->type == hsql::kExprLiteralString)
      {
        reg_data[update_column_pos] =
            reg_to_update->data.at(update_column_pos) =
                stmt->updates->at(0)->value->name;
      }
      else
      {
        reg_data[update_column_pos] =
            reg_to_update->data.at(update_column_pos) =
                std::to_string(stmt->updates->at(0)->value->ival);
      }

      regs_data.push_back(reg_data);

      if (updated_index != nullptr)
      {
        // Remove old index
        std::string idx_folder =
            this->indexes_path + updated_index->name + "/" + old_value + "/";
        std::string idx_path = idx_folder + reg.path().filename().string();
        remove(idx_path.c_str());
        if (fs::is_empty(fs::path(idx_folder)))
          remove(idx_folder.c_str());
        std::cout << "removed " << idx_path << "\n";

        // Add new index
        std::string new_idx_folder = this->indexes_path + updated_index->name +
                                     "/" + reg_data[update_column_pos] + "/";
        mkdir(new_idx_folder.c_str(), S_IRWXU);
        std::cout << "made folder " << new_idx_folder << "\n";

        std::ofstream new_idx(new_idx_folder + reg.path().filename().string());
        new_idx.close();
        std::cout << "made reg " << new_idx_folder
                  << reg.path().filename().string() << "\n";
      }
    }
  }

  for (const auto& path : regs_to_update_path)
    remove(path.c_str());

  // Reinsert regs with updated values
  for (const auto& reg : regs_data)
  {
    // Create filename
    std::string reg_file = regs_to_update_path[&reg - &regs_data[0]];

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
    std::string data;
    for (size_t i = 0; i < this->columns->size(); i++)
    {
      getline(data_file, data, '\t');
      reg_data.push_back(data);
    }

    // WHERE CLAUSE
    if (compare_where(column_type, reg_data[where_column_pos], stmt->expr))
    {
      regs_to_delete_path.push_back(reg.path());
      this->registers->erase(reg.path().filename().string());
      for (const auto& index : *this->indexes)
      {
        for (size_t i = 0; i < this->columns->size(); i++)
        {
          const auto col = this->columns->at(i);
          if (index->name == std::string(col->name))
          {
            std::string indexed_reg_folder =
                this->indexes_path + index->name + "/" + reg_data[i] + "/";

            std::string indexed_reg_path =
                indexed_reg_folder + reg.path().filename().string();

            remove(indexed_reg_path.c_str());
            if (fs::is_empty(fs::path(indexed_reg_folder)))
              remove(indexed_reg_folder.c_str());
          }
        }
      }
    }
  }

  for (const auto& path : regs_to_delete_path)
    remove(path.c_str());

  std::cout << "Deleted " << regs_to_delete_path.size() << " rows.\n";
  return 1;
}

bool Table::drop_table()
{
  std::error_code errorCode;
  if (!fs::remove_all(this->path, errorCode))
  {
    std::cout << errorCode.message() << "\n";
    return 0;
  }

  std::cout << "Dropped table " << this->name << ".\n";
  return 1;
}

bool Table::create_index(std::string column)
{
  // Check that the column actually exists in the table
  int column_pos;
  bool field_exists = 0;
  for (auto& col : *this->columns)
  {
    if (strcmp(column.c_str(), col->name) == 0)    // If strings are equal
    {
      field_exists = 1;
      column_pos = &col - &this->columns->at(0);
      break;
    }
  }

  if (!field_exists)
    throw DBException{COLUMN_NOT_IN_TABLE, this->name, column};

  for (const auto index : *this->indexes)
  {
    if (index->name == column)
      throw DBException{INDEX_ALREADY_EXISTS, this->name, column};
  }

  if (this->columns->at(column_pos)->type.data_type != hsql::DataType::INT)
    throw DBException{INDEX_NOT_INT};

  // TODO: add support for other datatypes
  // Create tree
  std::map<int, std::vector<std::string>> index_tree;

  // LOAD ALL REGS
  for (const auto& reg : fs::directory_iterator(this->regs_path))
  {
    // Load data in file to reg_data
    std::ifstream data_file(reg.path());

    // Load each piece of data into reg_data
    std::string data;
    for (int i = 0; i < column_pos + 1; i++)
      getline(data_file, data, '\t');

    // If the key is already in the map, then add current reg's path to the
    // vector
    // Else, insert the key in the map and add current reg's path to the
    // vector
    int val = stoi(data);
    auto elem = index_tree.find(val);
    if (elem != index_tree.end())
      elem->second.push_back(reg.path().filename().string());
    else
      index_tree.insert(std::pair<int, std::vector<std::string>>(
          val, std::vector<std::string>(1, reg.path().filename().string())));
  }

  std::string idx_path = this->indexes_path + column + "/";
  ft::createFolder(idx_path.c_str());

  for (const auto& idx : index_tree)
  {
    std::string idx_val_path = idx_path + std::to_string(idx.first) + "/";
    ft::createFolder(idx_val_path.c_str());

    for (const auto& filename : idx.second)
    {
      std::ofstream file(idx_val_path + filename);
      file.close();
    }
  }

  std::cout << "Index " << column << " was created successfully on table "
            << this->name << ".\n";

  return 0;
}
