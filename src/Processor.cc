#include "Processor.hh"

namespace fs = std::filesystem;
namespace ft = ftools;
namespace pu = printUtils;

bool Processor::insert_record(const hsql::InsertStatement* stmt,
                              std::unique_ptr<Table> const& table)
{
  if (table->registers == nullptr)
    table->loadStoredRegisters();

  fs::path reg_file;
  std::string filename;
  RegisterData inserted_reg{};

  if (stmt->columns != nullptr)
  {
    return 1;    // TODO: Insert on specific columns
  }
  else
  {
    if (stmt->values->size() < table->columns->size())
      throw DBException{MISSING_VALUES};
    if (stmt->values->size() > table->columns->size())
      throw DBException{TOO_MANY_VALUES};

    // Create filename
    reg_file = ft::getNewRegPath(table->name);
    filename = reg_file.filename();

    std::ofstream new_reg(reg_file);
    std::vector<std::string> new_reg_data;
    for (size_t i = 0; i < stmt->values->size(); i++)
    {
      hsql::Expr* value = stmt->values->at(i);
      hsql::ColumnDefinition* column = table->columns->at(i);
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
            throw DBException{CHAR_TOO_BIG, table->name, column->name};
          }
        }
        else if (column->type.data_type == hsql::DataType::DATE)
        {
          if (strlen(value->name) > 10)
          {
            new_reg.close();
            remove(reg_file.c_str());
            throw DBException{INVALID_DATE, table->name, value->name};
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
            throw DBException(INVALID_DATE, table->name, value->name);
          }
        }
        else
        {
          new_reg.close();
          remove(reg_file.c_str());
          throw DBException{INVALID_DATA_TYPE, table->name, column->name};
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
        throw DBException{INVALID_DATA_TYPE, table->name, column->name};
      }
      new_reg << "\t";
    }
    table->reg_count++;
    table->registers->push_back({std::to_string(table->reg_count) + ".sqlito",
                                 RegisterData(new_reg_data)});
    inserted_reg = table->registers->back().second;
    new_reg.close();
  }

  // Index new register
  for (size_t i = 0; i < table->columns->size(); i++)
  {
    auto col = table->columns->at(i);

    if (col->type.data_type == hsql::DataType::INT)
    {
      for (const auto& index : *table->indexes)
      {
        if (index->name == std::string(col->name))
        {
          std::string idx_folder = table->indexes_path + index->name + "/" +
                                   inserted_reg.at(i) + "/";

          mkdir(idx_folder.c_str(), S_IRWXU);

          std::ofstream indexed_file(idx_folder + filename);
          indexed_file.close();
        }
      }
    }
  }

  std::cout << "Inserted 1 row.\n";
  return 1;
}

bool Processor::show_records(const hsql::SelectStatement* stmt,
                             std::unique_ptr<Table> const& table)
{
  if (table->registers == nullptr)
    table->loadStoredRegisters();

  // Check WHERE clause correctness
  int where_column_pos;
  hsql::DataType column_data_type;
  if (!valid_where(stmt->whereClause, &where_column_pos, &column_data_type,
                   table))
    return 0;
  auto where = Where::get(stmt->whereClause, column_data_type);

  std::set<std::string> tmp;
  std::vector<size_t> fields_width;
  std::vector<int> requested_columns_order;

  if (stmt->selectList->size() == 1 &&
      stmt->selectList->at(0)->type == hsql::kExprStar)
  {
    delete stmt->selectList->back();
    stmt->selectList->pop_back();
    for (size_t i = 0; i < table->columns->size(); i++)
    {
      stmt->selectList->push_back(new hsql::Expr(hsql::kExprColumnRef));
      stmt->selectList->at(i)->name = table->columns->at(i)->name;
      fields_width.push_back(strlen(table->columns->at(i)->name) + 2);
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
      for (auto& col : *table->columns)
      {
        if (strcmp(field->name, col->name) == 0)    // If strings are equal
        {
          field_exists = 1;
          fields_width.push_back(strlen(col->name) + 2);
          requested_columns_order.push_back(&col - &table->columns->at(0));
          break;
        }
      }

      if (!field_exists)
        throw DBException{COLUMN_NOT_IN_TABLE, table->name, field->name};
    }
  }

  if (stmt->whereClause != nullptr && table->indexes->size() > 0)
  {
    for (const auto& index : *table->indexes)
    {
      if (index->name == std::string(stmt->whereClause->expr->name))
      {
        std::string indexed_data =
            table->indexes_path + std::string(index->name) + "/" +
            std::to_string(stmt->whereClause->expr2->ival);
        if (ft::dirExists(indexed_data))
        {
          // COLLECT DATA FROM ALL REGS
          std::vector<std::vector<std::string>> regs_data;
          for (const auto& reg : fs::directory_iterator(indexed_data))
          {
            // Load data in file to reg_data
            std::ifstream data_file(table->regs_path +
                                    reg.path().filename().string());
            std::vector<std::string> reg_data(stmt->selectList->size(), "");

            // Load piece of data
            std::string data;
            for (size_t i = 0; i < table->columns->size(); i++)
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
  for (const auto& [filename, reg_data] : *table->registers)
  {
    std::vector<std::string> requested_data(stmt->selectList->size(), "");
    bool satisfies_where = 1;
    for (size_t i = 0; i < table->columns->size(); i++)
    {
      // Find index of current field in the requested order
      auto field_pos = std::find(requested_columns_order.begin(),
                                 requested_columns_order.end(), i);

      std::string data = reg_data.at(i);
      // When we reach desired column, set flag
      if (i == where_column_pos && stmt->whereClause != nullptr)
        if (!where->compare(data))
        {
          satisfies_where = 0;
          break;
        }

      // If field was requested, add it to reg_data
      if (field_pos != requested_columns_order.end())
      {
        auto current_req_field =
            std::distance(requested_columns_order.begin(), field_pos);
        requested_data[current_req_field] = data;
        // If data witdth is larger than current field_width,
        // make it the new field_width
        if (fields_width[current_req_field] < data.size() + 2)
          fields_width[current_req_field] = data.size() + 2;
      }
    }

    if (satisfies_where)
      regs_data.push_back(requested_data);
  }

  pu::print_select_result(stmt->selectList, &regs_data, &fields_width);
  std::cout << "Returned " << regs_data.size() << " rows.\n";
  return 1;
}

bool Processor::update_records(const hsql::UpdateStatement* stmt,
                               std::unique_ptr<Table> const& table)
{
  if (table->registers == nullptr)
    table->loadStoredRegisters();

  // Check WHERE clause correctness
  int where_column_pos;
  hsql::DataType column_data_type;
  if (!valid_where(stmt->where, &where_column_pos, &column_data_type, table))
    return 0;
  auto where = Where::get(stmt->where, column_data_type);

  // Check UPDATE SET column exists
  bool column_exists = 0;
  int update_column_pos;
  hsql::ColumnDefinition* update_column;
  for (const auto& col : *table->columns)
    if (strcmp(col->name, stmt->updates->at(0)->column) == 0)
    {
      column_exists = 1;
      update_column = col;
      update_column_pos = &col - &table->columns->at(0);
    }

  if (!column_exists)
    throw DBException{COLUMN_NOT_IN_TABLE, table->name,
                      stmt->updates->at(0)->column};

  // Check assign value is correct
  if (((update_column->type.data_type == hsql::DataType::CHAR ||
        update_column->type.data_type == hsql::DataType::DATE) &&
       stmt->updates->at(0)->value->type != hsql::kExprLiteralString) ||
      (update_column->type.data_type == hsql::DataType::INT &&
       stmt->updates->at(0)->value->type != hsql::kExprLiteralInt))
    throw DBException{INVALID_DATA_TYPE, table->name,
                      stmt->updates->at(0)->column};

  Index* updated_index = nullptr;
  for (const auto& index : *table->indexes)
    if (index->name == std::string(update_column->name))
      updated_index = index;

  std::vector<std::string> regs_to_update_filename;
  std::vector<std::vector<std::string>> regs_data;

  for (auto& [filename, reg_data] : *table->registers)
  {
    if (where->compare(reg_data.at(where_column_pos)))
    {
      regs_to_update_filename.push_back(filename);
      std::string old_value = reg_data[update_column_pos];
      if (stmt->updates->at(0)->value->type == hsql::kExprLiteralString)
        reg_data[update_column_pos] = stmt->updates->at(0)->value->name;
      else
        reg_data[update_column_pos] =
            std::to_string(stmt->updates->at(0)->value->ival);

      regs_data.push_back(reg_data);

      if (updated_index != nullptr)
      {
        // Remove old index
        std::string idx_folder =
            table->indexes_path + updated_index->name + "/" + old_value + "/";
        std::string idx_path = idx_folder + filename;
        remove(idx_path.c_str());
        if (fs::is_empty(fs::path(idx_folder)))
          remove(idx_folder.c_str());

        // Add new index
        std::string new_idx_folder = table->indexes_path + updated_index->name +
                                     "/" + reg_data[update_column_pos] + "/";
        mkdir(new_idx_folder.c_str(), S_IRWXU);

        std::ofstream new_idx(new_idx_folder + filename);
        new_idx.close();
      }
    }
  }

  for (const auto& filename : regs_to_update_filename)
    remove((table->regs_path + filename).c_str());

  // Reinsert regs with updated values
  for (const auto& reg : regs_data)
  {
    // Create filename
    std::string reg_file =
        table->regs_path + regs_to_update_filename[&reg - &regs_data[0]];

    std::ofstream updated_reg(reg_file);
    for (const auto& data : reg)
      updated_reg << data << "\t";
    updated_reg.close();
  }

  std::cout << "Updated " << regs_to_update_filename.size() << " rows.\n";
  return 1;
}
// TODO: ONLY ALLOW 1 COLUMN TO BE AFFECTED AT THE TIME BY UPDATE

bool Processor::delete_records(const hsql::DeleteStatement* stmt,
                               std::unique_ptr<Table> const& table)
{
  if (table->registers == nullptr)
    table->loadStoredRegisters();

  // Check WHERE clause correctness
  int where_column_pos;
  hsql::DataType column_data_type;
  if (!valid_where(stmt->expr, &where_column_pos, &column_data_type, table))
    return 0;
  auto where = Where::get(stmt->expr, column_data_type);

  std::vector<std::string> regs_to_delete_filename;

  for (auto it = table->registers->begin(); it != table->registers->end();)
  {
    auto filename = it->first;
    auto reg_data = it->second;
    if (where->compare(reg_data[where_column_pos]))
    {
      regs_to_delete_filename.push_back(filename);
      for (const auto& index : *table->indexes)
      {
        for (size_t i = 0; i < table->columns->size(); i++)
        {
          const auto col = table->columns->at(i);
          if (index->name == std::string(col->name))
          {
            std::string indexed_reg_folder =
                table->indexes_path + index->name + "/" + reg_data[i] + "/";

            std::string indexed_reg_path = indexed_reg_folder + filename;

            remove(indexed_reg_path.c_str());
            if (fs::is_empty(fs::path(indexed_reg_folder)))
              remove(indexed_reg_folder.c_str());
          }
        }
      }
      it = table->registers->erase(it);
    }
    else
      ++it;
  }

  for (const auto& filename : regs_to_delete_filename)
    remove((table->regs_path + filename).c_str());

  std::cout << "Deleted " << regs_to_delete_filename.size() << " rows.\n";
  return 1;
}

bool Processor::drop_table(std::unique_ptr<Table> const& table)
{
  std::error_code errorCode;
  if (!fs::remove_all(table->path, errorCode))
  {
    std::cout << errorCode.message() << "\n";
    return 0;
  }

  std::cout << "Dropped table " << table->name << ".\n";
  return 1;
}

bool Processor::create_index(std::string column,
                             std::unique_ptr<Table> const& table)
{
  if (table->registers == nullptr)
    table->loadStoredRegisters();

  // Check that the column actually exists in the table
  int column_pos;
  bool field_exists = 0;
  for (auto& col : *table->columns)
  {
    if (strcmp(column.c_str(), col->name) == 0)    // If strings are equal
    {
      field_exists = 1;
      column_pos = &col - &table->columns->at(0);
      break;
    }
  }

  if (!field_exists)
    throw DBException{COLUMN_NOT_IN_TABLE, table->name, column};

  for (const auto index : *table->indexes)
  {
    if (index->name == column)
      throw DBException{INDEX_ALREADY_EXISTS, table->name, column};
  }

  if (table->columns->at(column_pos)->type.data_type != hsql::DataType::INT)
    throw DBException{INDEX_NOT_INT};

  table->indexes->push_back(new Index(column));

  // TODO: add support for other datatypes
  // Create tree
  std::map<int, std::vector<std::string>> index_tree;

  // LOAD ALL REGS
  for (const auto& reg : fs::directory_iterator(table->regs_path))
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

  std::string idx_path = table->indexes_path + column + "/";
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
            << table->name << ".\n";

  return 0;
}
