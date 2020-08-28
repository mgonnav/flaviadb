#include "DBException.hh"
#include "Processor.hh"
#include "Table.hh"
#include "filestruct.hh"
#include "flaviadb_definitions.hh"
#include "printutils.hh"
#include <filesystem>
#include <fstream>                  // ifstream, ofstream
#include <hsql/SQLParser.h>         // Include SQL Parser
#include <hsql/util/sqlhelper.h>    // Contains printing utilities
#include <iostream>
#include <sys/stat.h>    // stat, mkdir

namespace fs = std::filesystem;
namespace ft = ftools;
namespace pu = printUtils;

std::map<std::string, std::unique_ptr<Table>> tables;

int main()
{
  if (!ft::dirExists(FLAVIADB_DIR))
  {
    fprintf(stderr, "ERROR: Couldn't find .flaviadb/\n");
    exit(0);
  }
  if (!ft::dirExists(FLAVIADB_TEST_DB))
  {
    fprintf(stderr, "ERROR: coudn't find .flaviadb/test/\n");
    exit(0);
  }

  std::string filename;
  std::cout << "filename: ";
  std::cin >> filename;

  std::ifstream inFile(filename + ".fdb");
  std::string query;
  while (std::getline(inFile, query))
  {
    hsql::SQLParserResult* result = new hsql::SQLParserResult;
    hsql::SQLParser::parse(query, result);

    if (result->isValid() && result->size())
    {
      for (size_t i = 0; i < result->size(); i++)
      {
        const hsql::SQLStatement* statement = result->getStatement(i);

        switch (statement->type())
        {
        case hsql::kStmtSelect:
        {
          auto select_stmt = (hsql::SelectStatement*)statement;

          if (select_stmt->fromTable != nullptr)
          {
            try
            {
              auto table_name{select_stmt->fromTable->name};
              auto table_it = tables.find(table_name);
              if (table_it == tables.end())
              {
                auto [pair, inserted] = tables.insert(
                    {table_name, std::make_unique<Table>(table_name)});
                table_it = pair;
              }
              Processor::show_records(select_stmt, table_it->second);
            }
            catch (const DBException& e)
            {
              std::cout << e.what() << "\n";
            }
          }
          else
            fprintf(stderr, "ERROR: No source table was specified.\n");

          break;
        }
        case hsql::kStmtInsert:
        {
          auto insert_stmt = (hsql::InsertStatement*)statement;

          try
          {
            auto table_it = tables.find(insert_stmt->tableName);
            if (table_it == tables.end())
            {
              auto [pair, inserted] = tables.insert(
                  {insert_stmt->tableName,
                   std::make_unique<Table>(insert_stmt->tableName)});
              table_it = pair;
            }
            Processor::insert_record(insert_stmt, table_it->second);
          }
          catch (const DBException& e)
          {
            std::cout << e.what() << "\n";
          }

          break;
        }
        case hsql::kStmtUpdate:
        {
          auto update_stmt = (hsql::UpdateStatement*)statement;

          try
          {
            auto table_it = tables.find(update_stmt->table->name);
            if (table_it == tables.end())
            {
              auto [pair, inserted] = tables.insert(
                  {update_stmt->table->name,
                   std::make_unique<Table>(update_stmt->table->name)});
              table_it = pair;
            }
            Processor::update_records(update_stmt, table_it->second);
          }
          catch (const DBException& e)
          {
            std::cout << e.what() << "\n";
          }

          break;
        }
        case hsql::kStmtDelete:
        {
          auto delete_stmt = (hsql::DeleteStatement*)statement;

          try
          {
            auto table_it = tables.find(delete_stmt->tableName);
            if (table_it == tables.end())
            {
              auto [pair, inserted] = tables.insert(
                  {delete_stmt->tableName,
                   std::make_unique<Table>(delete_stmt->tableName)});
              table_it = pair;
            }
            Processor::delete_records(delete_stmt, table_it->second);
          }
          catch (const DBException& e)
          {
            std::cout << e.what() << "\n";
          }

          break;
        }
        case hsql::kStmtCreate:
        {
          auto create_stmt = (hsql::CreateStatement*)statement;
          std::string table_path = ft::getTablePath(create_stmt->tableName);

          if (create_stmt->type == hsql::kCreateTable)
          {
            if (!ft::dirExists(table_path))
            {
              tables.insert({create_stmt->tableName,
                             std::make_unique<Table>(create_stmt->tableName,
                                                     create_stmt->columns)});
            }
            else
              fprintf(stderr, "Table named %s already exists!\n",
                      create_stmt->tableName);
          }
          else if (create_stmt->type == hsql::kCreateIndex)
          {
            try
            {
              auto table_it = tables.find(create_stmt->tableName);
              if (table_it == tables.end())
              {
                auto [pair, inserted] = tables.insert(
                    {create_stmt->tableName,
                     std::make_unique<Table>(create_stmt->tableName)});
                table_it = pair;
              }
              Processor::create_index(create_stmt->columns->at(0)->name,
                                      table_it->second);
            }
            catch (const DBException& e)
            {
              std::cout << e.what() << "\n";
            }
          }

          break;
        }
        case hsql::kStmtDrop:
        {
          auto drop_stmt = (hsql::DropStatement*)statement;

          try
          {
            auto table_it = tables.find(drop_stmt->name);
            if (table_it == tables.end())
            {
              auto [pair, inserted] = tables.insert(
                  {drop_stmt->name, std::make_unique<Table>(drop_stmt->name)});
              table_it = pair;
            }
            Processor::drop_table(table_it->second);
            tables.erase(table_it);
          }
          catch (const DBException& e)
          {
            std::cout << e.what() << "\n";
          }

          break;
        }
        case hsql::kStmtShow:    // DESCRIBE
        {
          auto show_stmt = (hsql::ShowStatement*)statement;

          if (show_stmt->type == hsql::ShowType::kShowTables)
          {
            std::vector<std::string> tables;
            for (const auto& table : fs::directory_iterator(FLAVIADB_TEST_DB))
              tables.push_back(table.path().filename());
            pu::print_tables_list(tables);
          }
          else
          {
            try
            {
              auto table_it = tables.find(show_stmt->name);
              if (table_it == tables.end())
              {
                auto [pair, inserted] =
                    tables.insert({show_stmt->name,
                                   std::make_unique<Table>(show_stmt->name)});
                table_it = pair;
              }
              pu::print_table_desc(table_it->second);
            }
            catch (const DBException& e)
            {
              std::cout << e.what() << "\n";
            }
          }

          break;
        }

        default:
          fprintf(stderr, "Query implementation missing!\n");
        }
      }
    }
    else
    {
      fprintf(stderr, "Given string is not a valid SQL query.\n");
      fprintf(stderr, "%s\n", result->errorMsg());
    }
  }

  return 0;
}
