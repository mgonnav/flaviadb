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
#include <map>
#include <readline/history.h>
#include <readline/readline.h>
#include <sys/stat.h>    // stat, mkdir

namespace fs = std::filesystem;
namespace ft = ftools;
namespace pu = printUtils;

std::map<std::string, std::unique_ptr<Table>> tables;

int main()
{
  if (!ft::dirExists(FLAVIADB_DIR))
    ft::createFolder(FLAVIADB_DIR);
  if (!ft::dirExists(FLAVIADB_TEST_DB))
    ft::createFolder(FLAVIADB_TEST_DB);

  pu::print_welcome_message();

  std::string query_str;
  while (1)
  {
    // If we are writing a new query (already finished last query), prompt
    // 'FlaviaDB> '
    // Else we are writing a multiline query. In that case, we
    // continue the prompt using '-> '
    char* query =
        (query_str.empty()) ? readline("FlaviaDB> ") : readline("\t-> ");

    if (!query)
      break;

    query_str += query;
    if (*query && query_str.back() == ';')
    {
      hsql::SQLParserResult* result = new hsql::SQLParserResult;
      hsql::SQLParser::parse(query_str, result);

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
                auto it = tables.find(table_name);
                if (it == tables.end())
                {
                  auto [it, inserted] = tables.insert(
                      {table_name, std::make_unique<Table>(table_name)});
                }
                Processor::show_records(select_stmt, it->second);
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
              auto it = tables.find(insert_stmt->tableName);
              if (it == tables.end())
              {
                auto [it, inserted] = tables.insert(
                    {insert_stmt->tableName,
                     std::make_unique<Table>(insert_stmt->tableName)});
              }
              Processor::insert_record(insert_stmt, it->second);
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
              auto it = tables.find(update_stmt->table->name);
              if (it == tables.end())
              {
                auto [it, inserted] = tables.insert(
                    {update_stmt->table->name,
                     std::make_unique<Table>(update_stmt->table->name)});
              }
              Processor::update_records(update_stmt, it->second);
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
              auto it = tables.find(delete_stmt->tableName);
              if (it == tables.end())
              {
                auto [it, inserted] = tables.insert(
                    {delete_stmt->tableName,
                     std::make_unique<Table>(delete_stmt->tableName)});
              }
              Processor::delete_records(delete_stmt, it->second);
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
                auto it = tables.find(create_stmt->tableName);
                if (it == tables.end())
                {
                  auto [it, inserted] = tables.insert(
                      {create_stmt->tableName,
                       std::make_unique<Table>(create_stmt->tableName)});
                }
                Processor::create_index(create_stmt->columns->at(0)->name,
                                        it->second);
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
              auto it = tables.find(drop_stmt->name);
              if (it == tables.end())
              {
                auto [it, inserted] =
                    tables.insert({drop_stmt->name,
                                   std::make_unique<Table>(drop_stmt->name)});
              }
              Processor::drop_table(it->second);
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
                auto stored_tbl = std::make_unique<Table>(show_stmt->name);
                pu::print_table_desc(std::move(stored_tbl));
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

      add_history(query_str.c_str());
      query_str.clear();
    }
    else
      query_str += "\n";

    free(query);
  }

  return 0;
}
