#include "DBException.hh"
#include "Table.hh"
#include "filestruct.hh"
#include "flaviadb_definitions.hh"
#include "printutils.hh"
#include <filesystem>
#include <fstream>                  // ifstream, ofstream
#include <hsql/SQLParser.h>         // Include SQL Parser
#include <hsql/util/sqlhelper.h>    // Contains printing utilities
#include <iostream>
#include <readline/history.h>
#include <readline/readline.h>
#include <sys/stat.h>    // stat, mkdir

namespace fs = std::filesystem;
namespace ft = ftools;
namespace pu = printUtils;

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
                auto stored_tbl =
                    std::make_unique<Table>(select_stmt->fromTable->name);
                stored_tbl->show_records(select_stmt);
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
              auto stored_tbl = std::make_unique<Table>(insert_stmt->tableName);
              stored_tbl->insert_record(insert_stmt);
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
              auto stored_tbl =
                  std::make_unique<Table>(update_stmt->table->name);
              stored_tbl->update_records(update_stmt);
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
              auto stored_tbl = std::make_unique<Table>(delete_stmt->tableName);
              stored_tbl->delete_records(delete_stmt);
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
                auto table = std::make_unique<Table>(create_stmt->tableName,
                                                     create_stmt->columns);
              }
              else
                fprintf(stderr, "Table named %s already exists!\n",
                        create_stmt->tableName);
            }
            else if (create_stmt->type == hsql::kCreateIndex)
            {
              try
              {
                auto stored_tbl =
                    std::make_unique<Table>(create_stmt->tableName);
                stored_tbl->create_index(create_stmt->columns->at(0)->name);
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
              auto stored_tbl = std::make_unique<Table>(drop_stmt->name);
              stored_tbl->drop_table();
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
