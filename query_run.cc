#include <filesystem>
#include <fstream>                  // ifstream, ofstream
#include <hsql/SQLParser.h>         // Include SQL Parser
#include <hsql/util/sqlhelper.h>    // Contains printing utilities
#include <iostream>
#include <sys/stat.h>    // stat, mkdir

#include "Table.hh"
#include "filestruct.hh"
#include "flaviadb_definitions.hh"
#include "printutils.hh"

namespace fs = std::filesystem;
namespace ft = ftools;
namespace pu = printUtils;

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
      for (auto i = 0; i < result->size(); i++)
      {
        const hsql::SQLStatement* statement = result->getStatement(i);

        switch (statement->type())
        {
        case hsql::kStmtSelect:
        {
          hsql::SelectStatement* select_stmt =
              (hsql::SelectStatement*)statement;

          if (select_stmt->fromTable != nullptr)
          {
            try
            {
              Table* stored_tbl = new Table(select_stmt->fromTable->name);
              stored_tbl->show_records(select_stmt);
              delete stored_tbl;
            }
            catch (std::invalid_argument& e)
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
          hsql::InsertStatement* insert_stmt =
              (hsql::InsertStatement*)statement;

          try
          {
            Table* stored_tbl = new Table(insert_stmt->tableName);
            stored_tbl->insert_record(insert_stmt);
            delete stored_tbl;
          }
          catch (std::invalid_argument& e)
          {
            std::cout << e.what() << "\n";
          }

          break;
        }
        case hsql::kStmtUpdate:
        {
          hsql::UpdateStatement* update_stmt =
              (hsql::UpdateStatement*)statement;

          try
          {
            Table* stored_tbl = new Table(update_stmt->table->name);
            stored_tbl->update_records(update_stmt);
            delete stored_tbl;
          }
          catch (std::invalid_argument& e)
          {
            std::cout << e.what() << "\n";
          }

          break;
        }
        case hsql::kStmtDelete:
        {
          hsql::DeleteStatement* delete_stmt =
              (hsql::DeleteStatement*)statement;

          try
          {
            Table* stored_tbl = new Table(delete_stmt->tableName);
            stored_tbl->delete_records(delete_stmt);
            delete stored_tbl;
          }
          catch (std::invalid_argument& e)
          {
            std::cout << e.what() << "\n";
          }

          break;
        }
        case hsql::kStmtCreate:
        {
          hsql::CreateStatement* create_stmt =
              (hsql::CreateStatement*)statement;
          char* table_path = ft::getTablePath(create_stmt->tableName);

          if (create_stmt->type == hsql::kCreateTable)
          {
            if (!ft::dirExists(table_path))
            {
              Table* table =
                  new Table(create_stmt->tableName, create_stmt->columns);
              delete table;
            }
            else
              fprintf(stderr, "Table named %s already exists!\n",
                      create_stmt->tableName);
          }
          else if (create_stmt->type == hsql::kCreateIndex)
          {
            try
            {
              Table* stored_tbl = new Table(create_stmt->tableName);
              stored_tbl->create_index(create_stmt->columns->at(0)->name);
              delete stored_tbl;
            }
            catch (std::invalid_argument& e)
            {
              std::cout << e.what() << "\n";
            }
          }

          break;
        }
        case hsql::kStmtDrop:
        {
          hsql::DropStatement* drop_stmt = (hsql::DropStatement*)statement;

          try
          {
            Table* stored_tbl = new Table(drop_stmt->name);
            stored_tbl->drop_table();
            delete stored_tbl;
          }
          catch (std::invalid_argument& e)
          {
            std::cout << e.what() << "\n";
          }

          break;
        }
        case hsql::kStmtShow:    // DESCRIBE
        {
          hsql::ShowStatement* show_stmt = (hsql::ShowStatement*)statement;

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
              Table* stored_tbl = new Table(show_stmt->name);
              pu::print_table_desc(stored_tbl);
              delete stored_tbl;
            }
            catch (std::invalid_argument& e)
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
