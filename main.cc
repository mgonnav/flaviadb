#include <iostream>
#include <readline/readline.h>
#include <readline/history.h>
#include <hsql/SQLParser.h>       // Include SQL Parser
#include <hsql/util/sqlhelper.h>  // Contains printing utilities
#include <fstream>                // ifstream, ofstream
#include <sys/stat.h>             // stat, mkdir

#include "Table.hh"
#include "filestruct.hh"
#include "flaviadb_paths.hh"

int main()
{
  if ( dirExists(FLAVIADB_DIR) )
    if ( mkdir(FLAVIADB_DIR, S_IRWXU) == 0)
    {
      fprintf(stderr, "ERROR: Couldn't create .flaviadb/\n");
      exit(0);
    }
  if ( !dirExists(FLAVIADB_TEST_DB) )
    if ( mkdir(FLAVIADB_TEST_DB, S_IRWXU) != 0)
    {
      fprintf(stderr, "ERROR: coudn't create .flaviadb/test/\n");
      exit(0);
    }

  std::string query_str;
  while(1)
  {
    // If we are writing a new query (already finished last query), prompt 'flaviadb> '
    // Else, we are write a multiline query. In that case, we continue the prompt
    // using '-> '
    char* query = ( query_str.empty() ) ? readline("flaviadb> ") : readline("\t-> ");
    if ( !query ) break;

    query_str += query;
    if ( *query && query_str.back() == ';' )
    {
      hsql::SQLParserResult* result = new hsql::SQLParserResult;
      hsql::SQLParser::parse(query_str, result);

      if ( result->isValid() )
      {
        const hsql::SQLStatement* statement = result->getStatement(0);

        switch ( statement->type() )
        {
          case 1: // SELECT
            break;
          case 3: // INSERT
            break;
          case 4: // UPDATE
            break;
          case 5: // DELETE
            break;
          case 6: // CREATE
          {
            hsql::CreateStatement* create_stmt = (hsql::CreateStatement*) statement;
            char* tbl_path = getTablePath( create_stmt->tableName );

            if ( !dirExists(tbl_path) )
              Table* table = new Table(create_stmt->tableName, create_stmt->columns);
            else
              fprintf(stderr, "A table with that name already exists!\n");

            break;
          }

          default:
            fprintf(stderr, "Query implementation missing!\n");
        }
      }
      else
      {
        fprintf(stderr, "Given string is not a valid SQL query.\n");
        fprintf(stderr, "%s\n", result->errorMsg());
      }

      add_history( query_str.c_str() );
      query_str.clear();
    }
    else query_str += "\n";
    
    free(query);
  }

  return 0;
}
