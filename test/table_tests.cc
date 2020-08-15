#include "thirdparty/microtest/microtest.h"

#include "Table.hh"
#include "filestruct.hh"
#include <hsql/SQLParser.h>
#include <string>
using namespace std;

namespace ft = ftools;

void assertCorrectPaths(Table const& table);
void assertPathsExist(Table const& table);
void assertPathsDontExist(Table const& table);
void dropIfExists(string tableName);

TEST(CreateAndDropTableTest)
{
  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse(
      "CREATE TABLE createTable (id int, name char(10), birthdate date);", result);
  auto stmt = (hsql::CreateStatement*)result->getStatement(0);

  dropIfExists("createTable");

  unique_ptr<Table> tbl;
  tbl = make_unique<Table>("createTable", stmt->columns);
  ASSERT_NOTNULL(tbl);

  string expected_name = "createTable";
  ASSERT_STREQ(expected_name, tbl->name);

  assertCorrectPaths(*tbl);
  assertPathsExist(*tbl);

  // 4 (id) + 10 (name) + 8 (birthdate) = 22
  ASSERT_EQ(22, tbl->reg_size);

  ASSERT_EQ(0, tbl->reg_count);

  tbl->drop_table();
  assertPathsDontExist(*tbl);
}

void dropIfExists(string tableName)
{
  unique_ptr<Table> tbl;
  try
  {
    tbl = make_unique<Table>(tableName);
  }
  catch (invalid_argument& e)
  {
  }

  if (tbl != nullptr)
    tbl->drop_table();
}

void assertPathsDontExist(Table const& table)
{
  ASSERT(!ft::dirExists(table.path));
  ASSERT(!ft::dirExists(table.regs_path));
  ASSERT(!ft::fileExists(table.metadata_path));
  ASSERT(!ft::dirExists(table.indexes_path));
}

TEST(ConstructExistingTableByName)
{
  auto tbl = make_unique<Table>("dummyTable");
  ASSERT_NOTNULL(tbl);

  string expected_name = "dummyTable";
  ASSERT_STREQ(expected_name, tbl->name);

  assertCorrectPaths(*tbl);

  int expected_reg_size = 4;
  ASSERT_EQ(expected_reg_size, tbl->reg_size);

  int expected_reg_count = 0;
  ASSERT_EQ(expected_reg_count, tbl->reg_count);
}

void assertCorrectPaths(Table const& table)
{
  string expected_path = FLAVIADB_TEST_DB + table.name + "/";
  ASSERT_STREQ(expected_path, table.path);

  string expected_regs_path = expected_path + "registers/";
  ASSERT_STREQ(expected_regs_path, table.regs_path);

  string expected_metadata_path = expected_path + "metadata.dat";
  ASSERT_STREQ(expected_metadata_path, table.metadata_path);

  string expected_indexes_path = expected_path + "indexes/";
  ASSERT_STREQ(expected_indexes_path, table.indexes_path);
}

void assertPathsExist(Table const& table)
{
  ASSERT(ft::dirExists(table.path));
  ASSERT(ft::dirExists(table.regs_path));
  ASSERT(ft::fileExists(table.metadata_path));
  ASSERT(ft::dirExists(table.indexes_path));
}

TEST(FailConstructingNonExistingTable)
{
  unique_ptr<Table> tbl;
  try {
    tbl = make_unique<Table>("nonExistingTableName");
  }
  catch (invalid_argument& e) {
    ASSERT_NULL(tbl);
  }
}

TEST(InsertRecordTest) { 
  auto tbl = make_unique<Table>("testTable");

  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("INSERT INTO testTable VALUES (333, 'testName', '07-07-2001');", result);
  auto stmt = (hsql::InsertStatement*) result->getStatement(0);

  int expected_new_reg_count = tbl->registers->size() + 1;
  tbl->insert_record(stmt);
  ASSERT_EQ(expected_new_reg_count, tbl->registers->size());

  auto inserted_register = tbl->registers->at(std::to_string(tbl->reg_count) + ".sqlito");

  auto inserted_register_id = inserted_register->data.at(0);
  ASSERT_STREQ("333", inserted_register_id);

  auto inserted_register_name = inserted_register->data.at(1);
  ASSERT_STREQ("testName", inserted_register_name);

  auto inserted_register_date = inserted_register->data.at(2);
  ASSERT_STREQ("07-07-2001", inserted_register_date);
}

TEST(DeleteRecordsTest) {
  auto tbl = make_unique<Table>("testTable");

  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("DELETE FROM testTable WHERE id = 333;", result);
  auto stmt = (hsql::DeleteStatement*) result->getStatement(0);

  int expected_new_reg_count = tbl->registers->size() - 1;
  tbl->delete_records(stmt);
  ASSERT_EQ(expected_new_reg_count, tbl->registers->size());
}

TEST_MAIN();
