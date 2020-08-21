#include "thirdparty/microtest/microtest.h"

#include "Table.hh"
#include "Processor.hh"
#include "filestruct.hh"
#include <fstream>
#include <hsql/SQLParser.h>
#include <string>
using namespace std;

namespace ft = ftools;

void assertCorrectPaths(Table const& table);
void assertPathsExist(Table const& table);
void assertPathsDontExist(Table const& table);
void dropIfExists(string tableName);
void readFromFileTo(vector<string>& values, string filename);

string getFilenameWithExtension(string filename)
{
  return filename + ".sqlito";
}

string getFilenameWithExtension(int filename)
{
  return to_string(filename) + ".sqlito";
}

string getFilePath(Table const& tbl, string filename)
{
  return tbl.regs_path + getFilenameWithExtension(filename);
}

string getFilePath(Table const& tbl, int filename)
{
  return tbl.regs_path + getFilenameWithExtension(filename);
}

string getIndexedPath(Table const& tbl, string filename)
{
  return tbl.indexes_path + getFilenameWithExtension(filename);
}

string getIndexedPath(Table const& tbl, int filename)
{
  return tbl.indexes_path + getFilenameWithExtension(filename);
}

TEST(CreateAndDropTableTest)
{
  dropIfExists("createTable");

  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse(
      "CREATE TABLE createTable (id int, name char(10), birthdate date);",
      result);
  auto stmt = (hsql::CreateStatement*)result->getStatement(0);

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

  Processor::drop_table(tbl);
  assertPathsDontExist(*tbl);
}

void dropIfExists(string tableName)
{
  unique_ptr<Table> tbl;
  try
  {
    tbl = make_unique<Table>(tableName);
  }
  catch (const DBException& e)
  {
  }

  if (tbl != nullptr)
    Processor::drop_table(tbl);
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
  try
  {
    tbl = make_unique<Table>("nonExistingTableName");
  }
  catch (const DBException& e)
  {
    ASSERT_NULL(tbl);
  }
}

TEST(InsertRecordTest)
{
  auto tbl = make_unique<Table>("testTable");

  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse(
      "INSERT INTO testTable VALUES (333, 'testName', '07-07-2001');", result);
  auto stmt = (hsql::InsertStatement*)result->getStatement(0);

  int expected_new_reg_count = tbl->registers->size() + 1;
  Processor::insert_record(stmt, tbl);
  ASSERT_EQ(expected_new_reg_count, tbl->registers->size());

  auto inserted_register =
      tbl->registers->at(getFilenameWithExtension(tbl->reg_count));

  auto inserted_register_id = inserted_register.at(0);
  ASSERT_STREQ("333", inserted_register_id);

  auto inserted_register_name = inserted_register.at(1);
  ASSERT_STREQ("testName", inserted_register_name);

  auto inserted_register_date = inserted_register.at(2);
  ASSERT_STREQ("07-07-2001", inserted_register_date);

  string filename = getFilenameWithExtension(tbl->reg_count);
  ASSERT_TRUE(ft::fileExists(tbl->regs_path + filename));
}

TEST(UpdateRecordsTest)
{
  auto tbl = make_unique<Table>("testTable");

  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("UPDATE testTable SET id = 777 WHERE id = 333;",
                         result);
  auto stmt = (hsql::UpdateStatement*)result->getStatement(0);

  int expected_new_reg_count = tbl->registers->size();
  Processor::update_records(stmt, tbl);

  ASSERT_EQ(expected_new_reg_count, tbl->registers->size());

  auto it = tbl->registers->find(getFilenameWithExtension(tbl->reg_count));
  ASSERT_TRUE(it != tbl->registers->end());

  auto updated_register =
      tbl->registers->at(getFilenameWithExtension(tbl->reg_count));
  vector<string> stored_data{};
  readFromFileTo(stored_data, getFilePath(*tbl, tbl->reg_count));

  auto updated_register_id = updated_register.at(0);
  ASSERT_STREQ("777", updated_register_id);
  ASSERT_STREQ(stored_data.at(0), updated_register_id);

  auto updated_register_name = updated_register.at(1);
  ASSERT_STREQ("testName", updated_register_name);
  ASSERT_STREQ(stored_data.at(1), updated_register_name);

  auto updated_register_date = updated_register.at(2);
  ASSERT_STREQ("07-07-2001", updated_register_date);
  ASSERT_STREQ(stored_data.at(2), updated_register_date);
}

void readFromFileTo(vector<string>& values, string filename)
{
  ifstream inFile(filename);

  if (!inFile.is_open())
  {
    cout << "Couldn't open file " << filename << "\n";
    exit(1);
  }

  string data;
  while (getline(inFile, data, '\t'))
  {
    values.push_back(data);
  }
}

TEST(DeleteRecordsTest)
{
  auto tbl = make_unique<Table>("testTable");

  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("DELETE FROM testTable WHERE id = 777;", result);
  auto stmt = (hsql::DeleteStatement*)result->getStatement(0);

  int expected_new_reg_count = tbl->registers->size() - 1;
  Processor::delete_records(stmt, tbl);

  ASSERT_EQ(expected_new_reg_count, tbl->registers->size());

  auto it = tbl->registers->find(getFilenameWithExtension(tbl->reg_count));
  ASSERT_TRUE(it == tbl->registers->end());
}

TEST(CreateIndexOnEmptyTableTest)
{
  dropIfExists("indexedEmptyTable");

  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse(
      "CREATE TABLE indexedEmptyTable (id int, name char(10), birthdate date);",
      result);
  auto stmt = (hsql::CreateStatement*)result->getStatement(0);

  auto tbl = make_unique<Table>("indexedEmptyTable", stmt->columns);

  Processor::create_index("id", tbl);
  ASSERT_TRUE(ft::dirExists(tbl->indexes_path + "id/"));

  dropIfExists("indexedEmptyTable");
}

TEST(CreateIndexOnPopulatedTableTest)
{
  dropIfExists("indexedPopulatedTable");

  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("CREATE TABLE indexedPopulatedTable (id int, name "
                         "char(10), birthdate date);",
                         result);
  auto createStmt = (hsql::CreateStatement*)result->getStatement(0);

  auto tbl = make_unique<Table>("indexedPopulatedTable", createStmt->columns);

  result->releaseStatements();
  hsql::SQLParser::parse(
      "INSERT INTO indexedPopulatedTable VALUES (1, 'testName1', '01-01-2000');"
      "INSERT INTO indexedPopulatedTable VALUES (2, 'testName2', '02-01-2000');"
      "INSERT INTO indexedPopulatedTable VALUES (3, 'testName3', "
      "'03-01-2000');",
      result);

  for (const auto& stmt : result->getStatements())
    Processor::insert_record((hsql::InsertStatement*)stmt, tbl);

  Processor::create_index("id", tbl);
  ASSERT_TRUE(ft::dirExists(tbl->indexes_path + "id/"));
  ASSERT_TRUE(ft::dirExists(tbl->indexes_path + "id/" + "1/"));
  ASSERT_TRUE(ft::fileExists(tbl->indexes_path + "id/" + "1/" + "1.sqlito"));
  ASSERT_TRUE(ft::dirExists(tbl->indexes_path + "id/" + "2/"));
  ASSERT_TRUE(ft::fileExists(tbl->indexes_path + "id/" + "2/" + "2.sqlito"));
  ASSERT_TRUE(ft::dirExists(tbl->indexes_path + "id/" + "3/"));
  ASSERT_TRUE(ft::fileExists(tbl->indexes_path + "id/" + "3/" + "3.sqlito"));

  dropIfExists("indexedPopulatedTable");
}

TEST(InsertRegisterToIndexedTableTest)
{
  dropIfExists("indexedTable");

  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("CREATE TABLE indexedTable (id int, name "
                         "char(10), birthdate date);",
                         result);
  auto createStmt = (hsql::CreateStatement*)result->getStatement(0);

  auto tbl = make_unique<Table>("indexedTable", createStmt->columns);

  result->releaseStatements();
  hsql::SQLParser::parse(
      "INSERT INTO indexedTable VALUES (1, 'testName1', '01-01-2000');"
      "INSERT INTO indexedTable VALUES (2, 'testName2', '02-01-2000');"
      "INSERT INTO indexedTable VALUES (3, 'testName3', "
      "'03-01-2000');",
      result);

  Processor::create_index("id", tbl);
  for (const auto& stmt : result->getStatements())
    Processor::insert_record((hsql::InsertStatement*)stmt, tbl);

  ASSERT_TRUE(ft::dirExists(tbl->indexes_path + "id/"));
  ASSERT_TRUE(ft::dirExists(tbl->indexes_path + "id/" + "1/"));
  ASSERT_TRUE(ft::fileExists(tbl->indexes_path + "id/" + "1/" + "1.sqlito"));
  ASSERT_TRUE(ft::dirExists(tbl->indexes_path + "id/" + "2/"));
  ASSERT_TRUE(ft::fileExists(tbl->indexes_path + "id/" + "2/" + "2.sqlito"));
  ASSERT_TRUE(ft::dirExists(tbl->indexes_path + "id/" + "3/"));
  ASSERT_TRUE(ft::fileExists(tbl->indexes_path + "id/" + "3/" + "3.sqlito"));

  dropIfExists("indexedTable");
}
