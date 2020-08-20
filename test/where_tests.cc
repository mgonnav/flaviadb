#include "thirdparty/microtest/microtest.h"
#include "Where.hh"
#include <hsql/SQLParser.h>

TEST(GetCorrectWhereTest)
{
  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("SELECT * FROM testTable WHERE id = 17;"
                         "SELECT * FROM testTable WHERE name = 'string';"
                         "SELECT * FROM testTable WHERE bday = '01-01-1970';",
                         result);
  auto stmt = (hsql::SelectStatement*)result->getStatement(0);

  auto where_int = Where::get(stmt->whereClause, hsql::DataType::INT);
  ASSERT_TRUE(typeid(*where_int) == typeid(WhereInt));

  stmt = (hsql::SelectStatement*)result->getStatement(1);
  auto where_char = Where::get(stmt->whereClause, hsql::DataType::CHAR);
  ASSERT_TRUE(typeid(*where_char) == typeid(WhereChar));

  stmt = (hsql::SelectStatement*)result->getStatement(2);
  auto where_date = Where::get(stmt->whereClause, hsql::DataType::DATE);
  ASSERT_TRUE(typeid(*where_date) == typeid(WhereDate));
}

TEST(WhereIntTest)
{
  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("SELECT * FROM testTable WHERE id = 17;", result);
  auto stmt = (hsql::SelectStatement*)result->getStatement(0);

  auto w = Where::get(stmt->whereClause, hsql::DataType::INT);

  ASSERT_TRUE(w->compare("17"));
  ASSERT_FALSE(w->compare("7"));
  ASSERT_FALSE(w->compare("74"));

  delete w;
}

TEST(WhereCharTest)
{
  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("SELECT * FROM testTable WHERE name = 'flavia';", result);
  auto stmt = (hsql::SelectStatement*)result->getStatement(0);

  auto w = Where::get(stmt->whereClause, hsql::DataType::CHAR);

  ASSERT_TRUE(w->compare("flavia"));
  ASSERT_FALSE(w->compare("Flavia"));
  ASSERT_FALSE(w->compare("FLAVIA"));
  ASSERT_FALSE(w->compare("any string possible"));

  delete w;
}

TEST(WhereDateTest)
{
  auto result = new hsql::SQLParserResult;
  hsql::SQLParser::parse("SELECT * FROM testTable WHERE birthdate > '07-07-2001';",
                         result);
  auto stmt = (hsql::SelectStatement*)result->getStatement(0);

  auto w = Where::get(stmt->whereClause, hsql::DataType::DATE);

  ASSERT_FALSE(w->compare("07-07-2001"));
  ASSERT_FALSE(w->compare("01-01-2000"));
  ASSERT_TRUE(w->compare("31-12-2018"));

  delete w;
}
