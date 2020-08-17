#include "DBException.hh"
#include "flaviadb_definitions.hh"
#include "thirdparty/microtest/microtest.h"
#include <string>
using namespace std;

TEST(NonExistingTableExceptionTest)
{
  DBException e{NON_EXISTING_TABLE, "testTableName"};
  ASSERT_STREQ("Table testTableName doesn't exist.\n", e.what());
}

TEST(UnreadableMetadataExceptionTest)
{
  DBException e{UNREADABLE_METADATA};
  ASSERT_STREQ("ERROR: Could not read metadata.dat.\n", e.what());
}

TEST(DifferentMetadataNameExceptionTest)
{
  DBException e{DIFFERENT_METADATA_NAME};
  ASSERT_STREQ(
      "ERROR: Metadata's stored table name doesn't match requested table.\n",
      e.what());
}

TEST(MissingValuesExceptionTest)
{
  DBException e{MISSING_VALUES};
  ASSERT_STREQ(
      "ERROR: If you are ignoring some values, you need to provide the "
      "columns where the stated values will be inserted\n",
      e.what());
}

TEST(TooManyValuesExceptionTest)
{
  DBException e{TOO_MANY_VALUES};
  ASSERT_STREQ("ERROR: Received more values than columns in table.\n",
               e.what());
}

TEST(CharTooBigExceptionTest)
{
  DBException e{CHAR_TOO_BIG, "testTableName", "testColumnName"};
  ASSERT_STREQ(
      "ERROR: CHAR value inserted for column testColumnName is too big.\n",
      e.what());
}

TEST(InvalidDateExceptionTest)
{
  DBException e{INVALID_DATE, "testTableName", "testDateColumnName"};
  ASSERT_STREQ("ERROR: Value for testDateColumnName isn't a valid date or "
               "isn't a date. Max YEAR value supported is 9999. Please use " +
                   string(DATE_FORMAT) + " format.\n",
               e.what());
}

TEST(InvalidDataTypeExceptionTest)
{
  DBException e{INVALID_DATA_TYPE, "testTableName", "testColumnName"};
  ASSERT_STREQ(
      "ERROR: testColumnName's data type doesn't match received value's data "
      "type.\n",
      e.what());
}

TEST(StartNotAloneExceptionTest)
{
  DBException e{STAR_NOT_ALONE};
  ASSERT_STREQ("ERROR: Can't use * along with other fields.\n", e.what());
}

TEST(RepeatedFieldsExceptionTest)
{
  DBException e{REPEATED_FIELDS};
  ASSERT_STREQ("ERROR: Detected repeated fields in query.\n", e.what());
}

TEST(ColumnNotInTableExceptionTest)
{
  DBException e{COLUMN_NOT_IN_TABLE, "testTableName", "testColumnName"};
  ASSERT_STREQ(
      "ERROR: Column testColumnName not found in table testTableName.\n",
      e.what());
}

TEST(IndexAlreadyExistsExceptionTest)
{
  DBException e{INDEX_ALREADY_EXISTS, "testTableName", "testColumnName"};
  ASSERT_STREQ("ERROR: There's already an index on column testColumnName.\n",
               e.what());
}

TEST(IndexNotIntExceptionTest)
{
  DBException e{INDEX_NOT_INT};
  ASSERT_STREQ("ERROR: Indexed column must be of type INT.\n", e.what());
}
