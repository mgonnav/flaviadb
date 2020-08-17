#ifndef _DBEXCEPTION
#define _DBEXCEPTION

#include "flaviadb_definitions.hh"
#include <exception>
#include <iostream>
#include <string>

enum ErrorCode
{
  NON_EXISTING_TABLE,
  UNREADABLE_METADATA,
  DIFFERENT_METADATA_NAME,

  MISSING_VALUES,
  TOO_MANY_VALUES,
  CHAR_TOO_BIG,
  INVALID_DATE,
  INVALID_DATA_TYPE,

  STAR_NOT_ALONE,
  REPEATED_FIELDS,
  COLUMN_NOT_IN_TABLE,

  NON_MATCHING_UPDATE_DATA_TYPE,

  INDEX_ALREADY_EXISTS,
  INDEX_NOT_INT,
};

class DBException : public std::exception
{
protected:
  ErrorCode error_code;
  std::string error_message;
  std::string error_table;
  std::string error_column;
  std::string error_parameter;

public:
  explicit DBException(const ErrorCode& error_code);
  explicit DBException(const ErrorCode& error_code,
                       const std::string& error_table);
  explicit DBException(const ErrorCode& error_code,
                       const std::string& error_table,
                       const std::string& error_column);
  std::string formatErrorMessage();
  virtual const char* what() const throw();
};

inline DBException::DBException(const ErrorCode& error_code)
    : error_code(error_code)
{
  error_message = formatErrorMessage();
}

inline DBException::DBException(const ErrorCode& error_code,
                                const std::string& error_table)
    : error_code(error_code), error_table(error_table)
{
  error_message = formatErrorMessage();
}

inline DBException::DBException(const ErrorCode& error_code,
                                const std::string& error_table,
                                const std::string& error_column)
    : error_code(error_code), error_table(error_table),
      error_column(error_column)
{
  error_message = formatErrorMessage();
}

inline std::string DBException::formatErrorMessage()
{
  switch (error_code)
  {
  case NON_EXISTING_TABLE:
    return "Table " + error_table + " doesn't exist.\n";
  case UNREADABLE_METADATA:
    return "ERROR: Could not read metadata.dat.\n";
  case DIFFERENT_METADATA_NAME:
    return "ERROR: Metadata's stored table name doesn't match requested "
           "table.\n";
  case MISSING_VALUES:
    return "ERROR: If you are ignoring some values, you need to provide the "
           "columns where the stated values will be inserted\n";
  case TOO_MANY_VALUES:
    return "ERROR: Received more values than columns in table.\n";
  case CHAR_TOO_BIG:
    return "ERROR: CHAR value inserted for column " + error_column +
           " is too big.\n";
  case INVALID_DATE:
    return "ERROR: Value for " + error_column +
           " isn't a valid date or isn't a "
           "date. Max YEAR value supported is 9999. Please use " +
           DATE_FORMAT + " format.\n";
  case INVALID_DATA_TYPE:
    return "ERROR: " + error_column +
           "'s data type doesn't match received value's "
           "data type.\n";
  case STAR_NOT_ALONE:
    return "ERROR: Can't use * along with other fields.\n";
  case REPEATED_FIELDS:
    return "ERROR: Detected repeated fields in query.\n";
  case COLUMN_NOT_IN_TABLE:
    return "ERROR: Column " + error_column + " not found in table " +
           error_table + ".\n";
  case NON_MATCHING_UPDATE_DATA_TYPE:
    return "ERROR: Update column and value's types don't match.\n";
  case INDEX_ALREADY_EXISTS:
    return "ERROR: There's already an index on column " + error_column + ".\n";
  case INDEX_NOT_INT:
    return "ERROR: Indexed column must be of type INT.\n";

  default:
    return "";
  }
}

inline const char* DBException::what() const throw()
{
  return error_message.c_str();
}

#endif
