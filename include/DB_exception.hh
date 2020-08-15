#ifndef _DB_EXCEPTION
#define _DB_EXCEPTION

#include <exception>
#include <string>

class DB_exception : public std::exception
{
protected:
  std::string error_message;

public:
  explicit DB_exception(const std::string& what_arg);
  virtual const char* what() const throw();
};

inline DB_exception::DB_exception(const std::string& what_arg)
{
  this->error_message = what_arg;
}

inline const char* DB_exception::what() const throw()
{
  return error_message.c_str();
}

#endif
