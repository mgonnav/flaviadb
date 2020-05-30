#include "Table.hh"
#include <hsql/SQLParser.h>
#include <sys/stat.h>
#include <cstring>
#include <fstream>
#include "filestruct.hh"
#include <iostream> // TESTING

Table::Table(const char* name, std::vector< hsql::ColumnDefinition* >* cols)
{
  this->name = name;
  this->path = getTablePath(this->name);
  this->regs_path = getRegistersPath(this->name);

  if ( mkdir(this->path, S_IRWXU) != 0 )
  {
    fprintf(stderr, "ERROR: Couldn't create table!\n");
    return;
  }
  if ( mkdir(this->regs_path, S_IRWXU) != 0 )
  {
    fprintf(stderr, "ERROR: Couldn't create registers folder!\n");
    return;
  }

  this->cols = cols;
  reg_size = 0;
  for (const hsql::ColumnDefinition* col : *this->cols)
  {
    if      ( col->type.data_type == hsql::DataType::INT )
      reg_size += 4;
    else if ( col->type.data_type == hsql::DataType::CHAR )
      reg_size += col->type.length;
    else if ( col->type.data_type == hsql::DataType::DATE )
      reg_size += 8;
  }

  // Create medata.dat file for table
  // and fill it with table's name & cols info
  this->metadata_path = getMetadataPath(this->name);
  std::ofstream wMetadata (this->metadata_path);
  wMetadata << this->name << "\n";
  for (const hsql::ColumnDefinition* col : *this->cols)
    wMetadata << col->name << "\t" << col->type << "\t" << col->nullable << "\n";
  wMetadata.close();
}