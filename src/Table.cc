#include "Where.hh"
#include "printutils.hh"

namespace ft = ftools;
namespace fs = std::filesystem;
namespace pu = printUtils;

Table::Table(std::string name)
{
  this->name = name;
  loadPaths(name);
  checkTableExists();
  load_metadata();

  this->reg_count = ft::getRegCount(name);

  this->indexes = new std::vector<Index*>;
  loadIndexes();
}

void Table::loadPaths(std::string const& name)
{
  this->path = ft::getTablePath(name);
  this->regs_path = ft::getRegistersPath(name);
  this->indexes_path = ft::getIndexesPath(name);
  this->metadata_path = ft::getMetadataPath(name);
}

void Table::checkTableExists()
{
  if (!ft::dirExists(this->path))
    throw DBException{NON_EXISTING_TABLE, this->name};
}

bool Table::load_metadata()
{
  openMetadataFile();
  loadMetadataHeader();
  for (auto& column : *this->columns)
    loadColumnData(column);

  return 1;
}

void Table::openMetadataFile()
{
  this->metadata_file.open(this->metadata_path);
  if (!this->metadata_file.is_open())
    throw DBException{UNREADABLE_METADATA};
}

void Table::loadMetadataHeader()
{
  loadTableName();
  loadColumnCount();
  loadRegSize();
}

void Table::loadTableName()
{
  std::string metadata_table_name;
  getline(this->metadata_file, metadata_table_name, '\t');

  if (this->name != metadata_table_name)
    throw DBException{DIFFERENT_METADATA_NAME};
}

void Table::loadColumnCount()
{
  std::string column_count;
  getline(this->metadata_file, column_count, '\t');
  this->columns = new std::vector<hsql::ColumnDefinition*>(stoi(column_count));
}

void Table::loadRegSize()
{
  std::string reg_size;
  getline(this->metadata_file, reg_size, '\n');
  this->reg_size = stoi(reg_size);
}

void Table::loadColumnData(hsql::ColumnDefinition*& column)
{
  std::string data;

  getline(this->metadata_file, data, '\t');
  char* col_name = new char[data.size()];
  strcpy(col_name, data.c_str());

  hsql::ColumnType col_type = getColumnType();

  getline(this->metadata_file, data, '\n');
  bool col_nullable = stoi(data);

  column = new hsql::ColumnDefinition(col_name, col_type, col_nullable);
}

void Table::loadIndexes()
{
  for (const auto& reg : fs::directory_iterator(this->indexes_path))
  {
    std::string idx_name = reg.path().filename();
    indexes->push_back(new Index(idx_name));
  }
}

void Table::openDataFile() { this->data_file.open(this->regs_path); }

void Table::loadStoredRegisters()
{
  this->registers = std::make_unique<std::list<std::pair<int, RegisterData>>>();
  this->openDataFile();

  std::string data;

  while (this->data_file.eof() == 0)
  {
    std::vector<std::string> reg_data;

    int start_pos_in_file = data_file.tellg() - data.size() - 1;
    for (size_t i = 0; i < this->columns->size(); i++)
    {
      getline(data_file, data, '\t');
      reg_data.push_back(data);
    }
    getline(data_file, data);
    this->registers->push_back({start_pos_in_file, RegisterData(reg_data)});
  }

  this->data_file.close();
}

Table::Table(std::string name, std::vector<hsql::ColumnDefinition*>* cols)
{
  this->name = name;
  loadPaths(name);
  this->indexes = new std::vector<Index*>;

  createTableFolders();

  this->columns = cols;
  this->reg_size = calculateRegSize();
  this->registers = std::make_unique<std::list<std::pair<int, RegisterData>>>();

  // Create medata.dat file for table
  // and fill it with table's name & cols info
  std::ofstream wMetadata(this->metadata_path);
  wMetadata << this->name << "\t" << this->columns->size() << "\t"
            << this->reg_size << "\n";
  for (const hsql::ColumnDefinition* col : *this->columns)
    wMetadata << col->name << "\t" << (int)col->type.data_type << "\t"
              << col->type.length << "\t" << col->nullable << "\n";
  wMetadata.close();

  std::ofstream wCount(ft::getRegCountPath(this->name));
  wCount << 0;    // 0 regs when table is created
  wCount.close();

  std::ofstream r(this->regs_path);
  r.close();

  std::cout << "Table " << this->name << " was created successfully.\n";
}

void Table::createTableFolders()
{
  ft::createFolder(this->path);
  ft::createFolder(this->indexes_path);
}

int Table::calculateRegSize()
{
  int reg_size = 0;
  for (const auto& col : *this->columns)
  {
    hsql::DataType data_type = col->type.data_type;
    if (data_type == hsql::DataType::INT)
      reg_size += 4;
    else if (data_type == hsql::DataType::CHAR)
      reg_size += col->type.length;
    else if (data_type == hsql::DataType::DATE)
      reg_size += 8;
  }

  return reg_size;
}

Table::~Table()
{
  delete this->columns;
  delete this->indexes;
}

hsql::ColumnType Table::getColumnType()
{
  std::string data;

  getline(this->metadata_file, data, '\t');
  int col_enum = stoi(data);

  getline(this->metadata_file, data, '\t');
  int col_length = stoi(data);

  hsql::ColumnType col_type((hsql::DataType)col_enum, col_length);
  return col_type;
}
