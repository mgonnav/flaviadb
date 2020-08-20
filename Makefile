BIN = bin

########################################
############ Test & Example ############
########################################
TEST_BUILD   = $(BIN)/tests
TEST_TABLE	 = $(BIN)/table
TEST_DBEXCEPTION = $(BIN)/dbexception
TEST_WHERE   = $(BIN)/where
TEST_CFLAGS  = -std=c++1z -Iinclude/ -Itest/
TEST_CC      = $(shell find test/ -name '*.cc')
TEST_ALL     = $(shell find test/ -name '*.cc') $(shell find test/ -name '*.hh')
SRC_ALL			 = $(shell find src/ -name '*.cc')

test: $(TEST_BUILD)
	bash test/test.sh

$(TEST_BUILD): $(TEST_ALL)
	@mkdir -p $(BIN)/
	$(CXX) $(TEST_CFLAGS) $(TEST_ALL) $(SRC_ALL) -o $(TEST_BUILD) -lsqlparser

table_test: $(TEST_TABLE)
	bash test/test.sh

$(TEST_TABLE): test/table_tests.cc
	@mkdir -p $(BIN)/
	$(CXX) $(TEST_CFLAGS) test/test_main.cc test/table_tests.cc $(SRC_ALL) -o $(TEST_TABLE) -lsqlparser

where_test: $(TEST_WHERE)
	bash test/test.sh

$(TEST_WHERE): test/where_tests.cc
	@mkdir -p $(BIN)/
	$(CXX) $(TEST_CFLAGS) test/test_main.cc test/where_tests.cc src/Where.cc -o $(TEST_WHERE) -lsqlparser
	
dbexcpt_test: $(TEST_DBEXCEPTION)
	bash test/test.sh

$(TEST_DBEXCEPTION): test/dbexception_tests.cc
	@mkdir -p $(BIN)/
	$(CXX) $(TEST_CFLAGS) test/test_main.cc test/dbexception_tests.cc -o $(TEST_DBEXCEPTION) -lsqlparser
