BIN = bin

########################################
############ Test & Example ############
########################################
TEST_BUILD   = $(BIN)/tests
TEST_CFLAGS  = -std=c++1z -Iinclude/ -Itest/
TEST_CC      = $(shell find test/ -name '*.cc')
TEST_ALL     = $(shell find test/ -name '*.cc') $(shell find test/ -name '*.hh')
SRC_ALL			 = $(shell find src/ -name '*.cc')

test: $(TEST_BUILD)
	bash test/test.sh

$(TEST_BUILD): $(TEST_ALL)
	@mkdir -p $(BIN)/
	$(CXX) $(TEST_CFLAGS) $(TEST_CC) $(SRC_ALL) -o $(TEST_BUILD) -lsqlparser
