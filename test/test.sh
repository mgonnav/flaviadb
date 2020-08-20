#!/bin/bash
RET=0

expectSuccess() {
  if [ $? != 0 ]; then
    RET=$(($RET + 1))
    echo -e "\033[1;31m{  failed} "$1 "\033[0m"
  else
    echo -e "\033[0;32m{      ok} "$1 "\033[0m"
  fi
}

expectFailure() {
  if [ $? == 0 ]; then
    RET=$(($RET + 1))
    echo -e "\033[1;31m{      ok} "$1" (expected failure)\033[0m"
  else
    echo -e "\033[0;32m{  failed} "$1" (expected failure)\033[0m"
  fi
}

TEST_ALL=bin/tests
TABLE_TEST=bin/table
DBEXCPT_TEST=bin/dbexception
WHERE_TEST=bin/where

if [[ -f "$TEST_ALL" ]]; then
  bin/tests
  RET=$?
  expectSuccess "All Tests"
  rm bin/tests
fi

if [[ -f "$TABLE_TEST" ]]; then
  bin/table
  RET=$?
  expectSuccess "Table Test"
  rm bin/table
fi

if [[ -f "$DBEXCPT_TEST" ]]; then
  bin/dbexception
  RET=$?
  expectSuccess "DBException Test"
  rm bin/dbexception
fi

if [[ -f "$WHERE_TEST" ]]; then
  bin/where
  RET=$?
  expectSuccess "Where Test"
  rm bin/where
fi

exit $RET
