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

bin/tests
RET=$?
expectSuccess "Table Test"

rm bin/tests
exit $RET
