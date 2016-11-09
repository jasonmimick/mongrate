#!/bin/bash
echo "starting single test run"
export MONGRATE_TEST_DB_PORT=20187
export MONGRATE_HOME=$(realpath ..)

echo $1
test="$1/\test.sh"
echo "Running test $test"; 
echo `dirname $test`
cd `dirname $test`
# bash `basename $test'
bash test.sh
echo "test return code was $?"
if [ $? -eq 0 ]
then
    echo "$test SUCCESS"
else
    echo "$test FAILED"
fi
cd ..

