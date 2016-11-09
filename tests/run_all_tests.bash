#!/bin/bash
echo "starting full test run"
export MONGRATE_TEST_DB_PORT=20187
export MONGRATE_HOME=$(realpath ..)

for test in ./*/test.sh; do   
    echo "Running test $test"; 
    cd `dirname $test`
    # bash `basename $test'
    bash test.sh
    if [ $? -eq 0 ]
    then
        echo "$test SUCCESS"
    else
        echo "$test FAILED"
    fi
    cd ..

done;
