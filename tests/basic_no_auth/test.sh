#!/bin/bash
echo "basic_no_auth.sh test"
echo "test db port=$MONGRATE_TEST_DB_PORT"
mkdir ./testdata
if [ $? -eq 0 ]
then
    echo "created ./testdata directory"
else
    echo "./testdata exists, it should not, test cannot continue"
    exit 1
fi

mongod --dbpath ./testdata --logpath ./testdata/mongod.log --fork --port $MONGRATE_TEST_DB_PORT

if [ $? -eq 0 ]
then
    echo "test mongod started successfully"
else
    echo "unable to start test mongod"
    exit 1
fi

latest_commit=$(git rev-list --all | head -1)
echo "Rolling forward to latest commit $latest_commit"

python $MONGRATE_HOME/mongrate.py --action initialize --verbose 
python $MONGRATE_HOME/mongrate.py --action migrate --git-commit $latest_commit --verbose


mongo admin --port=$MONGRATE_TEST_DB_PORT --eval 'db.shutdownServer()'


rm -rf testdata/


