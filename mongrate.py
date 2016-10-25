#!/usr/bin/env python
#
# mongrate.py
# git-base migration tool for MongoDB
#
#

import yaml
import logging
import sys, os
import argparse
from git import *
import pymongo
from subprocess import Popen, PIPE
import json

class Mongrate():

    MONGRATE_DB = 'admin'
    MONGRATE_STATUS_COLL = 'mongrate.status'
    MONGRATE_HISTORY_COLL = 'mongrate.history'
    MONGRATE_WORKING_SCRIPT_COLL = 'mongrate.scripts'
    MONGRATE_HISTORY_SCRIPT_COLL = 'mongrate.history.scripts'

    def __init__(self, config, args, logger):
        self.config = config
        self.args = args
        self.logger = logger
        # convience to check if --dry-run flag specified
        if self.args.dry_run:
            self.DRY_RUN = True
        else:
            self.DRY_RUN = False

    def act(self,action):
        """Perform the request action"""
        try:
            self.logger.debug("got request for action '"+action+"'")
            act = getattr(self,action)
            act()
            self.logger.debug("action '" + action + "' complete.")
        except Exception as exp:
            self.logger.error(exp)
            if self.config['verbose']:
                raise

    def status(self):
        """Report on the current status of the git repo and MongoDB instance"""
        self.logger.debug("status called")
        git_status = self.__get_git_status()
        print "Current git status\n------------------"
        print git_status
        mongo_status = self.__get_mongo_status()
        print "\nCurrent mongo status\n--------------------"
        print mongo_status

    def migrate(self):
        """Migrate to/from the target git commit"""
        mongo_status = self.__get_mongo_status()
        if mongo_status['status'] == 'NOT MANAGED BY MONGRATE':
            raise Exception('Cannot migrate: %s' % (mongo_status['status']))
        change_list = self.get_git_changelist()
        for change in change_list:
            script = os.path.join(self.config['git'],change['file'])
            if not self.DRY_RUN:
                result = self.__load_script(script)
            else:
                self.logger.info("--dry-run: would have loaded %s" % script)
                result = True
            self.logger.debug("result from %s was %s" % (script, str(result)))


    def generate_template_migration(self):
        """Generate a template migration"""

    def test_run_script(self):
        for script in self.args.test_script.split(','):
            result = self.__load_script(script)
            self.logger.debug("result from %s was %s" % (script, str(result)))

    # git specific functions

    # TODO: modify this to deal with tags, branches
    # rather than just commit sha's
    def get_git_changelist(self):
        target_commit = self.args.git_commit
        self.logger.info("__get_git_changelist target_commit=%s" % target_commit)
        git_status = self.__get_git_status()
        current_commit = git_status['commits'][0];
        self.logger.debug("current_commit %s" % (str(current_commit)))
        # validate we already have the target_commit
        # if not, then we need to pull?
        got_target = False
        for commit in git_status['commits']:
            if commit.id == target_commit:
                self.logger.debug("found target commit=%s" % str(commit))
                got_target = True
        if not got_target:
            m = "target commit %s was not found in repo commits" % (target_commit)
            raise Exception(m)
        repo = self.__get_git_repo()
        if not target_commit == current_commit.id:
            diff = repo.git.diff(target_commit,"--name-status").split('\n')
        else:
            diff = repo.git.show(target_commit,"--name-status","--oneline").split('\n')[1:]
        self.logger.debug(diff)
        change_list = []
        for line in diff:
            parts = line.split('\t')
            self.logger.debug(parts)
            # filter change list based on migration_home
            if self.config['migration_home'] in parts[1]:
                change_list.append( { "action" : parts[0], "file" : parts[1] } )
            else:
                m = "found change %s but was not under %s" % (str(parts),self.config['migration_home'])
                self.logger.debug(m)
        self.logger.debug(change_list)
        return change_list

    def __get_git_status(self):
        git_status = {}
        git_status['git repo']=self.config['git']
        git_status['migration_home']=self.config['migration_home']
        repo = self.__get_git_repo()
        # commits() returns a list of commits with the latest commit first
        # so the first item in the list is our current "state"
        # we should store this info in the 'status' collection
        git_status['commits']=repo.commits()
        return git_status

    def __get_git_repo(self):
        if not hasattr(self,'repo'):
            self.repo = Repo( self.config['git'] )
        return self.repo

    # end git specific functions

    # mongo specific functions
    def __get_mongo_status(self):
        mongo_status = {}
        mongo_status['mongodb']=self.config['mongodb']
        mongo = self.__get_mongo_client()
        if not self.MONGRATE_STATUS_COLL in mongo[self.MONGRATE_DB].collection_names():
            self.logger.error("This MongoDB instance does not seem to be managed by mongrate")
            mongo_status['status'] = "NOT MANAGED BY MONGRATE"
        else:
            mongo_status['status'] = list(mongo[self.MONGRATE_DB][self.MONGRATE_STATUS_COLL].find())
        return mongo_status

    def __get_mongo_client(self):
        if not hasattr(self,'mongo'):
            # TODO: add in extra auth parameters here!
            try:
                self.logger.debug("attempting connection MongoDB: " + self.config['mongodb'])
                self.mongo = pymongo.MongoClient(self.config['mongodb'])
            except Exception as exp:
                logger.error(exp)
                raise
        return self.mongo

    # actually we should load each migration and save into
    # temp collection, then we can sort and run in order
    #
    def __load_script(self,script):
        """Run a script as specified by the full path to the .js file return True if OK, False if Error"""
        self.logger.debug("__load_script called for '"+script+"'")
        shell_args = []
        shell_args.append("mongo")
        # TODO: deal with auth creds given from mongrate args
        shell_args.append(self.config['mongodb'])
        eval_string = "mongrate = %s;" % (self.__get_mongrate_util_object(script))
        eval_string += "db=db.getSiblingDB('%s');" % self.MONGRATE_DB
        eval_string += "load('%s');" % (script)
        #eval_string += "printjson(mongrate);"
        eval_string += "eval('mongrate.tryLoad = ' + mongrate.tryLoad);"
        eval_string += "mongrate.tryLoad();"
        shell_args.append("--eval")
        shell_args.append(eval_string)
        self.logger.debug("shell_args: %s" % (shell_args))
        proc = Popen(shell_args, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if proc.returncode != 0:
            self.logger.error("Error running script '%s' output:'%s' error: '%s'" % (script, output, error))
            return False
        else:
            self.logger.debug("Output from '%s' was '%s'" % (script, output))
            return True

    def __get_mongrate_util_object(self,script):
        if not hasattr(self,'mongrate'):
            m = {}
            m['migrations'] = []
            m['migrations'].append(script)
            m['mongodb'] = self.config['mongodb']
            # not sure here, keep list of migrations?
            # add some "site identifier" so migration
            # implementation can check this
            self.mongrate = {}
            self.mongrate['meta'] = m
        else:
            self.mongrate['meta']['migrations'].append(script)

        m = {}
        # tryLoad will do checking on a given migration for required
        # things, and then insert the migration into a collection for later processing
        try_load="""function() {
            if (Object.keys(mongrate).indexOf('exports')==-1) {
                throw 'No exports property found on mongrate';
            }
            var p = Object.keys(mongrate.exports);
            if (p.indexOf('_id')==-1) {
                throw '%s missing _id';
            }
            if (p.indexOf('onLoad')!=-1) {
                print('Calling onLoad for %s');
                mongrate.exports.onLoad(this);
            } else {
                print('No onLoad found for %s');
            }
            var r = db.getSiblingDB('%s').getCollection('%s').insert(mongrate.exports);
            if ( !r.ok ) {
                throw r.getWriteError().errmsg;
            }

        }"""
        try_load = try_load.replace('\"','')
        m['tryLoad'] = try_load % (script,script,script,self.MONGRATE_DB,self.MONGRATE_WORKING_SCRIPT_COLL)
        self.mongrate['tryLoad']=m['tryLoad']
        return json.dumps(self.mongrate)

    # generate JSON for mongrate state object
    # which gets passed into each migration
    # script, this is generated from a property
    # so we can keep track of all the state
    def __get_mongrate_state_object(self, script):
        """Retuns a JSON string to initialize mongrate state object for migration"""
        if not hasattr(self,'mongrate'):
            m = {}
            m['migrations'] = []
            m['migrations'].append(script)
            m['mongodb'] = self.mongodb
            # not sure here, keep list of migrations?
            # add some "site identifier" so migration
            # implementation can check this
            self.mongrate = {}
            self.mongrate['meta'] = m
        else:
            self.mongrate['meta']['migrations'].append(script)
        return json.dumps(self.mongrate)

    # end mongo specific functions

    def __compile_migrations(self):
        print "__compile"


# 'main' starts here

def main():
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--action",default="status"
                        ,help='Action to perform. status, migrate, generate_migration, default is \'status\'')
    parser.add_argument("-f","--config",default="./mongrate.conf",help='Configuration file see docs')
    parser.add_argument("--git-commit",help="git tag/branch/commit hash to migrate to")
    parser.add_argument("--dry-run",action='store_true',default=False
                        ,help='Only show what would have been done, don\'t actually do anything')
    parser.add_argument("--test-script",help='Internal testing use only')
    parser.add_argument("--test-script-func",help='Internal testing use only')
    # TODO: add more command line options to allow setting security credentials for Mongo connection
    # so they do not need to be stored in config file
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))
    logger = logging.getLogger("mongrate")
    logger.setLevel(getattr(logging,config.get('loglevel','INFO').upper()))
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    if 'logfile' in config:
        handler = logging.FileHandler(os.path.abspath(config['logfile']))
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.debug("args: " + str(args))
    logger.debug("config: " + str(config))
    logger.info("log level set to " + logging.getLevelName(logger.getEffectiveLevel()))
    logger.info("--dry-run is " + str(args.dry_run))
    mongrate = Mongrate(config, args, logger)
    logger.info('Mongrate initialized, attempt to perform ' + args.action + ' action')
    mongrate.act(args.action)

if __name__ == '__main__':
    main()



