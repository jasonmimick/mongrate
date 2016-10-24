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

class Mongrate():

    MONGRATE_DB = 'admin'
    MONGRATE_STATUS_COLL = 'mongrate.status'

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

    def generate_template_migration(self):
        """Generate a template migration"""

    def test_run_script(self):
        self.__run_script(self.args.test_script,self.args.test_script_func)

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

    def __run_script(self,script,func_to_call="info"):
        """Run a script as specified by the full path to the .js file return True if OK, False if Error"""
        self.logger.info("__run_script called for '"+script+"'")
        shell_args = []
        shell_args.append("mongo")
        # TODO: deal with auth creds given from mongrate args
        shell_args.append(self.config['mongodb'])
        eval_string = "load('%s');migration.%s();" % (script,func_to_call)
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
    parser.add_argument("--git-hash",help="git tag/branch/commit hash to migrate to")
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



