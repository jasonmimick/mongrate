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
import datetime
from toposort import toposort, toposort_flatten
import uuid

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
        # TODO: move 'verbose' to args, not config, better to be able to turn on
        # as needed
        #if not hasattr(self.config['verbose']):
        #    self.config['verbose']=False
        self.decorate_mongo_connection_string()

    def act(self,action):
        """Perform the request action"""
        try:
            self.logger.debug("got request for action '"+action+"'")
            act = getattr(self,action)
            act()
            self.logger.debug("action '" + action + "' complete.")
        except Exception as exp:
            self.logger.error(exp)
            if self.args.verbose:
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

    def initialize(self):
        """Initialize a mongoDB instance to work with mongrate"""
        self.logger.info('starting initialize action')
        mongo_status = self.__get_mongo_status()
        if not mongo_status['status'] == 'NOT MANAGED BY MONGRATE':
            if not self.args.force:
                raise Exception('Cannot initialize: %s' % (mongo_status['status']))
            else:
                self.logger.info('Mongo instance seems to be already inialized, but force=%s so continuing' % (str(self.args.force)))
        self.__initialize_mongo_instance()
        self.logger.info("mongoDB initialization complete")

    def migrate(self):
        """Migrate to/from the target git commit"""
        mongo_status = self.__get_mongo_status()
        if mongo_status['status'] == 'NOT MANAGED BY MONGRATE':
            raise Exception('Cannot migrate: %s' % (mongo_status['status']))
        # get changes from git
        # load them into mongo
        self.__clean_stored_migrations()
        rollback, change_list = self.get_git_changelist()
        self.logger.info('migrate rollback=%s' % (str(rollback)))
        loading_result = True
        for change in change_list:
            script = os.path.join(self.config['git'],change['file'])
            if not self.DRY_RUN:
                result = self.__load_script(script)
                loading_result = loading_result and  result
            else:
                self.logger.info("--dry-run: would have loaded %s" % script)
                result = True
            self.logger.debug("result from %s was %s" % (script, str(result)))
        if not loading_result:
            self.logger.info("Error encountered loading migrations. Please check logs and retry")
            return
        # figure out run order
        sorted_scripts = self.__get_scripts_toposort(rollback)
        self.logger.debug(sorted_scripts)
        # TODO: if rollback need to run in reverse order!
        # keep list of scripts we ran, if any errors
        # call rollback in reverse order of how we ran
        executed_scripts = []
        undo = False
        running_result = True
        for script in sorted_scripts:
            if undo:
                break
            try:
                self.logger.debug("about to run script='%s' rollback=%s" % (script,str(rollback)))
                if not self.DRY_RUN:
                    result = self.__run_script(script,rollback)
                    running_result = running_result and  result
                else:
                    self.logger.info("--dry-run: would have run %s" % script)
                    result = True
                self.logger.debug("result from %s was %s" % (script, str(result)))
                executed_scripts.append(script)
            except Exception as exp:
                self.logger.error(exp)
                self.logger.error('Error during migration execution, going into undo mode')
                undo = True

        if undo:
            self.logger.info('starting undo of scripts=%s' % str(executed_scripts))
            # TODO: need to undo in reverse order
            ex = executed_scripts[:]
            ex.reverse()
            for script in ex:
                self.logger.info('running undo (down()) for script=%s' % script)
                if not self.DRY_RUN:
                    result = self.__run_script(script,not rollback)
                else:
                    self.logger.info("--dry-run: would have run %s" % script)
                    result = True
                self.logger.debug("undo result from %s was %s" % (script, str(result)))
        else:
            #self.logger.info('migrations completed successfully, updating commit to %s' % self.args.git_commit)
            #self.__update_mongo_mongrate_commit(self.args.git_commit)
            self.logger.info('migration complete')

    def generate_template_migration(self):
        """Generate a template migration"""
        self.logger.info('generating template migration')
        mig_id = self.args.migration_id
        self.logger.info('template migration name = %s' % mig_id)
        fname = self.args.migration_id + '.js'
        script_filename = os.path.join(self.config['git'],self.config['migration_home'],fname)
        self.logger.debug('script_filename=%s', script_filename)
        if os.path.isfile(script_filename):
            self.logger.error('detected %s already exists' % script_filename)
            script_filename = script_filename + '.' + str(uuid.uuid4())
            self.logger.info('updated script filename to %s' % script_filename)
        t = open(script_filename,"w")
        def write_line(fd,s):
            fd.write(s)
            fd.write('\n')
        write_line(t,'/*************************')
        write_line(t,'* MongoDB Migration')
        write_line(t,'*')
        write_line(t,'* Generated on '+str(datetime.datetime.now()))
        write_line(t,'* _id : ' + mig_id)
        write_line(t,'**************************/')
        write_line(t,'')
        write_line(t,'migration = {')
        write_line(t,'  \'_id\' : \'' + mig_id + '\',')
        write_line(t,'  \'runAfter\' : [],')
        write_line(t,'  \'onLoad\' : function() {')
        write_line(t,'      // TODO: Add onLoad logic here')
        write_line(t,'      },')
        write_line(t,'  \'up\' : function() {')
        write_line(t,'      // TODO: rollforward logic')
        write_line(t,'      // TODO: Be sure do \'use\' right right database!')
        write_line(t,'      },')
        write_line(t,'  \'down\' : function() {')
        write_line(t,'      // TODO: Add undo/rollback logic here')
        write_line(t,'      // TODO: Be sure do \'use\' right right database!')
        write_line(t,'      },')
        write_line(t,'  \'info\' : function() {')
        write_line(t,'      // output information on this migration for reporting')
        write_line(t,'      print(\'migration : \' + this._id)')
        write_line(t,'      },')
        write_line(t,'}')
        write_line(t,'')
        write_line(t,'mongrate.exports = migration;')
        t.close()
        self.logger.info('Template migration %s generated %s' % (mig_id, script_filename))
        self.logger.info('migration generation complete')


    def test_run_script(self):
        for script in self.args.test_script.split(','):
            result = self.__load_script(script)
            self.logger.debug("result from %s was %s" % (script, str(result)))

    # git specific functions

    # TODO: modify this to deal with tags, branches
    # rather than just commit sha's
    def get_git_changelist(self):
        rollback = False
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
        c = git_status['commits']
        target_commit_index = [i for i in range(len(c)) if c[i].id==target_commit][0]
        mongo_status = self.__get_mongo_status()
        mongrate_commit = [s for s in mongo_status['status'] if s['_id']=='COMMIT'][0]['value']
        self.logger.debug('mongrate_commit = %s' % mongrate_commit)
        current_mongrate_commit_index = [i for i in range(len(c)) if c[i].id==mongrate_commit][0]
        self.logger.debug('current_mongrate_commit_index=%s' % current_mongrate_commit_index)
        self.logger.debug('target_commit_index=%s' % target_commit_index)
        #commit list is in reverse order
        #recent commits, then older ones
        #so, if the index of the target commit is greater
        # than the index of where we currently are, then it's a rollback
        if target_commit_index > current_mongrate_commit_index:
            self.logger.info("Target commit before current commit, rollback = True")
            rollback = True
        repo = self.__get_git_repo()
        # roll forward
        if not target_commit == current_commit.id:
            diff = repo.git.diff(target_commit,"--name-status").split('\n')
        else:
            diff = repo.git.show(target_commit,"--name-status","--oneline").split('\n')[1:]
        self.logger.debug(diff)
        change_list = []

        # filter change list based on migration_home
        # run 'common' scripts and then optionally any scripts based upon
        # distributionCenter arg
        common_filter = os.path.join(self.config['migration_home'],self.config['migration_common_home'])
        self.logger.info('Filtering changes based on migration home common folder=%s' % common_filter)
        if self.args.distributionCenter:
            dc = self.args.distributionCenter
            self.logger.info('Found distributionCenter arg, adding scripts for dc %s' % dc)
            d = os.path.join( self.config['migration_home'], dc)
            dc_filter = dc
        else:
            dc_filter = None
        self.logger.debug('common_filter=%s dc_filter=%s' % (common_filter,dc_filter))
        for line in diff:
            parts = line.split('\t')
            self.logger.debug(parts)
            add_file = False
            add_file = common_filter in parts[1]
            if dc_filter and dc_filter in parts[1]:
                add_file = True
            if add_file:
                change_list.append( { "action" : parts[0], "file" : parts[1] } )
            else:
                m = "found change %s but was not under %s" % (str(parts),self.config['migration_home'])
                self.logger.debug(m)
        self.logger.debug(change_list)
        return rollback, change_list

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
        # ensure required status docs are there
        if not [s for s in mongo_status['status'] if s['_id']=='COMMIT']:
            mongo_status.append({'_id':'COMMIT','value':0})

        return mongo_status

    def __initialize_mongo_instance(self):
        """Initializes a mongoDB instance to work with mongrate"""
        self.logger.info("initializing status in MongoDB")
        mongo = self.__get_mongo_client()
        ts = datetime.datetime.now()
        init_doc = { '_id' : 'INITIALIZE', 'ts' : ts }
        try:
            # TODO: should we backup any existing status data?
            mongo[self.MONGRATE_DB][self.MONGRATE_STATUS_COLL].drop()
            wr = mongo[self.MONGRATE_DB][self.MONGRATE_STATUS_COLL].insert_one(init_doc)
            self.logger.debug('inserted status %s writeResult=%s' % (str(init_doc), str(wr)))
            commit_doc = { '_id' : "COMMIT", 'value' : 0, 'ts' : ts }
            wr = mongo[self.MONGRATE_DB][self.MONGRATE_STATUS_COLL].insert_one(commit_doc)
            self.logger.debug('inserted status %s writeResult=%s' % (str(commit_doc), str(wr)))
        except Exception as exp:
            self.logger.error(exp)
            raise

    def __update_mongo_mongrate_commit(self, commit):
        """Update status collection with info"""
        mongo = self.__get_mongo_client()
        try:
            q = { '_id' : 'COMMIT' }
            u = { '$set' : { 'value' : commit } }
            wr = mongo[self.MONGRATE_DB][self.MONGRATE_STATUS_COLL].update_one(q,u)
            self.logger.debug('update status %s %s writeResult=%s' % (str(q),str(u),str(wr)))
        except Exception as exp:
            self.logger.error(exp)
            raise

    def __update_mongo_status(self, message):
        """Update status collection with info"""
        mongo = self.__get_mongo_client()
        status_doc = { 'ts' : datetime.datetime.now(), "msg" : message }
        try:
            wr = mongo[self.MONGRATE_DB][self.MONGRATE_STATUS_COLL].insert_one(status_doc)
            self.logger.debug('inserted status %s writeResult=%s' % (status_doc, wr))
        except Exception as exp:
            self.logger.error(exp)
            raise


    def __get_mongo_client(self):
        if not hasattr(self,'mongo'):
            # TODO: add in extra auth parameters here!
            try:
                self.logger.debug("attempting connection MongoDB: " + self.config['masked_mongodb'])
                self.mongo = pymongo.MongoClient(self.config['mongodb'])
            except Exception as exp:
                self.logger.error(exp)
                raise
        return self.mongo

    def decorate_mongo_connection_string(self):
        """Adds in any runtime auth args to the connection string in the conf file."""
        got_user = self.args.user
        got_pwd = self.args.password
        need_to_fix_uri = False
        if self.args.user:
            self.logger.debug('got_user was True, attempting to fix MongoDB URI')
            need_to_fix_uri = True
        if self.args.password:
            self.logger.debug('got_pwd was True, attempting to fix MongoDB URI')
            need_to_fix_uri = True
        if not need_to_fix_uri:
            self.logger.debug('don\'t need to fix MongoDB URI')
            self.config['masked_mongodb']=self.config['mongodb']
            return
        cs = self.config['mongodb']
        self.config['original.mongodb'] = cs    # save off just in case
        parsed_cs = pymongo.uri_parser.parse_uri(cs)
        self.logger.debug('1 parsed_cs=%s' % parsed_cs)
        if self.args.user:
            parsed_cs['username']=self.args.user
            self.logger.debug('updated MongoDB URI with username from args')
        if self.args.password:
            parsed_cs['password']=self.args.password
            self.logger.debug('updated MongoDB URI with password from args')
        self.logger.debug('parsed_cs=%s' % parsed_cs)
        ncs = 'mongodb://'
        ncs += '%s:' % parsed_cs['username']
        ncs += '%s@' % parsed_cs['password']
        for n in parsed_cs['nodelist']:
            ncs += '%s:%s,' % (n[0],n[1])
        ncs = ncs[:-1] + '/'  #replace last comma with forward slash
        if parsed_cs['database']:
            ncs += '%s?' % parsed_cs['database']
        if self.args.authenticationDatabase:
            parsed_cs['options']['authSource']=self.args.authenticationDatabase
            self.logger.debug('updated MongoDB URI with authSource from authenticationDatabase argument')
        for k in parsed_cs['options']:
            ncs += '%s=%s&' % (k,parsed_cs['options'][k])
        ncs = ncs[:-1]      # trim trailing &
        self.config['mongodb']=ncs
        # mask password for any logging
        if len(self.config['mongodb'].split('@'))>0:
            parts = self.config['mongodb'].split('@')
            b = parts[0].split(':')
            b[2]="XXXXXXXXX"
            self.config['masked_mongodb']=':'.join(b)+'@'+parts[1]

        self.logger.debug('mongodb=%s' % self.config['masked_mongodb'])

    # this function fetchs the depedency info for the
    # set of migrations to be run
    # it then converts this into the format for the toposort
    # library
    # { 1 : { 2, 5 }, 5 : { 3, 7, 9 }, etc
    # flatten = [ 1,2,5,3,7,9 ]
    # https://pypi.python.org/pypi/toposort/1.0
    def __get_scripts_toposort(self,rollback=False):
        """Fetch scripts and dependecies (runAfter) and sort them"""
        mongo = self.__get_mongo_client()
        data = list(mongo['admin']['mongrate.scripts'].find({},{'runAfter':1}))
        self.logger.debug(data)
        dt = {}
        for d in data:
            if not 'runAfter' in d:
                d['runAfter']=[]
            #self.logger.debug(d)
            dt[str(d['_id'])]={str(x) for x in d['runAfter']}
        tsort = toposort_flatten(dt)
        if rollback:
            tsort.reverse()
        return tsort

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
        eval_string = "mongrate = %s;" % (self.__get_mongrate_util_object_on_load(script))
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

    def __run_script(self,script,rollback=False):
        """Run the up() or down() function of a script based on the _id of the migration, return True if OK, False if Error"""
        self.logger.debug("__run_script called for '"+script+"'")
        shell_args = []
        shell_args.append("mongo")
        # TODO: deal with auth creds given from mongrate args
        shell_args.append(self.config['mongodb'])
        eval_string = "mongrate = %s;" % (self.__get_mongrate_util_object_up_or_down(script,rollback))
        #eval_string += "db=db.getSiblingDB('%s');" % self.MONGRATE_DB
        #eval_string += "load('%s');" % (script)
        #eval_string += "printjson(mongrate);"
        eval_string += "eval('mongrate.tryFunc = ' + mongrate.tryFunc);"
        eval_string += "mongrate.tryFunc();"
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
    def __get_mongrate_util_object_on_load(self,script):
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
            if ( r.getWriteError() ) {
                throw r.getWriteError().errmsg;
            }

        }"""
        try_load = try_load.replace('\"','')
        m['tryLoad'] = try_load % (script,script,script,self.MONGRATE_DB,self.MONGRATE_WORKING_SCRIPT_COLL)
        self.mongrate['tryLoad']=m['tryLoad']
        return json.dumps(self.mongrate)

    def __get_mongrate_util_object_up_or_down(self,script,rollback=False):
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
        # do same for up() and down() build wrapper try_ methods
        try_func ="""function() {
            var mig = db.getSiblingDB('%s').getCollection('%s').findOne( { '_id' : '%s' } );
            if ( mig==undefined ) {
                throw 'Unable to find script with _id = \\'%s\\' in db.%s.%s';
            }
            try {
                print('Calling %s() for %s');
                mig.%s();
                print('%s() for %s complete');
            } catch(error) {
                print(error);
                throw error
            }
        }"""
        func = 'up'
        if rollback:
            func = 'down'
        try_func = try_func.replace('\"','')
        d = self.MONGRATE_DB
        c = self.MONGRATE_WORKING_SCRIPT_COLL
        m['tryFunc'] = try_func % (d,c,script,script,d,c,func,script,func,func,script)
        self.mongrate['tryFunc']=m['tryFunc']
        return json.dumps(self.mongrate)

    def __clean_stored_migrations(self):
        mongo = self.__get_mongo_client()
        mongo[self.MONGRATE_DB][self.MONGRATE_WORKING_SCRIPT_COLL].drop()

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
    description = u'mongrate - a MongoDB migration \U0001F528 \U0001F415 \U0001F3CB \U0001F3D1'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-a","--action",default="status"
                        ,help='Action to perform. status, migrate, generate_migration, default is \'status\'')
    parser.add_argument("-f","--config",default="./mongrate.conf",help='Configuration file see docs')
    parser.add_argument("--git-commit",help="git tag/branch/commit hash to migrate to")
    parser.add_argument("--distributionCenter",help="name of distribution center folder to run along with common migrations")
    parser.add_argument("--migration-id",help="id of migration to generate template")
    parser.add_argument("-u","--user",help="user name for MongoDB connection, overrides conf connection string")
    parser.add_argument("-p","--password",help="password for MongoDB connection, overrides conf connection string")
    parser.add_argument("--authenticationDatabase",help="user source, --user and --password are required for this argument to be applied")
    parser.add_argument("--dry-run",action='store_true',default=False
                        ,help='Only show what would have been done, don\'t actually do anything')
    parser.add_argument("--force",action='store_true',default=False
                        ,help='Force an action, override any internal checks')
    parser.add_argument("--verbose",action='store_true',default=False
                        ,help='Enable more verbose logging')
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
    logger.info(description)
    logger.info('mongrate startup')
    logger.debug("args: " + str(args))
    logger.debug("config: " + str(config))
    logger.info("log level set to " + logging.getLevelName(logger.getEffectiveLevel()))
    logger.info("--dry-run is " + str(args.dry_run))
    mongrate = Mongrate(config, args, logger)
    logger.info('Mongrate initialized, attempt to perform ' + args.action + ' action')
    mongrate.act(args.action)

if __name__ == '__main__':
    main()



