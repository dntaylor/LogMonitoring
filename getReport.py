#!/usr/bin/env python
import os
import json
import sys
import time
import logging
import argparse
import itertools
import fnmatch
import subprocess
import datetime
import tarfile
from LogMonitorAPI import LogMonitorAPI

logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# DBS modules
try:
    from dbs.apis.dbsClient import DbsApi
    dbsLoaded = True
except:
    dbsLoaded = False

def process(command):
    return subprocess.Popen(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT).communicate()[0]

def getDBSClient():

    if not dbsLoaded:
        logging.error('You must source a crab environment to use DBS API.\nsource /cvmfs/cms.cern.ch/crab3/crab.sh')
        return False

    url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    dbsclient = DbsApi(url)

    return dbsclient

def getLogMonitorClient():
    sqlfile = 'logMonitor.sqlite'
    lmclient = LogMonitorAPI(sqlfile)
    return lmclient

def generateReport(**kwargs):
    # remove arguments supported by log monitor, the rest are passed to dbs api
    dataset = kwargs.pop('dataset','/*/*/*')
    severity = kwargs.pop('severity','*')
    module = kwargs.pop('module','*')
    log_key = kwargs.pop('log_key','*')

    # setup clients
    dbsclient = getDBSClient()
    lmclient = getLogMonitorClient()

    # get processed files matching dataset
    files = lmclient.listProcessedFiles(dataset=dataset)

    datasets = set([f['dataset'] for f in files])
    
    # iterate through processed datasets to create summary object
    summary = {}
    for dataset in sorted(datasets):
        dfiles = [f['file_name'] for f in files if f['dataset']==dataset]
        dbsfiles = dfiles
        if kwargs and dbsLoaded: dbsfiles = [f['logical_file_name'] for f in dbsclient.listFiles(dataset=dataset,**kwargs)]
        allResults = []
        for f in dfiles:
            if f not in dbsfiles: continue
            allResults += lmclient.listModules(file_name=f,severity=severity,module=module,log_key=log_key)
        for result in allResults:
            if dataset not in summary: summary[dataset] = {}
            sev = result['severity']
            lk = result['log_key']
            mod = result['module']
            count = result['count']
            if sev not in summary[dataset]: summary[dataset][sev] = {}
            if lk not in summary[dataset][sev]: summary[dataset][sev][lk] = {}
            if mod not in summary[dataset][sev][lk]: summary[dataset][sev][lk][mod] = 0
            summary[dataset][sev][lk][mod] += count
    return json.dumps(summary, indent=4, sort_keys=True)

def getReport(**kwargs):
    allowed = [
        'dataset', 
        'severity',
        'module',
        'log_key',
        'parent_dataset', 
        'release_version', 
        'pset_hash', 
        'app_name', 
        'output_module_label', 
        'global_tag', 
        'processing_version', 
        'acquisition_era_name', 
        'run_num', 
        'physics_group_name', 
        'logical_file_name', 
        'primary_ds_name', 
        'primary_ds_type', 
        'processed_ds_name', 
        'data_tier_name', 
        'dataset_access_type', 
        'prep_id', 
        'create_by', 
        'last_modified_by', 
        'min_cdate', 
        'max_cdate', 
        'min_ldate', 
        'max_ldate', 
        'cdate', 
        'ldate', 
        'detail', 
        'dataset_id',
    ]
    valid = True
    response = ''
    for key,val in kwargs.iteritems():
        if key not in allowed:
            response += 'Unknown parameter: {0}={1}\n'.format(key,val)
            valid = False
    if valid:
        response = generateReport(**kwargs)
    else:
        response += 'Valid keys are: {0}'.format(allowed)
    return response


def parse_command_line(argv):
    parser = argparse.ArgumentParser(description='Log monitoring for RECO')

    dataset_full = parser.add_argument('--dataset', type=str, nargs='?', default='/*/*LogErrorMonitor*/USER', help='Full dataset name')
    dataset_full = parser.add_argument('--severity', type=str, nargs='?', default='*', help='Severity level')
    dataset_full = parser.add_argument('--module', type=str, nargs='?', default='*', help='Module names')
    dataset_full = parser.add_argument('--log_key', type=str, nargs='?', default='*', help='Log key')
    run = parser.add_argument('--run_num', type=str, nargs='*', default='', help='Runs to include in report')

    return parser.parse_args(argv)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse_command_line(argv)

    print getReport(**vars(args))

if __name__ == "__main__":
    status = main()
    sys.exit(status)
