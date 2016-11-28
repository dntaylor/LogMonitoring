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

    url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    dbsclient = DbsApi(url)

    return dbsclient

def getLogMonitorClient():
    sqlfile = 'logMonitor.sqlite'
    lmclient = LogMonitorAPI(sqlfile)
    return lmclient

def generateReport(args):
    dbsclient = getDBSClient()
    lmclient = getLogMonitorClient()

    files = lmclient.listProcessedFiles(dataset=args.dataset)

    datasets = set([f['dataset'] for f in files])

    summary = {}
    for dataset in sorted(datasets):
        dfiles = [f['file_name'] for f in files if f['dataset']==dataset]
        dbsfiles = dfiles
        if args.run: dbsfiles = [f['logical_file_name'] for f in dbsclient.listFiles(dataset=dataset,run_num=args.run)]
        allResults = []
        for f in dfiles:
            if f not in dbsfiles: continue
            allResults += lmclient.listModules(file_name=f)
        for result in allResults:
            if dataset not in summary: summary[dataset] = {}
            severity = result['severity']
            logkey = result['log_key']
            module = result['module']
            count = result['count']
            if severity not in summary[dataset]: summary[dataset][severity] = {}
            if logkey not in summary[dataset][severity]: summary[dataset][severity][logkey] = {}
            if module not in summary[dataset][severity][logkey]: summary[dataset][severity][logkey][module] = 0
            summary[dataset][severity][logkey][module] += count
    print json.dumps(summary, indent=4, sort_keys=True)

def parse_command_line(argv):
    parser = argparse.ArgumentParser(description='Log monitoring for RECO')

    dataset_full = parser.add_argument('--dataset', type=str, nargs='?', default='/*/*LogErrorMonitor*/USER', help='Full dataset name')
    run = parser.add_argument('--run', type=str, nargs='*', default='', help='Runs to include in report')

    return parser.parse_args(argv)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse_command_line(argv)

    generateReport(args)

if __name__ == "__main__":
    status = main()
    sys.exit(status)
