#!/usr/bin/env python
import os
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

import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True

ROOT.gSystem.Load("libFWCoreFWLite.so")
ROOT.gSystem.Load("libDataFormatsFWLite.so")
ROOT.FWLiteEnabler.enable()

from DataFormats.FWLite import Handle, Events

logging.basicConfig(level=logging.INFO, stream=sys.stderr)

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

def dataMonitor(args):
    '''Monitor script for data LogError'''
    dbsclient = getDBSClient()
    lmclient = getLogMonitorClient()

    #kwargs = {}
    #if args.primaryDataset: kwargs['primary_ds_name'] = args.primaryDataset
    #if args.acquisitionEra: kwargs['acquisition_era_name'] = args.acquisitionEra
    #if args.dataTier: kwargs['data_tier_name'] = args.dataTier
    ##kwargs['detail'] = True
    #datasets = dbsclient.listDatasets(**kwargs)

    datasets = dbsclient.listDatasets(dataset=args.dataset)

    for dataset in datasets:
        dsname = dataset['dataset']
        files = dbsclient.listFiles(dataset=dsname)
        print dsname, len(files)
        fnames = [f['logical_file_name'] for f in files]
        prevfiles = lmclient.listProcessedFiles(dataset=dsname)
        pfnames = [p['file_name'] for p in prevfiles]
        for fname in fnames:
            if fname in pfnames:
                print fname, 'already processed'
                continue
            print fname
            allSeverities = {}
            lfn = 'root://{0}/{1}'.format(args.redirector,fname)
            events = Events(lfn)
            errorSummaryHandle = Handle('std::vector<edm::ErrorSummaryEntry>')
            errorSummaryLabel = ('logErrorHarvester')
            for i,event in enumerate(events):
                event.getByLabel(errorSummaryLabel,errorSummaryHandle)
                errorSummary = errorSummaryHandle.product()
                for es in errorSummary:
                    severity = es.severity.getName()
                    category = es.category
                    module = es.module
                    count = es.count
                    if severity not in allSeverities:
                        allSeverities[severity] = {}
                    if category not in allSeverities[severity]:
                        allSeverities[severity][category] = {
                            'count' : 0,
                            'modules' : [],
                        }
                    allSeverities[severity][category]['count'] += count
                    allSeverities[severity][category]['modules'] += [module]
            for severity in allSeverities:
                #print severity
                for error in allSeverities[severity]:
                    #print '    ', error, allSeverities[severity][error]['count'], len(set(allSeverities[severity][error]['modules']))
                    if error=='MemoryCheck': continue # manually skip MemoryCheck module
                    for mod in set(allSeverities[severity][error]['modules']):
                        lmclient.insertModule(file_name=fname,module=mod,severity=severity,log_key=error,count=allSeverities[severity][error]['modules'].count(mod))
            lmclient.insertProcessedFile(file_name=fname,dataset=dsname)

    
def relvalMonitor(args):
    '''Monitor script for relval requests'''
    unmergedLogDir = '/store/unmerged/data/logs/prod/{year}/{month}/{day}'
    logDir = '/store/logs/prod/{year}/{month}/WMAgent'
    eos = '/afs/cern.ch/project/eos/installation/0.3.84-aquamarine/bin/eos.select'

    baseDir = unmergedLogDir if args.unmerged else logDir
    fullDir = baseDir.format(year=args.year,month=args.month,day=args.day)
    ls_command = '{0} ls {1}'.format(eos,fullDir)

    out = process(ls_command)
    samples = [x.strip() for x in out.split() if fnmatch.fnmatch(x.strip(),args.workflow)]
    for sample in samples[:1]:
        print sample
        ls_command = '{0} ls {1}/{2}'.format(eos,fullDir,sample)
        lcfiles = process(ls_command)
        lcfiles = [x.strip() for x in lcfiles.split() if fnmatch.fnmatch(x.strip(),'{0}-LogCollect*'.format(sample))]
        for lcfile in lcfiles[:1]:
            lfn = 'root://{0}/{1}/{2}/{3}'.format(args.redirector,fullDir,sample,lcfile)
            lfn = 'eos/cms/{1}/{2}/{3}'.format(args.redirector,fullDir,sample,lcfile)
            print '    ',lfn
            with tarfile.open(lfn) as tf:
                for member in tf.getmembers():
                    print '        ',member
                    tfg_f = tf.extractfile(member)
                    with tarfile.open(fileobj=tfg_f) as tfg:
                        for mem in tfg.getmembers():
                            print '            ',mem

def parse_command_line(argv):
    parser = argparse.ArgumentParser(description='Log monitoring for RECO')

    parser.add_argument('--redirector',type=str,nargs='?',default='cms-xrd-global.cern.ch', help='Redirector for xrootd')

    subparsers = parser.add_subparsers(help='Log monitor mode')

    ################################
    ### data LogError monitoring ###
    ################################
    parser_data = subparsers.add_parser('data', help='Monitor data taking')

    # dataset to process
    dataset_full = parser_data.add_argument('--dataset', type=str, nargs='?', default='/*/*LogErrorMonitor*/USER', help='Full dataset name')
    #dataset_components = parser_data.add_argument_group(description='Dataset components')
    #dataset_components.add_argument('--primaryDataset', type=str, nargs='?', default='*', help='Primary dataset names')
    #dataset_components.add_argument('--acquisitionEra', type=str, nargs='?', default='Run2016*', help='Acquisition era for dataset')
    #dataset_components.add_argument('--dataTier', type=str, nargs='?', default='USER', help='Data tier for dataset')

    parser_data.set_defaults(submit=dataMonitor)

    #########################
    ### relval monitoring ###
    #########################
    parser_relval = subparsers.add_parser('relval', help='Monitor relval workflows')

    now = datetime.datetime.now()

    parser_relval.add_argument('--workflow', type=str, nargs='?', default='*', help='RelVal workflow request')
    parser_relval.add_argument('--year', type=int, nargs='?', default=now.year, help='Year to process')
    parser_relval.add_argument('--month', type=int, nargs='?', default=now.month, help='Month to process')
    parser_relval.add_argument('--day', type=int, nargs='?', default=now.day, help='Day to process')
    parser_relval.add_argument('--unmerged', action='store_true', help='Use unmerged from T0 (data only)')

    parser_relval.set_defaults(submit=relvalMonitor)

    return parser.parse_args(argv)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse_command_line(argv)

    return args.submit(args)

if __name__ == "__main__":
    status = main()
    sys.exit(status)
