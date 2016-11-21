#!/usr/bin/env python
import os
import sys
import time
import logging
import argparse
import itertools
import fnmatch

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

def client():

    if not dbsLoaded:
        logging.error('You must source a crab environment to use DBS API.\nsource /cvmfs/cms.cern.ch/crab3/crab.sh')
        return

    url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    dbsclient = DbsApi(url)

    return dbsclient


def dataMonitor(args):
    '''Monitor script for data LogError'''
    dbsclient = client()

    kwargs = {}
    if args.primaryDataset: kwargs['primary_ds_name'] = args.primaryDataset
    if args.acquisitionEra: kwargs['acquisition_era_name'] = args.acquisitionEra
    if args.dataTier: kwargs['data_tier_name'] = args.dataTier
    #kwargs['detail'] = True
    datasets = dbsclient.listDatasets(**kwargs)

    for dataset in datasets:
        dsname = dataset['dataset']
        files = dbsclient.listFiles(dataset=dsname)
        print dsname, len(files)
        fnames = [f['logical_file_name'] for f in files]
        for fname in fnames[:1]:
            allSeverities = {}
            lfn = 'root://{0}/{1}'.format(args.redirector,fname)
            print lfn
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
                print severity
                for error in allSeverities[severity]:
                    print '    ', error, allSeverities[severity][error]['count'], len(set(allSeverities[severity][error]['modules']))

    
def relvalMonitor(args):
    '''Monitor script for relval requests'''
    return


def parse_command_line(argv):
    parser = argparse.ArgumentParser(description='Log monitoring for RECO')

    parser.add_argument('--redirector',type=str,nargs='?',default='cms-xrd-global.cern.ch', help='Redirector for xrootd')

    subparsers = parser.add_subparsers(help='Log monitor mode')

    ################################
    ### data LogError monitoring ###
    ################################
    parser_data = subparsers.add_parser('data', help='Monitor data taking')

    # dataset to process
    dataset_components = parser_data.add_argument_group(description='Dataset components')
    dataset_components.add_argument('--primaryDataset', type=str, nargs='?', default='*', help='Primary dataset names')
    dataset_components.add_argument('--acquisitionEra', type=str, nargs='?', default='Run2016*', help='Acquisition era for dataset')
    dataset_components.add_argument('--dataTier', type=str, nargs='?', default='USER', help='Data tier for dataset')

    parser_data.set_defaults(submit=dataMonitor)

    #########################
    ### relval monitoring ###
    #########################
    parser_relval = subparsers.add_parser('relval', help='Monitor relval workflows')

    parser_relval.add_argument('--workflow', type=str, nargs='?', default='*', help='RelVal workflow request')

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
