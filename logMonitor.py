#!/usr/bin/env python
import os
import sys
import time
import logging
import argparse
import json
import itertools
import fnmatch
import subprocess
import datetime
import tarfile
import socket
from LogMonitorAPI import LogMonitorAPI

import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True

ROOT.gSystem.Load("libFWCoreFWLite.so")
ROOT.gSystem.Load("libDataFormatsFWLite.so")
ROOT.FWLiteEnabler.enable()

from DataFormats.FWLite import Handle, Events

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# DBS modules
try:
    from dbs.apis.dbsClient import DbsApi
    from RestClient.RestApi import RestApi
    from RestClient.AuthHandling.X509Auth import X509Auth
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

class ReqMgrApi(object):

    def __init__(self,url=''):
        self.rest_api = RestApi(auth=X509Auth(ssl_cert=None, ssl_key=None, ssl_verifypeer=True, ca_info=None),proxy=None)
        self.url = url

    def get(self,**params):
        content = 'application/json'
        UserID = os.environ['USER']+'@'+socket.gethostname()
        userAgent = ''
        try:
            UserAgent = "DBSClient/"+os.environ['DBS3_CLIENT_VERSION']+"/"+ userAgent
        except:
            UserAgent = "DBSClient/Unknown"+"/"+ userAgent
        request_headers =  {"Content-Type": content, "Accept": content, "UserID": UserID, "User-Agent":UserAgent }
        response = self.rest_api.get(self.url,'',params,{},request_headers)
        return json.loads(response.body)['result']

def getReqMgrClient():
    if not dbsLoaded:
        logging.error('You must source a crab environment to use ReqMgr API.\nsource /cvmfs/cms.cern.ch/crab3/crab.sh')

    reqmgr = 'https://cmsweb.cern.ch/reqmgr2/data/request'
    reqmgrClient = ReqMgrApi(reqmgr)
    return reqmgrClient

def getLogMonitorClient():
    sqlfile = 'logMonitor.sqlite'
    lmclient = LogMonitorAPI(sqlfile)
    return lmclient

def processLogErrorFile(lfn):
    allSeverities = {}
    try:
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
    except Exception as e:
        print e

    return allSeverities

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
        logging.info('{0} {1}'.format(dsname, len(files)))
        fnames = [f['logical_file_name'] for f in files]
        prevfiles = lmclient.listProcessedFiles(dataset=dsname)
        pfnames = [p['file_name'] for p in prevfiles]
        nfiles = len(fnames)
        for f,fname in enumerate(fnames):
            if fname in pfnames:
                logging.info('{0}/{1} {2} already processed'.format(f+1, nfiles, fname))
                continue
            logging.info('{0}/{1} {2}'.format(f+1, nfiles, fname))
            lfn = 'root://{0}/{1}'.format(args.redirector,fname)
            allSeverities = processLogErrorFile(lfn)
            for severity in allSeverities:
                for error in allSeverities[severity]:
                    if error=='MemoryCheck': continue # manually skip MemoryCheck module
                    for mod in set(allSeverities[severity][error]['modules']):
                        lmclient.insertModule(file_name=fname,module=mod,severity=severity,log_key=error,count=allSeverities[severity][error]['modules'].count(mod))
            lmclient.insertProcessedFile(file_name=fname,dataset=dsname)


def parseFrameworkJobReport(content):
    return
    print content
    
def parseLogFile(results,content):
    for line in content:
        if 'MemoryCheck:' in line: continue
        if 'MSG-e' in line: # error
            severity = 'Error'
        elif 'MSG-w' in line: # warning
            severity = 'Warning'
        else:
            continue
        components = line.strip().split()
        log_key = components[1].strip(':') if len(components)>1 else 'unknown'
        module = components[2] if len(components)>2 else 'unknown'
        if severity not in results:
            results[severity] = {}
        if log_key not in results[severity]:
            results[severity][log_key] = {
                'modules': [],
            }
        results[severity][log_key]['modules'] += [module]
    return results
    
def processRequest(args,request):
    lmclient = getLogMonitorClient()

    unmergedLogDir = '/store/unmerged/data/logs/prod/{year}/{month:02d}/{day}'
    logDir = '/store/logs/prod/{year}/{month:02d}/WMAgent'
    eos = '/afs/cern.ch/project/eos/installation/0.3.84-aquamarine/bin/eos.select'
    reqname = request.keys()[0]

    # guess the request directory
    reqdate = request[reqname]['RequestDate']
    year = reqdate[0]
    month = reqdate[1]
    day = reqdate[2]
    baseDir = logDir
    fullDir = baseDir.format(year=year,month=month,day=day)

    # get output logs
    ls_command = '{0} ls {1}/{2}'.format(eos,fullDir,reqname)
    lcfiles = process(ls_command)
    lcfiles = [x.strip() for x in lcfiles.split() if fnmatch.fnmatch(x.strip(),'{0}-LogCollect*'.format(reqname))]
    nfiles = len(lcfiles)

    # fall back to try one month later
    if nfiles==0:
        newmonth = int(month)+1
        if newmonth>12: newmonth -= 12
        fullDir = baseDir.format(year=year,month=newmonth,day=day)
        ls_command = '{0} ls {1}/{2}'.format(eos,fullDir,reqname)
        lcfiles = process(ls_command)
        lcfiles = [x.strip() for x in lcfiles.split() if fnmatch.fnmatch(x.strip(),'{0}-LogCollect*'.format(reqname))]
        nfiles = len(lcfiles)

    # associate output logCollect with datasetnames
    tasks = [v for k,v in request[reqname].iteritems() if k.startswith('Task') and isinstance(v,dict)]
    outputdatasets = {}
    for ds in request[reqname]['OutputDatasets']:
        for task in tasks:
            if task['ProcessingString'] in ds: outputdatasets[ds] = task['TaskName']

    # check for previously processed files
    prevfiles = []
    for ds in outputdatasets:
        prevfiles += lmclient.listProcessedFiles(dataset=ds)
    pfnames = set([p['file_name'] for p in prevfiles])

    # process found files
    logging.info('{0} {1}'.format(outputdatasets.keys()[0], nfiles))
    for l, lcfile in enumerate(lcfiles):
        results = {}
        lfn = '{0}/{1}/{2}'.format(fullDir,reqname,lcfile)
        xrdpath = 'root://{0}/{1}'.format(args.redirector,lfn)
        eospath = 'eos/cms/{0}'.format(lfn)
        if lfn in pfnames:
            logging.info('{0}/{1} {2} already processed'.format(l+1,nfiles,lfn))
            continue
        logging.info('{0}/{1} {2}'.format(l+1,nfiles,lfn))
        with tarfile.open(eospath) as tf:
            for member in tf.getmembers()[:1]:
                tfg_f = tf.extractfile(member)
                with tarfile.open(fileobj=tfg_f) as tfg:
                    for mem in tfg.getmembers():
                        filename, ftype = os.path.splitext(mem.name)
                        if ftype not in ['.log','.xml']: continue
                        if 'FrameworkJobReport' in filename:
                            log_f = tfg.extractfile(mem)
                            content = log_f.read()
                            parseFrameworkJobReport(content)
                        if 'stdout' in filename:
                            log_f = tfg.extractfile(mem)
                            content = log_f.readlines()
                            results = parseLogFile(results,content)
        for severity in results:
            for log_key in results[severity]:
                for mod in set(results[severity][log_key]['modules']):
                    lmclient.insertModule(file_name=lfn,module=mod,severity=severity,log_key=log_key,count=results[severity][log_key]['modules'].count(mod))
        for ds in outputdatasets:
            if outputdatasets[ds] in lfn: lmclient.insertProcessedFile(file_name=lfn,dataset=ds)

def relvalMonitor(args):
    '''Monitor script for relval requests'''
    dbsclient = getDBSClient()
    rmclient = getReqMgrClient()

    if args.request:
        res = rmclient.get(name=args.request)
        for request in res:
            processRequest(args,request)
    else:
        # get datasets
        datasets = dbsclient.listDatasets(dataset=args.dataset)

        for dataset in datasets:
            # find request name from reqmgr
            dsname = dataset['dataset']
            res = rmclient.get(outputdataset=dsname)
            request = res[0]
            processRequest(args,request)

# previous version
#def relvalMonitor(args):
#    '''Monitor script for relval requests'''
#    lmclient = getLogMonitorClient()
#
#    unmergedLogDir = '/store/unmerged/data/logs/prod/{year}/{month}/{day}'
#    logDir = '/store/logs/prod/{year}/{month}/WMAgent'
#    eos = '/afs/cern.ch/project/eos/installation/0.3.84-aquamarine/bin/eos.select'
#
#    baseDir = unmergedLogDir if args.unmerged else logDir
#    fullDir = baseDir.format(year=args.year,month=args.month,day=args.day)
#    ls_command = '{0} ls {1}'.format(eos,fullDir)
#
#    out = process(ls_command)
#    samples = [x.strip() for x in out.split() if fnmatch.fnmatch(x.strip(),args.workflow)]
#    for sample in samples:
#        prevfiles = lmclient.listProcessedFiles(dataset=sample)
#        pfnames = [p['file_name'] for p in prevfiles]
#        ls_command = '{0} ls {1}/{2}'.format(eos,fullDir,sample)
#        lcfiles = process(ls_command)
#        lcfiles = [x.strip() for x in lcfiles.split() if fnmatch.fnmatch(x.strip(),'{0}-LogCollect*'.format(sample))]
#        nfiles = len(lcfiles)
#        logging.infor('{0} {1}'.format(sample, nfiles))
#        for l, lcfile in enumerate(lcfiles):
#            results = {}
#            lfn = '{0}/{1}/{2}'.format(fullDir,sample,lcfile)
#            xrdpath = 'root://{0}/{1}'.format(args.redirector,lfn)
#            eospath = 'eos/cms/{0}'.format(lfn)
#            if lfn in pfnames:
#                logging.info('{0}/{1} {2} already processed'.format(l,nfiles,lfn))
#                continue
#            logging.info('{0}/{1} {2}'.format(l,nfiles,lfn))
#            with tarfile.open(eospath) as tf:
#                for member in tf.getmembers()[:1]:
#                    tfg_f = tf.extractfile(member)
#                    with tarfile.open(fileobj=tfg_f) as tfg:
#                        for mem in tfg.getmembers():
#                            filename, ftype = os.path.splitext(mem.name)
#                            if ftype not in ['.log','.xml']: continue
#                            if 'FrameworkJobReport' in filename:
#                                log_f = tfg.extractfile(mem)
#                                content = log_f.read()
#                                parseFrameworkJobReport(content)
#                            if 'stdout' in filename:
#                                log_f = tfg.extractfile(mem)
#                                content = log_f.readlines()
#                                results = parseLogFile(results,content)
#            for severity in results:
#                for log_key in results[severity]:
#                    for mod in set(results[severity][log_key]['modules']):
#                        lmclient.insertModule(file_name=lfn,module=mod,severity=severity,log_key=log_key,count=results[severity][log_key]['modules'].count(mod))
#            lmclient.insertProcessedFile(file_name=lfn,dataset=sample)

def parse_command_line(argv):
    parser = argparse.ArgumentParser(description='Log monitoring for RECO')

    parser.add_argument('--redirector',type=str,nargs='?',default='cms-xrd-global.cern.ch', help='Redirector for xrootd')

    subparsers = parser.add_subparsers(help='Log monitor mode')

    ################################
    ### data LogError monitoring ###
    ################################
    parser_data = subparsers.add_parser('data', help='Monitor data taking')

    # dataset to process
    dataset_full = parser_data.add_argument('--dataset', required=True, type=str, nargs='?', default='/Dummy/*LogErrorMonitor*/USER', help='Full dataset name')
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

    relval_source = parser_relval.add_mutually_exclusive_group(required=True)
    relval_dataset = relval_source.add_argument('--dataset', type=str, nargs='?', default='/Dummy/*RelVal*/RECO', help='Full dataset name')
    relval_request = relval_source.add_argument('--request', type=str, nargs='?', default='', help='Request name from ReqMgr')
    #parser_relval.add_argument('--workflow', type=str, nargs='?', default='*', help='RelVal workflow request')
    #parser_relval.add_argument('--year', type=str, nargs='?', default=now.year, help='Year to process')
    #parser_relval.add_argument('--month', type=str, nargs='?', default=now.month, help='Month to process')
    #parser_relval.add_argument('--day', type=str, nargs='?', default=now.day, help='Day to process')
    #parser_relval.add_argument('--unmerged', action='store_true', help='Use unmerged from T0 (data only)')

    parser_relval.set_defaults(submit=relvalMonitor)

    parser.add_argument('-l','--log',nargs='?',type=str,const='INFO',default='INFO',choices=['INFO','DEBUG','WARNING','ERROR','CRITICAL'],help='Log level for logger')

    return parser.parse_args(argv)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse_command_line(argv)

    loglevel = getattr(logging,args.log)
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s', level=loglevel, datefmt='%Y-%m-%d %H:%M:%S')

    return args.submit(args)

if __name__ == "__main__":
    status = main()
    sys.exit(status)
