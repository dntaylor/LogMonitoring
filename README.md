# LogMonitoring
LogWarning and LogError monitoring for RECO workflows with CMS.

The utilities here require an environment with the DBS client available.
This can be achieved by sourcing a crab environment.
```bash
source /cvmfs/cms.cern.ch/crab3/crab.sh
```

## LogMonitorAPI.py
Command line API access for the LogMonitor SQL tables.

Two tables are available:
* logMonitor: stores the relation between the file_name, module, log_key, severity, count
* processedFiles: stores the relation between dataset, file_name for the processed files

Unit tests can be run by
```bash
python LogMonitorAPI.py
```

## logMonitor.py
Command line utility to process log files.

```bash
./logMonitor.py -h
```

There are two types of log files produced during CMS workflows. These can be accessed via the command line arguments:

* `data`

   The log files produced during PromptReco by the Tier0 are stored in a USER dataset matching `/*/*LogErrorMonitor*/USER`.
   
* `relval`

   The log files produced during central production (RelVal workflows, ReReco, MC production) are stored in text files archives.
   They are located in `/store/logs/prod/{year}/{month:02d}/WMAgent`.
   In addition, data PromptReco are temporarily located in `/store/unmerged/data/logs/prod/{year}/{month:02d}/{day}`.
   The relval portion can be run via either the `--dataset` or `--request` arguments.
   `--dataset` will search DBS to find the dataset name and then search ReqMgr to find the matching request.
   `--request` will search RegMgr directly.
   
The logs will be processed and statistics on the frequency of LogErrors and LogWarnings will be stored in the datasbase.

## getReport.py
Utility to summarized the observed counts of LogErrors and LogWarnings matching a search criteria.

```bash
./getReport.py -h
```

Currently supports search by `dataset`, `module`, `severity`, `log_key`, `run_num`. Can be expanded to support any DBS search key.

## logMonitor_web.py
A CherryPy application to provide web search of database.

A running example can be seen at [dntaylor-test.cern.ch/logMonitor](dntaylor-test.cern.ch/logMonitor). Note, this is only viewable from within the CERN network.

The `query` form supports queries of the form `key=val` with support for Unix-like wildcard replacements.
The supported keys are: `dataset`, `module`, `log_key`, `severity`, and DBS keys.
The output will be a table of `log_key`, `module` pairs with associated counts, separated by dataset and severity.

To run the server:
```bash
python logMonitor_web.py
```
