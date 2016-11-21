import sys
import os
import sqlite3
import logging

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

class LogMonitorAPI(object):

    def __init__(self,sqlfile):
        self.sqlfile = sqlfile

        logName = 'logMonitor'
        logColumns = {
            'file_name': 'TEXT',
            'module' : 'TEXT',
            'severity' : 'TEXT',
            'count' : 'INTEGER',
            'log_key' : 'TEXT',
        }
        fileName = 'processedFiles'
        fileColumns = {
            'file_name': 'TEXT',
        }
        if not os.path.isfile(self.sqlfile):
            self.__create(logName,**logColumns)
            self.__create(fileName,**fileColumns)

    def __execute(self,command):
        logging.debug(command)
        conn = sqlite3.connect(self.sqlfile)
        c = conn.cursor()
        c.execute(command)
        conn.commit()
        conn.close()

    def __executeReturn(self,command):
        logging.debug(command)
        conn = sqlite3.connect(self.sqlfile)
        c = conn.cursor()
        c.execute(command)
        result = c.fetchall()
        conn.commit()
        conn.close()
        return result

    def __create(self,tableName,**columns):
        command = 'CREATE TABLE {table} ({columns})'.format(table=tableName,columns=', '.join([' '.join([key,val]) for key,val in columns.iteritems()]))
        self.__execute(command)

    def __insert(self,tableName,**kwargs):
        columns = []
        values = []
        for col,val in kwargs.iteritems():
            columns += [col]
            values += ["'{0}'".format(val) if isinstance(val,basestring) else "{0}".format(val)]
        command = 'INSERT INTO {table} ({columns}) VALUES ({values})'.format(
            table=tableName,
            columns=', '.join(columns),
            values=', '.join(values)
        )
        self.__execute(command)

    def __select(self,tableName,*columns,**conditions):
        conds = []
        for cond,val in conditions.iteritems():
            if isinstance(val,basestring): conds += ["{0} LIKE '{1}'".format(cond,val.replace('*','%')) if '*' in val else "{0}='{1}'".format(cond,val)]
        command = 'SELECT {columns} FROM {table}'.format(
            columns=', '.join(columns) if columns else '*',
            table=tableName,
        )
        if conds:
            command += ' WHERE {conditions}'.format(
                conditions=' AND '.join(conds)
            )
        result = self.__executeReturn(command)
        return result
        
    ###################
    ### Insert data ###
    ###################
    def insertModule(self,**kwargs):
        self.__insert('logMonitor',**kwargs)

    def insertProcessedFile(self,**kwargs):
        self.__insert('processedFiles',**kwargs)

    ##################
    ### query data ###
    ##################
    def listModules(self,**kwargs):
        return self.__select('logMonitor',**kwargs)

    def listProcessedFiles(self,**kwargs):
        return self.__select('processedFiles',**kwargs)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    # simple setup
    sqlfile = 'test.sqlite'
    if os.path.exists(sqlfile): os.remove(sqlfile)
    api = LogMonitorAPI(sqlfile)
    api.insertModule(file_name='dummy',module='mod1',severity='INFO',count=1,log_key='ModErrorType')
    api.insertModule(file_name='dummy',module='mod2',severity='WARNING',count=2,log_key='ModErrorType')
    api.insertProcessedFile(file_name='dummy')

    # query test
    print api.listModules(file_name='dummy')
    print api.listProcessedFiles()

if __name__ == "__main__":
    status = main()
    sys.exit(status)
