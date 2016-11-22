import sys
import os
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stderr)

class LogMonitorAPI(object):

    def __init__(self,sqlfile):
        self.sqlfile = sqlfile

        logName = 'logMonitor'
        logColumns = {
            'file_name' : 'TEXT',
            'module' : 'TEXT',
            'log_key' : 'TEXT',
            'severity' : 'TEXT',
            'count' : 'INTEGER',
        }
        logUnique = ['file_name','module','log_key','severity']
        fileName = 'processedFiles'
        fileColumns = {
            'file_name' : 'TEXT',
            'dataset' : 'TEXT',
        }
        fileUnique = ['file_name']
        if not os.path.isfile(self.sqlfile):
            self.__create(logName,*logUnique,**logColumns)
            self.__create(fileName,*fileUnique,**fileColumns)

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
        conn.close()
        return result

    def __create(self,tableName,*unique,**columns):
        command = 'CREATE TABLE {table} ({columns}, UNIQUE({unique}))'.format(
            table=tableName,
            columns=', '.join([' '.join([key,val]) for key,val in columns.iteritems()]),
            unique=', '.join(unique)
        )
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
        try:
            self.__execute(command)
        except sqlite3.IntegrityError as e:
            logging.error(e)

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
        
    def __wrapResult(self,columns,result):
        newresult = []
        for r in result:
            nr = {}
            for k,v in zip(columns,r):
                nr[k] = v
            newresult += [nr]
        return newresult

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
        columns = ['file_name', 'module', 'log_key', 'severity', 'count']
        result = self.__select('logMonitor',*columns,**kwargs)
        return self.__wrapResult(columns,result)

    def listProcessedFiles(self,**kwargs):
        columns = ['file_name','dataset']
        result = self.__select('processedFiles',*columns,**kwargs)
        return self.__wrapResult(columns,result)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    logging.getLogger().setLevel(logging.DEBUG)

    # simple setup
    sqlfile = 'test.sqlite'
    if os.path.exists(sqlfile): os.remove(sqlfile)
    api = LogMonitorAPI(sqlfile)
    api.insertModule(file_name='dummy',module='mod1',severity='INFO',count=1,log_key='ModErrorType')
    api.insertModule(file_name='dummy',module='mod2',severity='WARNING',count=2,log_key='ModErrorType')
    api.insertProcessedFile(file_name='dummy',dataset='/a/b/c')

    # attempt to insert duplicate
    api.insertModule(file_name='dummy',module='mod1',severity='INFO',count=1,log_key='ModErrorType')

    # query test
    print api.listModules(file_name='dummy')
    print api.listProcessedFiles(dataset='/a/b/c')

if __name__ == "__main__":
    status = main()
    sys.exit(status)
