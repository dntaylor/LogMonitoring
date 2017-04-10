from __future__ import print_function

import random
import string
import json
import cherrypy
from copy import deepcopy

from getReport import getReport


@cherrypy.expose
class HelloWorld(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self,**kwargs):
        name = kwargs.get('name',None)
        if name:
            return 'Hello {0}'.format(name)
        else:
            return 'Hello World!'

@cherrypy.expose
class LogMonitor(object):

    @cherrypy.expose
    def index(self,**kwargs):
        query = kwargs.get('query',None)
        if query:
            args = self.parse(query)
            result = getReport(**args)
            content = self.makeTable(json.loads(result),**args)
            return self.getPage(query,content)
        else:
            return self.getPage('','')

    def makeTable(self,content,**kwargs):
        result = ''
        for dataset,datasets in content.iteritems():
            result += '<h3>{0}</h3>'.format(dataset)
            result += '<br/>'
            for errorType,errors in datasets.iteritems():
                result += '<table>'
                result += '<tr>'
                result += '<th>{0}</th>'.format(errorType)
                result += '<th>Module</th>'
                result += '<th>Count</th>'
                result += '</tr>'
                for errorName,modules in errors.iteritems():
                    for moduleName,count in modules.iteritems():
                        result += '<tr>'
                        newargs = deepcopy(kwargs)
                        newargs.update({'log_key':errorName})
                        result += '<td><a href="{0}">{1}</a></td>'.format(self.makeURL(**newargs),errorName)
                        newargs = deepcopy(kwargs)
                        newargs.update({'module':moduleName})
                        result += '<td><a href="{0}">{1}</a></td>'.format(self.makeURL(**newargs),moduleName)
                        result += '<td>{0}</td>'.format(count)
                        result += '</tr>'
                result += '</table>'
                result += '<br/>'
        return result

    def makeURL(self,**kwargs):
        return '?query='+'+'.join(self.makeQuery(**kwargs).split(' '))
                        
    def makeQuery(self,**kwargs):
        return ' '.join(sorted(['{0}={1}'.format(key,val) for key,val in kwargs.iteritems()]))

    #@cherrypy.tools.accept(media='application/json')
    #def GET(self,**kwargs):
    #    dataset = kwargs.get('dataset',None)
    #    if dataset:
    #        return getReport(**kwargs)
    #        #return 'Error summary for dataset={0}'.format(dataset)
    #    return 'LogMonitor Home'

    def getForm(self,query):
        return '''
            <form method="get" action="">
              Query: <input type="text" size="80" value="{0}" name="query" />
              <button type="submit">Submit</button>
            </form>
        '''.format(query)

    def getPage(self,query,content):
        return """<html>
          <head></head>
          <body>
          {0}
          <br/>
          {1}
          </body>
        </html>""".format(self.getForm(query),content)

    def parse(self,query):
        # TODO: validate
        kwargs = {}
        components = query.split()
        for c, component in enumerate(components):
            if c==0 and '=' not in component: kwargs['sort'] = component
            if '=' in component:
                key,val = component.split('=')
                kwargs[key] = val
        return kwargs


if __name__ == '__main__':
    global_conf = {
        'server.socket_host': 'dntaylor-test.cern.ch',
        'server.socket_port': 80,
    }
    cherrypy.config.update(global_conf)

    logMonitor_conf = {
        '/': {
            #'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            #'tools.sessions.on': True,
            #'tools.response_headers.on': True,
            #'tools.response_headers.headers': [('Content-Type', 'application/json')],
        },
    }

    cherrypy.tree.mount(LogMonitor(), '/logMonitor', logMonitor_conf)

    #hello_conf = {
    #    '/': {
    #        'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
    #        'tools.sessions.on': True,
    #        'tools.response_headers.on': True,
    #        'tools.response_headers.headers': [('Content-Type', 'text/plain')],
    #    },
    #}
    #cherrypy.tree.mount(HelloWorld(), '/', hello_conf)

    cherrypy.engine.start()
    cherrypy.engine.block()
