#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################################
## This file is part of Tango Control System:  http://www.tango-controls.org/
##
## $Author: Sergi Rubio Manrique, srubio@cells.es
## copyleft :    ALBA Synchrotron Controls Section, www.cells.es
##
## Tango Control System is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as published
## by the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## Tango Control System is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""
PyTangoArchiving.schemas: This module provides the Schemas object; 
a singleton to detect and manage multiple archiving schemas.
"""

import traceback,time,re
import fandango as fn
from fandango import clmatch, time2str, str2time

from PyTangoArchiving.utils import parse_property, overlap

import sys
EXPERT_MODE = True
    #any(a in str(sys.argv) for a in 
        #('ArchivingBrowser.py','ctarchiving','taurustrend',
         #'taurusfinder','ctsearch','ipython', 'test','-c',
         #'archiving2csv','archiving2plot','matlab','PyExtractor'))
         
class SchemaDict(dict): #fn.Struct):
    
    ## The .get method seems to work differently in Struct and Dict!
    
    #def __init__(self,other,load=False):
        #super(SchemaDict,self).__init__(**other)
        #self._load = load
    
    def __getitem__(self,key):
        
        if key=='dbname':
            v = super(SchemaDict,self).get('db_name',None)
            if v is None:
                v = super(SchemaDict,self).get('schema',None)
            return v

        if key=='reader':
            r = super(SchemaDict,self).get('reader',None)
            #print('%s.get(%s)' % (self['schema'],key))
            
            # if self._load and 
            if isinstance(r,str):
                self['reader'] = Schemas.getReader(self['schema'],self.copy())
                #print(self['reader'])
                
        return super(SchemaDict,self).__getitem__(key)
    
    def get(self,key,default=None):
        try:
            return self.__getitem__(key)
        except:
            return default
    
        
class Schemas(object):
    """ Schemas kept in a singleton object """
    
    SCHEMAS = fn.SortedDict()
    #Limited access to fandango library
    MODULES = {'fandango':fn.functional,'fn':fn.functional,'fun':fn.functional} 
    LOCALS = fn.functional.__dict__.copy()
    
    def __init__(self):
        self.load()
    
    @classmethod
    def __contains__(k,o):
        return o in k.SCHEMAS.keys()
    
    @classmethod
    def keys(k):
        if not k.SCHEMAS: 
            k.load()
        return k.SCHEMAS.keys()
    
    @classmethod
    def values(k):
        if not k.SCHEMAS: 
            k.load()
        return k.SCHEMAS.values()
    
    @classmethod
    def items(k):
        if not k.SCHEMAS: 
            k.load()
        return k.SCHEMAS.items()
    
    @classmethod
    def __iter__():
        """ TODO: iter() does not work in classes!"""
        return k.SCHEMAS.__iter__()
    
    @classmethod
    def __contains__(k,key):
        return k.SCHEMAS.__contains__(key)
    
    @classmethod
    def __getitem__(k,key):
        return k.SCHEMAS.__getitem__(key)
    
    @classmethod
    def get(k,key,default=None):
        return k.SCHEMAS.get(key,default)
    
    @classmethod
    @fn.Catched
    @fn.Cached(expire=60.)
    def load(k,tango='',prop='',logger=None):

        tangodb = fn.tango.get_database(tango)
        schemas = prop or tangodb.get_property('PyTangoArchiving',
                    ['DbSchemas','Schemas'])

        pname = 'DbSchemas' if 'DbSchemas' in schemas else 'Schemas'
        schemas = schemas.get(pname,[])

        if not schemas:
            schemas = ['tdb','hdb']
            tangodb.put_property('PyTangoArchiving',{'DbSchemas':schemas})

        print('Loading %s from tango@%s ... ' % (pname, tangodb.get_db_host()))

        [k.getSchema(schema,tango,write=True,logger=logger) 
            for schema in schemas]

        return k.SCHEMAS
    
    @classmethod
    def pop(k,key):
        k.SCHEMAS.pop(key)
    
    @classmethod
    def _load_object(k,obj,dct):
        rd = obj
        m = rd.split('(')[0].rsplit('.',1)[0]
        c = rd[len(m)+1:]
        if m not in k.MODULES:
            fn.evalX('import %s'%m,modules=k.MODULES)
        #print('getSchema(%s): load %s reader'%(schema,dct.get('reader')))
        return fn.evalX(obj, modules=k.MODULES, _locals=dct)
    
    @classmethod
    def getReader(k,schema,dct=None):
        # This method initializes a reader object from Schema config
        # It does not update the Schema object, just returns a reader
        
        dct = dct if dct is not None else k.getSchema(
            schema if fn.isString(schema) else schema.get('schema'))
        rd = dct.get('reader',dct.get('api'))

        if rd and isinstance(rd,str):
            try:
                #print('Schemas.getReader(%s): instantiating reader' % schema)
                
                rd = k._load_object(rd,dct)
                #print('getReader(%s): %s' % (schema,type(rd)))
                if not hasattr(rd,'is_attribute_archived'):
                    rd.is_attribute_archived = lambda *a,**k:True
                if not hasattr(rd,'get_attributes'):
                    rd.get_attributes = lambda *a,**k:[]
                if not hasattr(rd,'get_attribute_values'):
                    if dct['method']:
                        rd.get_attribute_values = getattr(rd,dct['method'])
                if not hasattr(rd,'schema'):
                    rd.schema = schema
            except:
                print('getReader(%s) failed!' % schema)
                #traceback.print_exc()
                rd = None
        
        return rd
        
    
    @classmethod
    def getSchema(k,schema,tango='',prop='',logger=None, write=False):

        if schema.startswith('#') and EXPERT_MODE:
            schema = schema.strip('#')
            print('%s is enabled'%schema)

        if schema in k.SCHEMAS:
            # Failed schemas should be also returned (to avoid unneeded retries)
            return k.SCHEMAS[schema]
        
        dct = {'match':clmatch,'clmatch':clmatch}
        if ';' in schema:
            schema,dct = schema.split(';',1)
            dct = dict(d.split('=',1) for d in dct.split(';'))
        dct['schema'] = schema
        dct = SchemaDict(dct)
        props = []

        try:
            tango = fn.tango.get_database(tango)
            props = prop or tango.get_property('PyTangoArchiving',
                                               schema)[schema]
            assert len(props)
            if fn.isSequence(props):
                props = dict(map(str.strip,t.split('=',1)) for t in props)
            if 'check' in dct:
                props.pop('check')
            dct.update(props)
            dct['logger'] = logger

        except Exception as e:
            print('getSchema(%s): failed!'%schema)
            print(dct,props)
            exc = traceback.format_exc()
            try: 
                logger.warning(exc)
            except: 
                print(exc)
            dct = None
        
        if write:
            k.SCHEMAS[schema] = dct

        return dct
    
    @classmethod
    def getSchemasForAttribute(attr,start=0,stop=fn.END_OF_TIME):
        """
        returns a fallback schema chain for the given dates
        """
        return [s for s in k.SCHEMAS if k.checkSchema(s,attr,start,stop)]
        
    
    @classmethod
    def checkSchema(k, schema, attribute='', start=None, stop=None):
        if not isinstance(schema, SchemaDict):
            schema = k.getSchema(schema)
        if not schema: 
            return False
        
        f = schema.get('check')
        if not f: 
            print('%s has no check function' % str(schema))
            return True

        try:
            now = time.time()
            start = (str2time(start) if fn.isString(start) 
                     else fn.notNone(start,now-1))
            stop = (str2time(stop) if fn.isString(stop) 
                    else fn.notNone(stop,now))
            xmatch = lambda e,a: clmatch(e,a,extend=True)
            k.LOCALS.update({
                    'attr':attribute.lower(),
                    'attribute':attribute.lower(),
                    'device':attribute.lower().rsplit('/',1)[0],
                    'match':lambda r: xmatch(r,attribute),
                    'clmatch':xmatch,
                    'overlap':overlap,
                    'time2str':time2str,'str2time':str2time,
                    't2s':time2str,'s2t':str2time,
                    'start':start,'stop':stop,'now':now,
                    'begin':start,'end':stop,'NOW':now,
                    'reader':schema.get('reader',schema.get('api')),
                    'schema':schema.get('schema'),
                    'dbname':schema.get('dbname',
                        schema.get('db_name',schema.get('schema',''))),
                    })
            if 'reader' in f:
                k.getReader(schema.get('schema'))
            if 'api' in f:
                k.getApi(schema.get('schema'))
                
            #print('In reader.Schemas.checkSchema(%s,%s,%s,%s): %s'
                #% (schema,attribute,start,stop,f))                
            #print('(%s)%%(%s)'%(f,[t for t in k.LOCALS.items() if t[0] in f]))
            v =fn.evalX(f,k.LOCALS,k.MODULES)
        except:
            print('checkSchema(%s,%s) failed!' % (schema,attribute))
            traceback.print_exc()
            v = False

        #print('checkSchema(%s): %s'%(schema,v))
        return v
  
    @classmethod
    def getApi(k,schema):
        if fn.isString(schema):
            schema = k.getSchema(schema)
            if schema is not None:
                api = schema.get('api','PyTangoArchiving.ArchivingAPI')
                if fn.isString(api): 
                    api = k._load_object(api,schema)
                return api(schema['schema']) if isinstance(api,type) else api
        else:
            return schema
        


