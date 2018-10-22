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
PyTangoArchiving.reader: This module provides the Reader object; 
a lightweigth api for archiving clients and/or scripts.
"""

import traceback,time,os,re
from random import randrange
from collections import defaultdict

import fandango
from fandango.objects import Object,SingletonMap,Cached
from fandango.log import Logger
import fandango.functional as fun

from fandango.dicts import CaselessDict, SortedDict
from fandango.linos import check_process,get_memory
from fandango.tango import ( get_tango_host,parse_tango_model, get_full_name,
    get_normal_name, get_free_property,get_class_property,get_device_property)

from PyTangoArchiving.utils import * #PyTango, patch_booleans
import PyTangoArchiving.utils as utils

from PyTangoArchiving.dbs import ArchivingDB, get_table_name
from PyTangoArchiving.common import DB_MODES, translate_attribute_modes
from PyTangoArchiving.schemas import Schemas
import MySQLdb,MySQLdb.cursors,datetime

__test__ = {}

STARTUP = time.time()
def getArchivedTrendValues(*args,**kwargs):
    """ For backwards compatibility, preload TaurusTrend layer """
    try:
        from PyTangoArchiving.widget.trend import getArchivedTrendValues
        return getArchivedTrendValues(*args,**kwargs)
    except:
        traceback.print_exc()
        return []

###############################################################################
# Helpers
    
def expandEvalAttribute(attribute):
    if '{' not in attribute: return []
    else: return [a.strip('{}') for a in re.findall('[\{][^\{]*[\}]',attribute)]
                
def isAttributeArchived(attribute,reader=None,schema=''):
    """
    This method returns whether an attribute contains values or not in the database.
    The attribute could be no longer archived, but if there's data to retrieve 
    it will return True
    """
    reader = reader or Reader(schema)
    reader.debug('In PyTangoArchiving.reader.isAttributeArchived(%s)'%attribute)
    try:
      if expandEvalAttribute(attribute):
          return all(isAttributeArchived(a) for a in expandEvalAttribute(a))
      attribute = reader.alias.get(attribute,attribute)
      attribute = attribute.lower().split('[')[0]

      if not reader.check_state() or attribute in reader.failed_attributes: 
        return False

      if attribute in reader.available_attributes: 
        return True

      try:
        value = attribute in reader.get_attributes()
      except:
        value = False

      (reader.available_attributes if value else reader.failed_attributes
            ).append(attribute)
      return value

    except:
      return False
  
    
def getArchivingReader(attr_list=None,start_date=0,stop_date=0,
                       hdb=None,tdb=None,logger=None,tango='',schema=''): 
    """
    It returns the most suitable reader for a list of attributes
    
    It is done counting the fail/errors per schema
    """
    attr_list = fun.toList(attr_list or [])
    try:
      schemas = Schemas.SCHEMAS or Schemas.load()
    except:
      schemas = ['hdb','tdb']
      
    pref = set(Reader.get_preferred_schema(a) for a in attr_list)    
    if any(pref): #schema set by history dialog
        schemas = dict(s for s in schemas.items() if s[0] in pref)
    if schema: #schema passed by user
        schemas = dict(s for s in schemas.items() if schema in s[0])
      
    if not attr_list: return None

    if logger is True: 
        log,logger = fandango.printf,None
    else: 
        log = logger and logger.debug or (lambda *args:None)

    log('getArchivingReader(%s): %s'%(attr_list,schemas.keys()))
    a,failed = '',fandango.defaultdict(int)
    
    #By default, it iterates over sorted Schemas.SCHEMAS
    for name in schemas:
      try:
        data = Schemas.getSchema(name,tango=tango,logger=log)
        if data is None: continue #Unreached schema
      
        ## Backwards compatibility
        if 'tdb' in (name,data.get('schema'),data.get('dbname')):
          if not data.get('reader'):
            data['reader'] = tdb or Reader('tdb',tango_host=tango,logger=logger)
          if not data.get('check'):
            data['check'] = 'now-reader.RetentionPeriod < start '\
                                                '< now-reader.ExportPeriod'

        if 'hdb' in (name,data.get('schema'),data.get('dbname')):
          if not data.get('reader'):
            data['reader'] = hdb or Reader('hdb',tango_host=tango,logger=logger)
            
        if not data.get('reader'): continue
        
        for a in attr_list:
          log('getArchivingReader(%s): trying on %s'%(a,name))
          if not Schemas.checkSchema(name,a,start_date,stop_date):
            log('getArchivingReader(%s,%s,%s): not in %s schema!'%(
              a,start_date,stop_date,name))
            failed[name]+=1
          elif not data['reader'].is_attribute_archived(a):
            log('getArchivingReader(%s,%s): not archived!'%(name,a))
            failed[name]+=1
            
      except Exception,e:
        log('getArchivingReader(%s,%s): failed!: %s'%(
            name,a,traceback.format_exc()))
        failed[name]+=1
          
      if not failed[name]: 
        if log: log('getArchivingReader(): Using %s'%name)
        return data['reader']
    
    #Return the best match
    failed = sorted((c,n) for n,c in failed.items())
    if failed and failed[0][0]!=len(attr_list):
        rd = data[failed[0][1]].get('reader')
        if log: 
            log('getArchivingReader(): Using %s'%failed[0][1])
        return rd
    return None
    
    ##@TODO: OLD CODE, TO BE REMOVED IN NEXT RELEASE
    #hdb,tdb = hdb or Reader('hdb',logger=logger),tdb or Reader('tdb',logger=logger)
    #attr_list = map(tdb.get_attribute_alias,attr_list if fandango.isSequence(attr_list) else [attr_list])
    #intdb,inhdb = all(map(tdb.is_attribute_archived,attr_list)),any(map(hdb.is_attribute_archived,attr_list))
    #now = time.time()
    #if intdb and (
            #not inhdb and stop_date and stop_date>(now-tdb.RetentionPeriod) 
        #or  (now-tdb.RetentionPeriod)<start_date<(now-tdb.ExportPeriod)
        #):
        #return tdb
    #elif inhdb: 
        #return hdb
    #else: 
        #return None

###############################################################################
###############################################################################
       
class Reader(Object,SingletonMap):
    """ 
    Lightweight API for read-only archiving applications 
    
    Arguments:
    
     * If no arguments are passed the Reader object uses *HdbExtractor*s to access the archiving.    
     * If a *db* argument is passed it creates a MySQL connection.
     * *config* param must be an string like **user:passwd@host[/db_name]**
     * if no *config* is passed it is read from Extractor klass properties
     * *schema* selects between hdb and tdb databases (historical and temporary)
     * Using '*' as db_name or schema will return an all-db reader that will get data from HDB or TDB depending on dates
     * This option will work only if all config is available from *Extractor klass properties
     
    USAGE:
    
    from PyTangoArchiving import hdb
    reader = hdb.Reader('hdb','user:passwd@host') #Initializes an HdbAPI with the values just required for reading
    reader = hdb.Reader() #In this case initializes an API with no db, using the HdbExtractor instances
    reader.read_attribute(attr,start_date,stop_date) #Which extractor/method to use must be hidden from user
    
    3 : import PyTangoArchiving
    4 : rd = PyTangoArchiving.Reader()
    5 : values = rd.get_attr_values('Straights/VC02/MKS-01/P1','2009-07-24 10:00:00','2009-07-29 16:00:00')
   
    """
    
    ## Methods replaced by SingletonMap
    #__singleton__ = None
    #@classmethod
    #def singleton(cls,*p,**k):
        #if not cls.__singleton__: 
            #cls.__singleton__ = cls(*p,**k)
        #return cls.__singleton__
    
    RetentionPeriod = 3*24*3600
    ExportPeriod = 600
    CacheTime = 600
    DefaultSchemas = ['hdb','tdb',] #'snap',) 
                     #'*','all') @TODO: Snap should be readable by Reader
    ValidArgs = ['db','config','servers','schema','timeout',
                 'log','logger','tango_host','alias_file']
    
    Preferred = CaselessDict() #Store specific attribute/schema user preferences
    
    @classmethod
    def set_preferred_schema(k,attr,sch):
        if sch=='*': sch = None
        attr = get_full_name(attr,fqdn=True)
        #print('Reader.set_preferred_schema(%s,%s)'%(attr,sch))
        Reader.Preferred[attr] = sch

    @classmethod
    def get_preferred_schema(k,attr):
        attr = get_full_name(attr,fqdn=True)
        sch = Reader.Preferred.get(attr)
        #print('Reader.get_preferred_schema(%s): %s'%(attr,sch))
        return sch
    
    @classmethod
    def parse_instance_key(cls,*p,**k):
        key = ''#','.join(x or '' for x in p) if p else ''
        k.update(zip(('db','config'),p))
        if 'db' in k: key+=':'+k['db']
        if 'config' in k: key+=':'+k['config']
        if 'schema' in k: key+=':'+(k['schema'] or '').replace('*','') # or (not k.get('db','') and '*'))
        if 'tango_host' in k: key+=':'+(k['tango_host'] or get_tango_host())
        if not key: 
            key = SingletonMap.parse_instance_key(cls,*p,**k)
        return key
            
    def __init__(self,db='*',config='',servers = None, schema = None,
                 timeout=300000,log='INFO',logger=None,tango_host=None,
                 multihost=False,alias_file=''):
        '''@param config must be an string like user:passwd@host'''
        if not logger:
            self.log = Logger('%s.Reader'%schema,
                format='%(levelname)-8s %(asctime)s %(name)s: %(message)s')
            self.log.setLogLevel(log)
        else: 
            self.log = logger
        
        self.configs = SortedDict()
        if schema is None: schema = db
        if schema is not None and db=='*': db = schema
        if any(s in ('*','all') for s in (db,schema)): db,schema = '*','*'
        self.log.debug('In PyTangoArchiving.Reader.__init__(%s)' 
                       % (schema or db or '...'))        
        self.db_name = db
        self._last_db = ''
        self.dbs = {}
        self.alias,self.servers,self.extractors = {},{},[]
        self.schema = schema if schema is not None else (
            [s for s in self.DefaultSchemas if s in db.lower()] or ['*'])[0]
        self.tango_host = tango_host or get_tango_host()
        self.multihost = multihost
        self.tango = PyTango.Database(*self.tango_host.split(':'))
        self.timeout = timeout
        self.modes = {}
        self.updated = time.time()
        self.attr_extracted = {}
        self.cache = {}
        self.is_hdbpp = False
        
        props = ['DbConfig'] + ([self.db_name] if self.db_name!='*' else [])
        dprops = self.tango.get_property('PyTangoArchiving',props)
        self.default = dprops.get(props[-1]) or dprops.get(props[0])
        
        #Initializing Database connection
        if '*' in (self.db_name,self.schema):
            self.init_universal(logger)
        else: 
            self.init_for_schema(self.schema or self.db_name,config,servers)
            
        try:
            alias_file = alias_file or get_alias_file()       
            self.alias = alias_file and read_alias_file(alias_file)
        except Exception,e: 
            self.log.warning('Unable to read alias file %s: %s'%(alias_file,e))

        #Initializing the state machine        
        self.reset() 
        
    @fandango.Catched
    def init_for_schema(self,schema,config='',servers=[]):
        print('%s.init_for_schema(%s,%s)' % (self.schema,schema,config))

        if not config and schema in Schemas.keys():
                #raise 'NotImplemented!, Use generic Reader() instead'
                sch = Schemas.getSchema(schema)
                sch = map(sch.get,('user','passwd','host','db_name'))
                if all(sch): config = '%s:%s@%s/%s' % tuple(sch)
                
        if not config and schema in ('hdb','tdb'):
            try:
                prop = '%sextractor'%self.schema
                prop = self.tango.get_class_property(prop,['DbConfig'])
                config = '\n'.join(prop['DbConfig'] or [''])
            except: 
                pass
            if not config and self.default: 
                config = '\n'.join(self.default)
            
        if config:
            self.configs.update( (0 if '<' not in c 
                else str2epoch(c.split('<')[0]),c.split('<')[-1]) 
                for c in config.split() )
            
        if not config and self.db_name in Schemas.keys() \
                and self.schema not in ('hdb','tdb'):
            raise 'NotImplemented!, Use generic Reader() instead'

        ## THIS METHOD OF CHECKING HDB++ IS FLAWED!! (and unused) @TODO
        #if any(a.lower() in s for s in map(str,(self.db_name,schema,config)) 
               #for a in ('hdbpp','hdb++','hdblite')):
        if schema.lower() not in ('*','hdb','tdb'):
            self.is_hdbpp = True
            c = sorted(self.configs.items())[-1][-1]
            self.db_name = c.split('/')[-1] if '/' in c else schema
            self.log.debug("Created HDB++ reader")
        else:
            self.log.debug("Created '%s' reader"%self.db_name)
        
        if self.schema.lower() == 'tdb': 
            #RetentionPeriod must be updated for all generic readers
            try:
                prop = self.tango.get_class_property('TdbArchiver',
                                ['RetentionPeriod'])['RetentionPeriod']
                prop = prop[0] if prop else 'days/3'
                Reader.RetentionPeriod = max((Reader.RetentionPeriod,
                                eval('1./(%s)'%prop,{'days':1./(3600*24)})))
            except Exception,e: 
                self.log.warning('Error on RetentionPeriod: %s'%e)
            
        #Initializing archiver extractors proxies
        if self.get_database() is None:
            from fandango.servers import ServersDict
            self.servers = servers or ServersDict(logger=self.log)
            #self.servers.log.setLogLevel(log)
            if self.tango_host == os.getenv('TANGO_HOST'):
                self.servers.load_by_name('%sextractor'%schema)
                self.extractors = self.servers.get_class_devices(
                    ['TdbExtractor','HdbExtractor'][schema=='hdb'])
            else:
                self.extractors = list(self.tango.get_device_exported(
                    '*%sextractor*'%self.schema))        
        
    def init_universal(self,logger):

        self.log.debug("Reader.init_universal(%s)"%','.join(Schemas.SCHEMAS))
        rd = getArchivingReader()
        #Hdb++ classes will be scanned when searching for HDB
        #tclasses = map(str.lower,fandango.get_database().get_class_list('*'))
        for s in Schemas.SCHEMAS:
            #if (s in self.DefaultSchemas 
                    #and any(c.startswith(s.lower()) for c in tclasses)):
                #self.configs[s] = Reader(s,logger=logger)

            #else:
            sch = Schemas.getSchema(s,logger=self.log)
            if sch: 
                self.configs[sch.get('schema')] = sch.get('reader')
            else:
                self.log.warning('%s schema not loaded!' % s)

        self.log.debug("... created")        
        
        
    def __del__(self):
        for k in self.dbs.keys()[:]:
            o = self.dbs.pop(k)
            del o
        
    def reset(self):
        self.log.debug('Reader.reset()')
        self.last_dates = defaultdict(lambda:(1e10,0))
        if hasattr(self,'state'):
            [db.renewMySQLconnection() for db in self.dbs.values()]
        self.last_retry = 0
        self.available_attributes = []
        self.current_attributes = []
        self.failed_attributes = []
        self.attr_schemas = fandango.defaultdict(list)
        self.cache.clear()
        if self.extractors or self.dbs or self.configs:
            self.state = PyTango.DevState.INIT
        else:
            self.state = PyTango.DevState.FAULT
            self.log.info('No available extractors found, '
                'PyTangoArchiving.Reader disabled')
                
    def get_database(self,epoch=-1):
        try:
            if epoch<0: epoch = time.time()
            config = sorted((e,c) for e,c in self.configs.items() if e<=epoch)[-1][-1]
        except Exception,e:
            #traceback.print_exc()
            self.log.warning('Unable to get DB(%s,%s) config at %s, using Java Extractors.\n%s'%(self.db_name,self.schema,epoch,e))
            return None
        #print('get_database(%s)' % self.schema)
        try:
            user,host = '@' in config and config.split('@',1)\
                or (config,os.getenv('HOSTNAME'))
            user,passwd = ':' in user and user.split(':',1) or (user,'')
            host,db_name = host.split('/') if '/' in host \
                else (host,self.db_name)
            #(self.log.info if len(self.configs)>1 else self.log.debug)('Accessing MySQL using config = %s:...@%s/%s' % (user,host,db_name))
        except:
            self.log.warning('Wrong format of DB config: %s.\n%s'%(config,traceback.format_exc()))
            return None
        try:
            if (host,db_name) not in self.dbs:
                if self.is_hdbpp:
                    from PyTangoArchiving.hdbpp import HDBpp
                    self.dbs[(host,db_name)] = HDBpp(db_name,host,user,passwd)
                else: 
                    self.dbs[(host,db_name)] = ArchivingDB(db_name,host,user,passwd,loglevel=self.log.getLogLevel(),default_cursor=MySQLdb.cursors.SSCursor)
                
            if '%s@%s'%(db_name,host) != self._last_db:
                self._last_db = '%s@%s'%(db_name,host)
                self.log.debug('In get_database(%s): %s'%(epoch,self._last_db))
            return self.dbs[(host,db_name)]
        except:
            self.log.warning('Unable to access MySQL using config = %s:...@%s/%s\n%s' % (user,host,db_name,traceback.format_exc()))
            return None
        
    def check_state(self,period=300):
        """ It tries to reconnect to extractors every 5 minutes. """
        if (time.time()-self.last_retry)>period: 
            self.last_retry = time.time()
            states = [PyTango.DevState.FAULT,PyTango.DevState.ON]
            try:
                if self.is_hdbpp:
                    return True
                elif self.db_name=='*':
                    for v in self.configs.values():
                      if hasattr(v,'check_state') and v.check_state():
                        self.state = PyTango.DevState.ON
                        return True
                elif self.get_database(): 
                    self.state = states[self.get_database().check()]
                elif self.extractors:
                    self.state = states[bool(self.get_extractor(check=False))]
                else:
                    self.state = states[False]
            except: 
                self.log.error('In Reader.check_state: %s'%traceback.format_exc())
                self.state = states[False]
        
        return (self.state!=PyTango.DevState.FAULT)
        
    #################################################################################################
    #################################################################################################
                
    def get_extractor(self,check=True,attribute=''):
        """ Gets a random extractor device."""
        #Try Unified Reader
        if self.db_name=='*':
            return self.configs[('tdb' if self.configs['tdb'].is_attribute_archived(attribute) else 'hdb')].get_extractor(check,attribute)
        
        extractor = None
        if (check and not self.check_state()) or not self.extractors:
            self.log.warning('get_extractor(): Archiving seems not available')
            return None
        
        # First tries to get the previously used extractor, if it 
        # is not available then searches for a new one ....
        if attribute and attribute in self.attr_extracted:
            extractor = self.servers.proxies[self.attr_extracted[attribute]]
            try:
                extractor.ping()
                extractor.set_timeout_millis(self.timeout)
            except Exception,e: extractor = None
            
        if not extractor:
            remaining = self.extractors[:]
            while remaining: #for i in range(len(self.extractors)):
                next = randrange(len(remaining))
                devname = remaining.pop(next)
                if ':' not in devname: devname = self.tango_host +'/' +devname
                extractor = self.servers.proxies[devname]
                try:
                    extractor.ping()
                    extractor.set_timeout_millis(self.timeout)
                    break
                except Exception,e: 
                    self.log.debug(traceback.format_exc())
                    
        self.state = PyTango.DevState.ON if extractor else PyTango.DevState.FAULT
        return extractor    
        
    @Cached(depth=10,expire=15.)
    def get_attributes(self,active=False):
        """ Queries the database for the current list of archived attributes."""
        self.log.debug('get_attributes(%s)'%active)
        t0 = now()

        if self.available_attributes and self.current_attributes \
                and time.time()<(self.updated+self.CacheTime):
            return (self.available_attributes,self.current_attributes)[active]

        self.log.debug('%s: In Reader(%s).get_attributes(): '
            'last update was at %s'%(time.ctime(),self.schema,self.updated))
        self.log.debug('multihost = %s' % self.multihost)
        
        get_model = self.get_attribute_model
        get_models = lambda l: sorted(set(map(get_model,l)))
        
        self.available_attributes, self.current_attributes = [],[]
        
        #Try Unified Reader
        if self.db_name=='*':    
            for c,x in self.configs.items():
                self.log.debug('Getting %s attributes' % c)
                for act in (True,False):
                    try:
                        attrs = x.get_attributes(active=act)
                        self.log.debug('%d' % len(attrs))
                        self.log.debug('parse models')
                        attrs = map(self.get_attribute_model,attrs)
                        self.log.debug('%d' % len(attrs))
                        self.log.debug('union')
                        if act:
                            self.current_attributes.extend(attrs)
                            self.log.debug('%d' % len(self.current_attributes))
                        else:
                            self.available_attributes.extend(attrs)
                            self.log.debug('%d' % len(self.available_attributes))
                            self.log.debug('update schemas')
                            [self.attr_schemas[a].append(c) for a in attrs
                             if c not in self.attr_schemas[a]]
                    except:
                        self.log.warning('Unable to get %s attributes:\n %s' 
                            % (c, traceback.format_exc()))
                        
                self.log.debug('%d' % len(self.available_attributes))
                self.log.debug(self.available_attributes and self.available_attributes[0])
                self.log.debug(self.available_attributes and self.available_attributes[-1])
                
            self.log.debug('sorting')
            self.available_attributes = sorted(set(self.available_attributes))
            self.current_attributes = sorted(set(self.current_attributes))                
        else:
            if self.get_database(): #Using a database Query
                t1 = now()
                self.log.debug('query')
                avs = self.get_database().get_attribute_names(active=False)
                currs = self.get_database().get_attribute_names(active=True)
                self.log.debug('models')
                self.available_attributes = map(get_model,avs)
                self.current_attributes = map(get_model,currs)

            elif self.extractors: #Using extractors
                attrs = self.__extractorCommand(self.get_extractor(), 
                                            'GetCurrentArchivedAtt')
                self.current_attributes = map(get_model,attrs)
                self.available_attributes = self.current_attributes
                
            self.log.debug('schemas')
            for a in self.available_attributes:
                self.attr_schemas[a] = [self.schema]

        #This match is already done by isAttributeArchived and it interferres
        #with get_attribute_alias()
        #self.log.debug('Updating %d aliases' % len(self.alias))
        #for a,m in self.alias.items():
            #a,m = get_model(a),get_model(m)
            #if m in self.current_attributes:
                #self.current_attributes.append(get_model(a))
            #if m in self.available_attributes:
                #self.available_attributes.append(get_model(a))
                #self.attr_schemas[a] = [s for s in Schemas.SCHEMAS if a in
                    #(self.attr_schemas[a]+self.attr_schemas[m])]
            
        self.log.debug('sorting')
        self.available_attributes = sorted(set(self.available_attributes))
        self.current_attributes = sorted(set(self.current_attributes))
        self.updated = now()
        self.log.debug('In Reader(%s).get_attributes(): '
            '%s attributes available in the database (+%ds)'
            % (self.schema,len(self.available_attributes),self.updated-t0))
        
        return (self.available_attributes,self.current_attributes)[active]
    
    #@Cached(depth=10000,expire=86400)
    def get_attribute_model(self,attribute):
        """
        Returns normal/full name depending on multihost mode
        """
        #return (get_normal_name,get_full_name)[self.multihost](attribute)
        attribute = attribute.lower()
        if not self.multihost and attribute.count('/')>=3:
            return '/'.join(attribute.split('/')[-4:])
        if self.multihost and attribute.count(':')==2:
            return attribute
        #self.log.debug('parsing(%s)' % attribute)
        m = parse_tango_model(attribute)
        return (m.simplename,m.fullname)[self.multihost]

    @Cached(depth=10000,expire=60.)        
    def get_attribute_alias(self,model):
        #Check if attribute has an alias
        try:
            attribute = str(model)
            attribute = (expandEvalAttribute(attribute) or [attribute])[0]
            self.get_attributes()
            attribute = attribute.lower()
            if attribute in self.current_attributes:
                return attribute
            elif attribute in self.alias:
                attribute = self.alias.get(attribute)
            elif attribute:
                attribute = utils.translate_attribute_alias(attribute)
                if attribute != str(model):
                    attribute,alias = \
                        self.get_attribute_alias(attribute),attribute

        except Exception,e:
             print('Unable to find alias for %s: %s'%(model,str(e)[:40]))
        return attribute
                
    @Cached(depth=10000,expire=60.)
    def get_attribute_modes(self,attribute,force=False):
        """ Returns mode configuration, accepts wildcards """
        attribute = self.get_attribute_alias(attribute)
        attribute = re.sub('\[([0-9]+)\]','',attribute.lower())
        if force or attribute not in self.modes:
            if self.db_name in ('hdb','tdb'):
                self.modes[attribute] = dict((translate_attribute_modes(k),v) 
                    for k,v in 
                        self.get_database().get_attribute_modes(
                            attribute,asDict=True).items()
                        if k in DB_MODES or k.lower() in ('archiver','id'))
            else:
                self.modes[attribute] = {}
                schemas = self.is_attribute_archived(attribute)
                for s in schemas:
                    c = self.configs[s]
                    try:
                        m = c.get_attribute_modes(attribute,force)
                    except:
                        m = {}
                    self.modes[attribute][s] = m
                    
        return self.modes[attribute]
    
    @Cached(depth=10000,expire=60.)
    def is_attribute_archived(self,attribute,active=False,preferent=True):
        """ 
        This method uses two list caches to avoid redundant device 
        proxy calls, launch .reset() to clean those lists. 
        """

        if self.is_hdbpp: # NEVER CALLED IF setting reader=HDBpp(...)
            self.log.warning('HDBpp.is_attribute_archived() OVERRIDE!!')
            return True
        
        if expandEvalAttribute(attribute):
            return all(self.is_attribute_archived(a,active) 
                       for a in expandEvalAttribute(attribute))

        self.get_attributes() #Updated cached lists
        attr = self.get_attribute_alias(attribute)
        attr = self.get_attribute_model(attribute)
        
        if self.db_name=='*':
            # Universal reader
            pref = self.get_preferred_schema(attr)
            if preferent and pref not in (None,'*'): 
                return [pref]
            elif len(self.attr_schemas[attr]):
                return self.attr_schemas[attr]
            else:
                sch = []
                for a,c in self.configs.items():
                    if a == self.db_name: continue
                    try:
                        if (c and (a not in Schemas.keys() or
                                Schemas.checkSchema(a,attr)) 
                            and c.is_attribute_archived(attr,active)):
                            sch.append(a)
                    except: 
                        self.log.warning('%s archiving not available'%a)
                        self.log.warning(traceback.format_exc())
                        
                return tuple(sch) 
                #return tuple(a for a in self.configs if self.configs.get(a) \
                #and (a not in Schemas.keys() or Schemas.checkSchema(a,attribute))
                #and self.configs[a].is_attribute_archived(attribute,active))
        else:
            # Schema reader
            # first remove array indexes
            if (attr in (self.current_attributes if active 
                    else self.available_attributes)):
                return attr
            
            else: #Reloading attribute lists
                alias = self.get_attribute_alias(attr)
                alias = parse_tango_model(alias)
                assert alias.tango_host == self.tango_host, \
                    Exception('multihost aliases not implemented!')
                alias = alias.simplename
                cache = (self.current_attributes if active 
                    else self.available_attributes)
                if self.get_attribute_model(alias) in cache:
                    return True
                
        return False
                
    def load_last_values(self,attribute,schema=None,epoch=None):
        """ Returns the last values stored for each schema """
        result = dict()
        if fandango.isSequence(attribute):
            for attr in attribute:
                result[attr] = self.load_last_values(attr,schema,epoch)
            return result
        elif schema is None:
            schemas = self.is_attribute_archived(attribute)
        else:
            schemas = fandango.toList(schema)
        for s in schemas:
            api = Schemas.getApi(s)
            vs = api.load_last_values(attribute)
            vs = vs.values() if hasattr(vs,'values') else vs
            r = vs and vs[0]
            if r and isinstance(r[0],datetime.datetime):
                r = [fun.date2time(r[0]),r[1],(r[2:3] or [None])[0],
                     fun.date2str(r[0])]
            result[s] = r
        return result
        
        
    def get_last_attribute_dates(self,attribute):
        """ 
        This method returns the last cached start/stop dates 
        returned for an attribute.
        """
        if expandEvalAttribute(attribute):
            return sorted(self.get_last_attribute_dates(a) 
                          for a in expandEvalAttribute(attribute))[-1]
        elif self.db_name=='*':
            return sorted(self.configs[s].last_dates.get(attribute,(0,0)) 
                          for s in ('tdb','hdb'))[-1]
        else:
            return self.last_dates[attribute]
          
    @staticmethod
    def get_time_interval(start_date,stop_date):
        """
        This method will take any valid input time format and will return four values:
        start_date,start_time,stop_date,stop_time
        """
        start_time = start_date if isinstance(start_date,(int,float)) \
                            else (start_date and str2epoch(start_date) or 0)
        stop_time = stop_date if isinstance(stop_date,(int,float)) \
                                else (stop_date and str2epoch(stop_date) or 0)
        if not start_time or 0<=start_time<START_OF_TIME:
            raise Exception('StartDateTooOld(%s)'%start_date)
        elif start_time<0: 
            start_time = time.time()+start_time
        if not stop_time: #Query optimized to get the latest values
            stop_time,GET_LAST = time.time(),True
        else:
            if stop_time<0: stop_time = time.time()+stop_time
            GET_LAST = False

        start_date,stop_date = epoch2str(start_time),epoch2str(stop_time)
        if not start_time<stop_time: 
            raise Exception('StartDateMustBeLowerThanStopDate(%s,%s)'
                            %(start_date,stop_date))
        
        return start_date,start_time,stop_date,stop_time
        
        
    def get_attribute_values(self,attribute,start_date,stop_date=None,
            asHistoryBuffer=False,decimate=False,notNone=False,N=0,
            cache=True,fallback=True):
        '''         
        This method reads values for an attribute between specified dates.
        This method may use MySQL queries or an H/TdbExtractor DeviceServer to 
        get the values from the database.
        The format of values returned is [(epoch,value),]
        The flag 'asHistoryBuffer' forces to return the rawHistBuffer 
        returned by the DS.
                
        :param attributes: list of attributes
        :param start_date: timestamp of the first value
        :param stop_date: timestamp of the last value
        :param asHistoryBuffer: return a history buffer object instead 
                of a list (for trends)
        :param N: if N>0, only the last N values will be returned
                  if N<0, values will be extracted from the end of the query
        :param decimate: remove repeated values, False by default but True 
                when called from trends
        
        :return: a list with values (History or tuple values depending of args)
        '''
        if not self.check_state(): 
            self.log.info('In PyTangoArchiving.Reader.get_attribute_values:'
                ' Archiving not available!')
            return []

        start_date,start_time,stop_date,stop_time = \
            self.get_time_interval(start_date,stop_date)
          
        GET_LAST = 0 < (time.time()-stop_time) < 3
        
        l1,l2 = start_time,stop_time
        self.last_dates[attribute] = l1,l2

        # WHY NOT TO CHECK FOR ALIAS HERE!? ... alias should be per schema?
        # Checks if the attribute is a member of an array 
        # This part will be duplicated wherever alias is checked
        array_index = re.search('\[([0-9]+)\]',attribute) 
        if array_index: 
            attr = attribute.replace(array_index.group(),'')
            array_index = array_index.groups()[0]
        else:
            attr = attribute        
        
        ###################################################################
        # CACHE MANAGEMENT
        
        cache = self.cache if cache else {}
        if cache:
            self.log.debug('Checking Keys in Cache: %s'%self.cache.keys())
                
            margin = max((60.,.01*abs(l2-l1)))
            nearest = [(a,s1,s2,h,d) for a,s1,s2,h,d in self.cache 
                if a==attr and h==asHistoryBuffer and d==bool(decimate) 
                and (s1-margin<=l1 and l2<=s2+margin)]
            if nearest: 
                attr,l1,l2,asHistoryBuffer,decimate = nearest[0]
                
            ckey = (attr,l1,l2,asHistoryBuffer,bool(decimate))
            values = cache.get(ckey,False)
            
            #any(len(v)>1e7 for v in self.cache.values()) or 
            if get_memory()>1e9:
                self.log.warning('... Reader.cache clear()')
                self.cache.clear()
            
            if values:
                self.log.info('Reusing Cached values for (%s)' % (str(ckey)))
                if array_index: #Array index is an string or None
                    values = self.extract_array_index(
                                values,array_index,decimate,asHistoryBuffer)                
                return values     
        
        ######################################################################    
        # Evaluating Taurus Formulas : 
        #   it overrides the whole get_attribute process
        
        if expandEvalAttribute(attribute):
            
            getId = lambda s: s.strip('{}').replace('/','_').replace('-','_')
            attribute = attribute.replace('eval://','')
            attributes = expandEvalAttribute(attribute)
            for a in attributes:
                attribute = attribute.replace('{%s}'%a,' %s '%getId(a))

            resolution = max((1,(stop_time-start_time)/MAX_RESOLUTION))
            vals = dict((k,fandango.arrays.filter_array(v,window=resolution)) 
                            for k,v in self.get_attributes_values(
                                attributes,start_date,stop_date).items())
            cvals = self.correlate_values(vals,resolution=resolution,
                                rule=choose_last_value)#(lambda t1,t2,tt:t2))

            values,error = [],False
            for i,t in enumerate(cvals.values()[0]):
                v = None
                try:
                    pars = dict((getId(k),v[i][1]) for k,v in cvals.items())
                    if None not in pars.values(): v = eval(attribute,pars)
                except:
                    if not error: traceback.print_exc()
                    error = True
                values.append((t[0],v))
            
        #######################################################################
        # Generic Reader, using PyTangoArchiving.Schemas properties
        
        elif self.db_name=='*':
          
            rd = getArchivingReader(attribute,start_time,stop_time,
                  self.configs.get('hdb',None),self.configs.get('tdb',None),
                  logger=self.log)
            if not rd: 
                self.log.warning('In get_attribute_values(%s): '
                  'No valid schema at %s'%(attribute,start_date))
                return []
            #@debug
            self.log.warning('In get_attribute_values(%s): '
              'Using %s schema at %s'%(attribute,rd.schema,start_date))

            #@TODO, implemented classes should have polimorphic methods
            values = rd.get_attribute_values(attribute,start_date,stop_date,
                    asHistoryBuffer=asHistoryBuffer,decimate=decimate,
                    notNone=notNone,N=N)
            
            # If no data, it just tries the next database
            if fallback and (values is None or not len(values)): 
                sch = self.is_attribute_archived(attribute)[1:]
                while not len(values) and len(sch):
                    self.log.warning('In get_attribute_values(%s,%s,%s)(%s): '
                      'fallback to %s as %s returned no data'%(
                        attribute,start_date,stop_date,rd.schema,
                        sch[0], rd.schema))
                    values = self.configs[sch[0]].get_attribute_values(
                        attribute,start_date,stop_date,
                        asHistoryBuffer=asHistoryBuffer,decimate=decimate,N=N)
                    sch = sch[1:]
          
        # END OF GENERIC CODE
        #######################################################################
          
        #######################################################################
        # HDB/TDB Specific Code
        else:
            
            alias = self.get_attribute_alias(attribute).lower()
            #Needed to record last read values for both alias and real name
            attribute,alias = alias,attribute 
            #@debug
            self.log.warning('In PyTangoArchiving.Reader.get_attribute_values'
                '(%s,%s,%s,%s)'%(self.db_name,attribute,start_date,stop_date))
            
            #Checks if the attribute is a member of an array 
            array_index = re.search('\[([0-9]+)\]',attribute) 
            if array_index: 
                attribute = attribute.replace(array_index.group(),'')
                #Gets the index as an string
                array_index = array_index.groups()[0] 
            
            self.last_dates[alias] = l1,l2
            db = self.get_database(l1)
            
            ###################################################################
            # QUERYING NEW HDB/TDB VALUES
            if not db:
                ##USING JAVA EXTRACTORS
                values = self.get_extractor_values(attribute, start_date, 
                                        stop_date, decimate, asHistoryBuffer)
            else:
                values = self.get_attribute_values_from_db(attribute, db,
                            start_date, stop_date, decimate, 
                            asHistoryBuffer, N, notNone, GET_LAST)
                
        #######################################################################
        #DECIMATION IS DONE HERE
        
        l0,t1 = len(values),time.time()
        if l0 > 128 and decimate:
            decimate,window = decimate if isSequence(decimate) \
                                        else (decimate,'0')
            if isString(decimate):
                try: 
                    decimate = eval(decimate)
                except:
                    self.log.warning('Decimation(%s)?: %s'
                        % (decimate, traceback.format_exc()))
                        
            ## Decimation by data_has_changed is done always
            values = decimation(values, decimate, window=window, 
                                logger_obj=self.log)
                
            #@debug
            self.log.warning('\tDecimated %s[%d > %d] in %s s' 
                    % (attribute,l0,len(values),time.time()-t1))
            t1 = time.time()
                    
        #Simulating DeviceAttributeHistory structs
        if asHistoryBuffer:
            values = [FakeAttributeHistory(*v) for v in values]                
                    
        #Array index is an string or None
        if array_index: 
            values = self.extract_array_index(values,array_index,
                                                decimate,asHistoryBuffer)
                
        #######################################################################
        # SAVE THE CACHE
        if cache:
            self.cache[(attribute,l1,l2,asHistoryBuffer,bool(decimate))
                    ] = values[:]                

        return values
    
    def get_attribute_values_from_db(self, attribute, db, 
            start_date, stop_date, decimate, asHistoryBuffer, 
            N, notNone, GET_LAST):
        """
        Query MySQL HDB/TDB databases to extract the attribute data
        """
        # CHOOSING DATABASE METHODS
        if not self.is_hdbpp:
            try:
                full_name,ID,data_type,data_format,writable = \
                    db.get_attribute_descriptions(attribute)[0]
            except Exception,e: 
                raise Exception('%s_AttributeNotArchived: %s'
                                %(attribute,e))

            data_type,data_format = (utils.cast_tango_type(
                PyTango.CmdArgType.values[data_type]),
                PyTango.AttrDataFormat.values[data_format])
            
            self.log.debug('%s, ID=%s, data_type=%s, data_format=%s'
                            %(attribute,ID,data_type,data_format))
            table = get_table_name(ID)
            method = db.get_attribute_values
        else:
            table = attribute
            method = db.get_attribute_values
            data_type = float
            data_format = PyTango.AttrDataFormat.SCALAR

        #######################################################################
        # QUERYING THE DATABASE 
        #@TODO: This retrying should be moved down to ArchivingDB class instead
        retries,t0,s0,s1 = 0,time.time(),start_date,stop_date
        while retries<2 and t0>(time.time()-10):
            if retries: 
                #(reshape/retry to avoid empty query bug in python-mysql)
                self.log.warning('\tQuery (%s,%s,%s) returned 0 values, '
                                 'retrying ...' % (attribute,s0,s1))
                s0,s1 = epoch2str(str2epoch(s0)-30),epoch2str(str2epoch(s1)+30) 
            result = method(table,s0,s1 if not GET_LAST else None,
                            N=N,unixtime=True)
            if len(result): 
                if retries:
                    result = [r for r in result if start_date<=r[0]<=stop_date]
                break
            retries+=1

        if not result: 
            self.log.warning('Empty query after %d retries? (%s) = [0] in %ss'
                % (retries,str((table,start_date,stop_date,GET_LAST,N,0)),
                   time.time()-t0))
            return []
        
        l0 = len(result)
        t1 = time.time()
        #@debug
        self.log.info('\tQuery(%s,%s,%s,%s,%s) = [%d] in %s s'
                       %(table,start_date,stop_date,GET_LAST,N,l0,t1-t0))       
        self.last_reads = result and (result[0][0],result[-1][0]) or (1e10,1e10)
        
        try:
            values = self.extract_mysql_data(result,
                            data_type,data_format,notNone)
            values = patch_booleans(values)
        except Exception,e:
            self.log.info(traceback.format_exc())
            raise Exception('Reader.UnableToConvertData(%s,format=%s)'
                            % (attribute,data_format),str(e))
        
        return values
    
    def extract_mysql_data(self, result, data_type, data_format, notNone):
        # CASTING DATATYPES AND DECIMATION
        #Returning a list of (epoch,value) tuples
        values = []
        t1 = time.time()
        ## The following queries are optimized for performance
        #getting read_value index (w/out dimension)
        ix = 1 if len(result[0])<4 else 2 

        #THIS CAST METHODS ARE USED WHEN PARSING DATA FROM SPECTRUMS
        if data_type is bool: 
            cast_type = mysql2bool
        elif data_type is int: 
            #Because int cannot parse '4.0'
            cast_type = lambda x:int(float(x)) 
        else: 
            cast_type = data_type
        
        self.log.debug(str(data_type)+' '+str(notNone))
        
        if data_format==PyTango.AttrDataFormat.SPECTRUM:
            
            dt,df = (cast_type,0.0) if data_type in (int,bool) \
                                        else (data_type,None)
            if notNone: 
                values = [(w[0],mysql2array(w[ix],dt,df)) 
                            for w in result if w[ix] is not None]
            else:
                values = [(w[0],mysql2array(w[ix],dt,df) 
                            if w[ix] else None) for w in result]

        #SCALAR values, queries are optimized for performance
        elif data_type in (bool,) and notNone:
            values = [(w[0],cast_type(w[ix])) 
                        for w in result if w is not None]
        elif data_type in (bool,):
            values = [(w[0],cast_type(w[ix])) for w in result]
        elif notNone:
            values = [(w[0],w[ix]) for w in result if w[ix] is not None]
        else:
            values = [(w[0],w[ix]) for w in result]

        #@debug
        self.log.info('\tParsed [%d] in %s s'%(len(values),time.time()-t1))

        return values    
        
    def extract_array_index(self,values,array_index,decimate=False,asHistoryBuffer=False):
        # Applying array_index to the obtained results, it has to be applied after attribute loading to allow reusing cache in array-indexed attributes
        last,l0 = (0,None),len(values)
        
        self.log.debug('Applying array_index(%s) to the obtained results'%array_index)
        array_index = int(array_index)
        new_values = [] # We create a new list on purpose to not modify the cached values
        fin = (lambda v: (v.time,v.value[array_index] if v.value is not None and len(v.value)>array_index else None)) if asHistoryBuffer \
            else (lambda v: (v[0],v[1][array_index] if v[1] is not None and len(v[1])>array_index else None))
        fcmp = (lambda l: (l[0].tv_sec,l[1])) if asHistoryBuffer else (lambda l: l)
        fout = (lambda vv: FakeAttributeHistory(*(vv))) if asHistoryBuffer else (lambda vv: vv)
        for i,v in enumerate(values):
            try:
                if v is None: continue
                vv = fin(v)
                next = ((values[i+1] and fin(values[i+1]) or None) if i+1<l0 else None)
                if not decimate or next is None or not new_values or data_has_changed(fcmp(vv),fcmp(last),fcmp(next)):
                    new_values.append(fout(vv))
                    last = vv
            except Exception,e:
                self.log.warning('reader.get_attribute_values(...,asHistoryBuffer=%s): Unable to parse %d[%s]:(%s); %s'%(asHistoryBuffer,i,array_index,v,traceback.format_exc()))
        if decimate:
            self.log.info('\tIn extract_array_index(...).raw: decimated repeated values in spectrum ... %s -> %s'%(l0,len(new_values)))
        return new_values
    
    def get_attributes_values(self,attributes,start_date,stop_date=None,correlate=False,
                              asHistoryBuffer=False,trace = False, text = False, N=0):
        """ 
        This method reads values for a list of attributes between specified dates.
        
        :param attributes: list of attributes
        :param start_date: timestamp of the first value
        :param stop_date: timestamp of the last value
        :param correlate: group values by time using first attribute timestamps
        :param asHistoryBuffer: return a history buffer object instead of a list (for trends)
        :param trace: print out the values obtained
        :param text: return a tabulated text instead of a dictionary of values
        :param N: if N>0, only the last N values will be returned
        
        :return: a dictionary with the values of each attribute or (if text=True) a text with tabulated columns
        
        """
        if not attributes: raise Exception('Empty List!')
        start = time.time()
        values = dict([(attr,self.get_attribute_values(attr,start_date,stop_date,asHistoryBuffer,N=N)) for attr in attributes])
        self.log.debug('Query finished in %d milliseconds'%(1000*(time.time()-start)))
        if correlate or text:
            if len(attributes)>1:
                table = self.correlate_values(values,str2time(stop_date),resolution=(correlate if correlate is not True and fun.isNumber(correlate)  else None))
            else:
                table = values
            if trace or text: 
                csv = self.export_to_text(table,order=list(attributes))
                if text: return csv
                elif trace: print csv
            return table
        else:
            if trace: print values
            return values
          
    @staticmethod
    def export_to_text(table,order=None,**kwargs):
        """
        It will convert a [(timestamp,value)] array in a CSV-like text.
        Order will be used to set the order to data columns (date and timestamp will be always first and second).

        Other parameters are available:

          sep : character to split values in each row
          arrsep : character to split array values in a data column
          linesep : characters to insert between lines
          
        """
        sep = kwargs.get('sep','\t')
        arrsep = kwargs.get('arrsep',kwargs.get('separator',', '))
        linesep = kwargs.get('linesep','\n')
        
        start = time.time()
        if not hasattr(table,'keys'): table = {'attribute':table}
        if not order or not all(k in order for k in table): keys = list(sorted(table.keys()))
        else: keys = sorted(table.keys(),key=order.index)
        csv = sep.join(['date','time']+keys)+linesep
        def value_to_text(s):
          v = (str(s) if not fandango.isSequence(s) else arrsep.join(map(str,s))).replace('None','')
          return v
        time_to_text = lambda t: time2str(t,cad='%Y-%m-%d_%H:%M:%S')+('%0.3f'%(t%1)).lstrip('0') #taurustrend timestamp format
        for i in range(len(table.values()[0])):
            csv+=sep.join([time_to_text(table.values()[0][i][0]),str(table.values()[0][i][0])]+[value_to_text(table[k][i][1]) for k in keys])
            csv+=linesep
        print('Text file generated in %d milliseconds'%(1000*(time.time()-start)))
        return csv

    def correlate_values(self,values,stop=None,resolution=None,debug=False,rule=None,MAX_VALUES=50000):
        ''' Correlates values to have all epochs in all columns
        :param values:  {curve_name:[values]}
        :param resolution: two epochs with difference smaller than resolution will be considered equal
        :param stop: an end date for correlation
        :param rule: a method(tupleA,tupleB,epoch) like (min,max,median,average,last,etc...) that will take two last column (t,value) tuples and time and will return the tuple to keep
        '''
        start = time.time()
        self.log.info('correlate_values(%d x %d,resolution=%s,MAX_VALUES=%d) started at %s'%(
            len(values),max(len(v) for v in values.values()),resolution,MAX_VALUES,time.ctime(start)))
        stop = stop or start
        keys = sorted(values.keys())
        table = dict((k,list()) for k in keys)
        index = dict((k,0) for k in keys)
        lasts = dict((k,(0,None)) for k in keys)
        first,last = min([t[0][0] if t else 1e12 for t in values.values()]),max([t[-1][0] if t else 0 for t in values.values()])
        if resolution is None:
            #Avg: aproximated time resolution of each row
            avg = (last-first)/min((MAX_VALUES/6,max(len(v) for v in values.values()) or 1))
            if avg < 10: resolution = 1
            elif 10 <= avg<60: resolution = 10
            elif 60 <= avg<600: resolution = 60
            elif 600 <= avg<3600: resolution = 600
            else: resolution = 3600 #defaults
            self.log.info('correlate_values(...) resolution set to %2.3f -> %d s'%(avg,resolution))
        assert resolution>.1, 'Resolution must be > 0'
        if rule is None: rule = fun.partial(choose_first_value,tmin=-resolution*10)
        #if rule is None: rule = fun.partial(choose_last_max_value,tmin=-resolution*10)
        
        epochs = range(int(first-resolution),int(last+resolution),int(resolution))
        for k,data in values.items():
            self.log.info('Correlating %s->%s values from %s'%(len(data),len(epochs),k))
            i,v,end = 0,data[0] if data else (first,None),data[-1][0] if data else (last,None)

            for t in epochs:
                v,tt = None,t+resolution
                #Inserted value will  be (<end of interval>,<correlated value>)
                #The idea is that if there's a value in the interval, it is chosen
                #If there's no value, then it will be generated using previous/next values
                #If there's no next or previous then value will be None
                #NOTE: Already tried a lot of optimization, reducing number of IFs doesn't improve
                #Only could guess if iterating through values could be better than iterating times
                if i<len(data):
                    for r in data[i:]:
                        if r[0]>(tt):
                            if v is None: #No value in the interval
                                if not table[k]: v = (t,None)
                                else: v = rule(*[table[k][-1],r,tt]) #Generating value from previous/next
                            break
                        #therefore, r[0]<=(t+resolution)
                        else: i,v = i+1,(t,r[1])
                        ## A more ellaborated election (e.g. to maximize change)
                        #elif v is None: 
                           #i,v = i+1,(t,r[1])
                        #else:
                           #i,v = i+1,rule(*[v,r,tt])
                else: #Filling table with Nones
                    v = (t+resolution,None)
                table[k].append((tt,v[1]))

            self.log.info('\t%s values in table'%(len(table[k])))
        self.log.info('Values correlated in %d milliseconds'%(1000*(time.time()-start)))
        return table
                    
    #################################################################################################
    #################################################################################################
    
    def get_extractor_values(self, attribute, start_date, stop_date, decimate, asHistoryBuffer):
        """ Getting attribute values using Java Extractors """
        
        self.log.info('Using Java Extractor ...')
        try: 
            extractor = self.get_extractor(attribute=attribute)
            #self.clean_extractor(extractor)
            result = self.__extractorCommand(extractor,'GetAttDataBetweenDates',[attribute,start_date,stop_date])
            vattr,vsize=str(result[1][0]),int(result[0][0])
            time.sleep(0.2)
            if vattr not in [a.name for a in extractor.attribute_list_query()]:
                raise Exception,'%s_NotIn%sAttributeList'%(vattr,extractor.name())
            self.log.debug( '\treading last value of attribute %s'%vattr)
            last_value = extractor.read_attribute(vattr).value
            self.log.debug('\treading %s attribute history values of %s (last_value = %s)'% (vsize,vattr,last_value))
            history=extractor.attribute_history(vattr,vsize)
            if N>0: history = history[-N:]
            #DECIMATION IS DONE HERE ##########################################################################
            if decimate:
                nhist,l0 = [],len(history)
                for i,h in enumerate(history):
                    #if not i or h.value!=history[i-1].value or ((i+1)<l0 and history[i+1].value!=h.value) or h.time.tv_sec>=(300+nhist[-1].time.tv_sec):
                    if not i or data_has_changed(h_to_tuple(h),h_to_tuple(history[i-1]),h_to_tuple(history[i+1]) if i+1<l0 else None):
                        nhist.append(h)
                self.log.debug('\tIn get_attribute_values(%s,...).extractor: decimated repeated results ... %s -> %s'%(attribute,len(history),len(nhist)))
                history = nhist
            #Sorting extracted values
            try: history=[v for t,u,v in sorted((h.time.tv_sec,h.time.tv_usec,h) for h in history)]
            except Exception,e: self.log.error('Unable to sort history values: %s'%e)
            
            self.clean_extractor(extractor,vattr)
            self.attr_extracted[attribute]=(lambda s: s if ':' in s else self.tango_host+'/'+s)(extractor.name())
        except Exception,e: 
            self.log.warning( traceback.format_exc())
            raise Exception,'Archiving.Reader_ExtractorFailed(%s)!:%s' % (extractor.name(),str(e))
        if int(PyTango.__version__.split('.')[0])>=7:
            values = asHistoryBuffer and history or [(ctime2time(h.time),h.value) for h in history]
            self.last_reads = history and (ctime2time(history[0].time),ctime2time(history[-1].time)) or (1e10,1e10)
        else:
            values = asHistoryBuffer and history or [(ctime2time(h.value.time),h.value.value) for h in history]
            self.last_reads = history and (ctime2time(history[0].value.time),ctime2time(history[-1].value.time)) or (1e10,1e10)

        return values
        
    def clean_extractor(self,extractor,vattr=None):
        ''' removing dynamic attributes from extractor devices ...'''
        #self.log.debug('In PyTangoArchiving.Reader.__cleanExtractor(): removing dynamic attributes')
        self.log.debug( 'In PyTangoArchiving.Reader.__cleanExtractor(): removing dynamic attributes')
        self.log.debug( '%s(%s)'%(type(extractor),extractor) )
        if hasattr(extractor,'dev_name'):
            name,proxy=extractor.dev_name(),extractor
        else: 
            name,proxy=str(extractor),self.servers.proxies[str(extractor)]
        if vattr: proxy.RemoveDynamicAttribute(vattr)
        else: proxy.RemoveDynamicAttributes()
        
    #def __initMySQLconnection(self):
        #try: self.db = MySQLdb.connect(db=self.db_name,host=self.host,user=self.user,passwd=self.passwd)
        #except Exception,e:
            #self.log.error( 'Unable to create a MySQLdb connection to "%s"@%s.%s: %s'%(self.user,self.host,self.db_name,traceback.format_exc()))
            #self.db = None
            
    def __extractorCommand(self,extractor=None,command='',args=[]):
        if not command: raise Exception,'Reader__extractorCommand:CommandArgumentRequired!'
        if not extractor: extractor = self.get_extractor()
        extractor.ping()        
        try:
            self.log.debug( 'in damn Reader.__extractorCommand: calling HdbExtractor(%s).%s(%s)'%(extractor.name(),command,args))
            result = extractor.command_inout(*([command]+(args and [args] or [])))
        except PyTango.DevFailed, e:
            #e.args[0]['reason'],e.args[0]['desc'],e.args[0]['origin']
            reason = '__len__' in dir(e.args[0]) and e.args[0]['reason'] or e.args[0]
            if 'Broken pipe' in str(reason):
                extractor.init()
                result = extractor.command_inout(*([command]+(args and [args] or [])))
            elif 'MEMORY_ERROR' in str(reason):
                #raise Exception,'Extractor_%s'%reason
                self.clean_extractor(extractor.name())
                extractor.init()
                result = extractor.command_inout(*([command]+(args and [args] or [])))
            else:
                self.log.warning(traceback.format_exc())
                raise Exception,'Reader__extractorCommand:Failed(%s)!'% str(e)
        #self.log.debug( 'in Reader.__extractorCommand: command finished')
        return result
            
#################################################################################################
# Multiprocess Class for Reader
#################################################################################################

class ReaderByBunches(Reader):
    """
    Class that splits in bunches every query done against the database.
    It allows only database queries; not extractor
    """
    DEFAULT_BUNCH_SIZE = 1000
    def init_buncher(self):
        self._process_event,self._threading_event,self._command_event = multiprocessing.Event(),threading.Event(),threading.Event()
        self._receiver = threading.Thread(target=self._receive_data)
        self._receiver.daemon = True #Therefore, keep_alive routines should not be needed!
        self._last_alive = time.time()
        self.callbacks = defaultdict(list)
        
    def __del__(self):
        self.stop()
        type(self).__base__.__del__(self)
    
    def start(self):
        self._reader.start()
        self._receiver.start()
        self.get_attributes()
    def stop(self):
        self.log.info('ReaderProces().stop()')
        self._process_event.set(),self._threading_event.set()
        self._pipe1.close(),self._pipe2.close() 
    def alive(self):
        if not self._reader.is_alive():
            raise Exception('ReaderProcess is not Alive!!! (last contact at %s)'%time.ctime(self._last_alive))
        self._last_alive = time.time()
        return self._last_alive
    
    # Protected methods
    @staticmethod
    def get_key(d):
        return str(sorted(d.items()))
    
    def _receive_data(self):
        """
        It will be receive data who will really process the data
        In that process, first check the cache, if the query is contained by a cached array, just return it
        Then, for each query, split in bunches and with each result launch callback and update cache.
        Finally, proceed with the next queried value
        """
        while not self._threading_event.is_set():
            try:
                #self.log.info('... ReaderThread: Polling ...')
                assert self._reader.is_alive()
                if self._pipe1.poll(0.1):
                    #self.log.info('... ReaderThread: Receiving ... (%s)'%(ReaderProcess.__instances.keys()))
                    key,query = self._pipe1.recv()
                    if key.lower() in self.asked_attributes:
                        #Updating last_dates dictionary
                        self.debug('... ReaderThread: Received %s last_dates %s'%(key,query))
                        self.last_dates[key] = query
                    else:
                        self.log.info('... ReaderThread: Received data = %s [%s]; %s queries pending'%(
                            key,isSequence(query) and len(query) or type(query),len(self.callbacks)))
                        for callback in self.callbacks[key]:
                            try:
                                self.debug('\tlaunching callback %s'%callback)
                                callback(query)
                            except:
                                self.warning('\tError in %s callback %s!'%(key,callback))
                                self.warning(traceback.format_exc())
                        self.callbacks.pop(key)
            except Exception,e:
                self.warning('\tError in thread!\n%s'%(traceback.format_exc()))
            self._threading_event.wait(0.1)
        self.log.info('Exiting PyTangoArchiving.ReaderProcess()._receive_data thread')
        
    def _send_query(self,key,query,callback):
        assert self.alive()
        if key not in self.callbacks: 
            self.callbacks[key] = [callback]
            self._pipe1.send((key,query))
        elif callback not in self.callbacks[key]: 
            self.callbacks[key].append(callback)
        return

    def get_attribute_values(self,attribute,callback,start_date,stop_date=None,
                             asHistoryBuffer=False,decimate=False,notNone=False,N=0):
        """This method should be capable of>
         - cut queries in pieces, 
         - execute a callback for each of 
         - but, keep the complete query in cache (for reading arrays)
        
        This method reads values for an attribute between specified dates.
        This method may use MySQL queries or an H/TdbExtractor DeviceServer to get the values from the database.
        The format of values returned is [(epoch,value),]
        The flag 'asHistoryBuffer' forces to return the rawHistBuffer returned by the DS.
                
        :param attributes: list of attributes
        :param start_date: timestamp of the first value
        :param stop_date: timestamp of the last value
        :param asHistoryBuffer: return a history buffer object instead of a list (for trends)
        :param N: if N>0, only the last N values will be returned
        :param decimate: remove repeated values, False by default but True when called from trends
        
        :return: a list with values (History or tuple values depending of args)

        """
        #Previous implementation was discarded due to this exception
        #raise Exception("MySQLdb.SSCursor.fetchmany failed due to (2013, 'Lost connection to MySQL server during query')")
        assert self.alive()
        """
        This method will just put the query in the queue
        """
        decimate,window = decimate if isSequence(decimate) else (decimate,'0')
        if callable(decimate): decimate = decimate.__module__+'.'+decimate.__name__
        query = {'attribute':attribute,'start_date':start_date,'stop_date':stop_date,'asHistoryBuffer':asHistoryBuffer,'decimate':(decimate,window),'N':N}
        assert hasattr(callback,'__call__'),'2nd argument must be callable'
        self.asked_attributes.append(attribute.lower())
        key = self.get_key(query)
        self.log.info('thread.send_query(%s)'%key)
        self._send_query(key,query,callback)

    def get_attributes_values(self,attributes,callback,start_date,stop_date=None,
            correlate=False,asHistoryBuffer=False,trace = False, text = False, N=0
            ):
        """
        Works like Reader.get_attributes_values, but 2nd argument must be a callable to be executed with the values received as argument
        """
        assert self.alive()
        query = {'attributes':attributes,'start_date':start_date,'stop_date':stop_date,'correlate':correlate,'asHistoryBuffer':asHistoryBuffer,'trace':trace,'text':text,'N':N}
        assert hasattr(callback,'__call__'),'2nd argument must be callable'
        [self.asked_attributes.append(a.lower()) for a in attributes]
        self._send_query(self.get_key(query),query,callback)

class ReaderProcess(Logger,SingletonMap): #,Object,SingletonMap):
    """
    Class that provides a multiprocessing interface to PyTangoArchiving.Reader class
    """
    ALIVE_PERIOD = 3
    
    @classmethod
    def parse_instance_key(cls,*p,**k):
        key = ','.join(p) if p else ''
        if 'tango_host' in k: key+=':'+(k['tango_host'] or get_tango_host())
        if 'db' in k: key+=':'+k['db']
        if 'config' in k: key+=':'+k['config']
        if 'schema' in k: key+=':'+k['schema']
        if not key: 
            key = SingletonMap.parse_instance_key(cls,*p,**k)
        return key
    
    def __init__(self,db='*',config='',servers = None, schema = None,
            timeout=300000,log='INFO',logger=None,tango_host=None,alias_file=''):
        
        import multiprocessing
        import threading
        from collections import defaultdict
        Logger.__init__(self,'ReaderProcess')
        self.logger = logger
        if self.logger: [setattr(self,f,getattr(logger,f)) for f in ('debug','info','warning','error')]
        self.info('In ReaderProcess(%s)'%([db,config,servers,schema,timeout,log,logger,tango_host,alias_file]))

        #Reader Part
        self.available_attributes,self.failed_attributes,self.asked_attributes = [],[],[]
        self.last_dates = defaultdict(lambda:(1e10,0))
        self.updated,self.last_retry = 0,0
        self.tango_host = tango_host or os.getenv('TANGO_HOST')
        self.tango = PyTango.Database(*self.tango_host.split(':'))

        try:
            alias_file = alias_file or get_alias_file()       
            self.alias = alias_file and read_alias_file(alias_file)
        except Exception,e: 
            self.log.warning('Unable to read alias file %s: %s'%(alias_file,e))

        if schema is not None and db=='*': db = schema
        if any(s in ('*','all') for s in (db,schema)): db,schema = '*','*'
        self.state = PyTango.DevState.INIT
        self.schema = schema
        
        #Process Part
        self._pipe1,self._pipe2 = multiprocessing.Pipe()
        self._process_event,self._threading_event,self._command_event = multiprocessing.Event(),threading.Event(),threading.Event()
        self._local = Reader(db,config,servers,schema,tango_host=tango_host,alias_file=alias_file,logger=logger)
        logger = None #It is harmful to use it on Processes
        self._reader = multiprocessing.Process(
            target=self._reader_process,
            args=(db,config,servers,schema,timeout,log,None,tango_host,alias_file,
                self._pipe2,self._process_event)
            )
        self._receiver = threading.Thread(target=self._receive_data)
        self._reader.daemon,self._receiver.daemon = True,True #Therefore, keep_alive routines should not be needed!
        self._last_alive = time.time()
        self.callbacks = defaultdict(list)
        #Overriding all missing Reader members
        self.configs = self._local.configs
        self.start()
    
    def __del__(self):
        self.stop()
        type(self).__base__.__del__(self)
    
    def start(self):
        self._reader.start()
        self._receiver.start()
        self.get_attributes()
    def stop(self):
        self.info('ReaderProces().stop()')
        self._process_event.set(),self._threading_event.set()
        self._pipe1.close(),self._pipe2.close() 
    def alive(self):
        if not self._reader.is_alive():
            raise Exception('ReaderProcess is not Alive!!! (last contact at %s)'%time.ctime(self._last_alive))
        self._last_alive = time.time()
        return self._last_alive
    
    # Protected methods
    @staticmethod
    def get_key(d):
        return str(sorted(d.items()))
    
    @staticmethod
    def _reader_process(db,config,servers,schema,timeout,log,logger,tango_host,alias_file,pipe,event,alive=ALIVE_PERIOD):
        
        reader = Reader(db=db,config=config,servers=servers,schema=schema,timeout=timeout,
                        log=log,logger=logger,
                        tango_host=tango_host,alias_file=alias_file,
                        )
        self = Logger()
        reader.log.setLogLevel('INFO')
        last_alive = time.time()
        while not event.is_set(): #and (pipe.poll() or time.time()<(last_alive+2*alive)): #Alive should not be needed if process is daemonic
            if pipe.poll(.1):
                self.debug('PyTangoArchiving.ReaderProcess(): receiving ...')
                key,query = pipe.recv()
                try:
                    if time.time()<(last_alive+2*alive): #if key=='ALIVE':
                        if not check_process(os.getppid()):
                            self.error('PARENT PROCESS %s NOT RUNNING, SHUTDOWN!'%os.getppid())
                            event.set()
                        last_alive = time.time()
                    if hasattr(getattr(reader,key,None),'__call__'):
                        #Executing a Reader object method
                        self.info('PyTangoArchiving.ReaderProcess(): launching command %s(%s)'%(key,query))
                        pipe.send((key,getattr(reader,key)(*query)))
                    else:
                        #Executing a get_attribute(s)_values call, when finished it also sends last_dates all requested attributes.
                        if 'attributes' in query: 
                            self.info('PyTangoArchiving.ReaderProcess(): launching query %s'%(query))
                            values = reader.get_attributes_values(**query)
                            pipe.send((key,values))
                        else: 
                            self.info('PyTangoArchiving.ReaderProcess(): launching query %s'%(query))
                            values = reader.get_attribute_values(**query)
                            pipe.send((key,values))
                        self.info('PyTangoArchiving.ReaderProcess(): sending query %s [%d]'%(query,len(values)))
                        [pipe.send((a,reader.get_last_attribute_dates(a))) for a in query.get('attributes',[query['attribute']])]
                    values = []
                except:
                    self.info('\tError in %s process!\n%s'%(key,traceback.format_exc()))
                    pipe.send((key,None))
            event.wait(0.1)
        self.info( 'Exiting PyTangoArchiving.ReaderProcess()._reader_process: event=%s, thread not alive for %d s' % (event.is_set(),time.time()-last_alive))
        
    def _receive_data(self):
        while not self._threading_event.is_set():
            try:
                #self.info('... ReaderThread: Polling ...')
                assert self._reader.is_alive()
                if self._pipe1.poll(0.1):
                    #self.info('... ReaderThread: Receiving ... (%s)'%(ReaderProcess.__instances.keys()))
                    key,query = self._pipe1.recv() #Here it is where the reader hungs
                    if key.lower() in self.asked_attributes:
                        #Updating last_dates dictionary
                        self.debug('... ReaderThread: Received %s last_dates %s'%(key,query))
                        self.last_dates[key] = query
                    else:
                        self.info('... ReaderThread: Received data = %s [%s]; %s queries pending'%(
                            key,isSequence(query) and len(query) or type(query),len(self.callbacks)))
                        for callback in self.callbacks[key]:
                            try:
                                self.debug('\tlaunching callback %s'%callback)
                                callback(query)
                            except:
                                self.warning('\tError in %s callback %s!'%(key,callback))
                                self.warning(traceback.format_exc())
                        self.callbacks.pop(key)
            except Exception,e:
                self.warning('\tError in thread!\n%s'%(traceback.format_exc()))
            self._threading_event.wait(0.1)
        self.info('Exiting PyTangoArchiving.ReaderProcess()._receive_data thread')
        
    def _send_query(self,key,query,callback):
        assert self.alive()
        if key not in self.callbacks: 
            self.callbacks[key] = [callback]
            self._pipe1.send((key,query))
        elif callback not in self.callbacks[key]: 
            self.callbacks[key].append(callback)
        return
        
    def _remote_command(self,command,args=None):
        assert self.alive()
        self._return,args = None,args or []
        self._send_query(command,args,lambda q,e=self._command_event,s=self:(setattr(s,'_return',q),e.set()))
        while not self._command_event.is_set(): self._command_event.wait(.02)
        self._command_event.clear()
        return self._return
        
    # Public methods
    #@Cached(depth=10,expire=60.)
    def check_state(self,period=300):
        """ It tries to reconnect to extractors every 5 minutes. """
        assert self.alive()
        if (time.time()-self.last_retry)>period: 
            self.last_retry = time.time()
            self.state = self._remote_command('check_state')
        return (self.state!=PyTango.DevState.FAULT)

    @Cached(depth=10,expire=60.)
    def get_attributes(self,active=False):
        """ Queries the database for the current list of archived attributes."""
        return self._local.get_attributes(active=active)
    
    def is_attribute_archived(self,attribute,active=False):
        """ This method uses two list caches to avoid redundant device proxy calls, launch .reset() to clean those lists. """
        #return self._remote_command('is_attribute_archived',[attribute,active])
        return self._local.is_attribute_archived(attribute)
    def reset(self):
        return self._remote_command('reset',[])
    def get_last_attribute_dates(self,attribute):
        return self.last_dates[attribute]

    def get_attribute_values(self,attribute,callback,start_date,stop_date=None,
            asHistoryBuffer=False,decimate=False,notNone=False,N=0,
            cache=True,fallback=True):
        """
        Works like Reader.get_attribute_values, but 2nd argument must be a 
        callable to be executed with the values received as argument
        """
        assert self.alive()
        decimate,window = decimate if isSequence(decimate) else (decimate,'0')
        if callable(decimate): 
            decimate = decimate.__module__+'.'+decimate.__name__
        query = {'attribute':attribute,'start_date':start_date,
                 'stop_date':stop_date,'asHistoryBuffer':asHistoryBuffer,
                 'decimate':(decimate,window),'notNone':notNone,'N':N,
                 'cache':cache,'fallback':fallback}
        assert hasattr(callback,'__call__'),'2nd argument must be callable'
        self.asked_attributes.append(attribute.lower())
        key = self.get_key(query)
        self.info('thread.send_query(%s)'%key)
        self._send_query(key,query,callback)
        
    def get_attributes_values(self,attributes,callback,start_date,stop_date=None,
            correlate=False,asHistoryBuffer=False,trace = False, text = False,
            N=0
            ):
        """
        Works like Reader.get_attributes_values, but 2nd argument must be a 
        callable to be executed with the values received as argument
        """
        assert self.alive()
        query = {'attributes':attributes,'start_date':start_date,
                 'stop_date':stop_date,'correlate':correlate,
                 'asHistoryBuffer':asHistoryBuffer,'trace':trace,
                 'text':text,'N':N}
        assert hasattr(callback,'__call__'),'2nd argument must be callable'
        [self.asked_attributes.append(a.lower()) for a in attributes]
        self._send_query(self.get_key(query),query,callback)


__test__['Reader.export_to_text'] = {'result':'date;time;attribute.1970-01-01_01:00:01.000;1;3,4,5.',
        'args':[[(1,[3,4,5])]],'kwargs':{'sep':';','arrsep':',','linesep':'.'}}
