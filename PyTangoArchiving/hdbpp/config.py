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

import PyTangoArchiving
from PyTangoArchiving.dbs import ArchivingDB
from PyTangoArchiving.common import CommonAPI
from PyTangoArchiving.reader import Reader
from PyTangoArchiving.utils import CatchedAndLogged
from PyTangoArchiving.schemas import Schemas

import fandango as fn
from fandango.objects import SingletonMap, Cached
from fandango.tango import *
import MySQLdb,traceback,re
from PyTango import AttrQuality

__test__ = {}

def get_search_model(model):
    if model.count(':')<2:
        model = '%/'+model
    model = model.split('[')[0]
    model = clsub('[:][0-9]+','%:%',model)
    return model

class HDBppDB(ArchivingDB,SingletonMap):
    """
    Python API for accessing HDB++
    
    This API is not split in config and reader part to allow to use
    methods from both devices or database.
    
    If devices are running, they are used by default to acquire archiving
    status.
    """
    
    def __init__(self,db_name='',host='',user='',
                 passwd='', manager='',
                 other=None, port = '3306',
                 log_level = 'WARNING'):
        """
        Configuration can be loaded from PyTangoArchiving.Schemas,
        an HdbConfigurationManager or another DB object.
        
        api = pta.HDBpp(db_name,host,user,passwd,manager)
        """
        assert db_name or manager, 'db_name/manager argument is required!'
        self.tango = get_database()
        self.schema = db_name

        if not all((db_name,host,user,passwd)):
            if other:
                print('HDBpp(): Loading from DB object')
                db_name,host,user,passwd = \
                    other.db_name,other.host,other.user,other.passwd
            elif manager:
                print('HDBpp(): Loading from manager')
                d,h,u,p = HDBpp.get_db_config(manager=manager,db_name=db_name)
                db_name = db_name or d
                host = host or h    
                user = user or u
                passwd = passwd or p                
            else:
                sch = Schemas.getSchema(self.schema)
                if sch:
                    #print('HDBpp(): Loading from Schemas')
                    db_name = sch.get('dbname',sch.get('db_name'))
                    host = host or sch.get('host')
                    user = user or sch.get('user')
                    passwd = passwd or sch.get('passwd')
                    port = port or sch.get('port')
                    self.libname = sch.get('libname','')
                    self.schema = sch.get('schema',self.schema)
                elif not manager:
                    print('HDBpp(): Searching for manager')
                    m = self.get_manager(db_name)
                    t = HDBpp.get_db_config(manager=m,db_name=db_name)
                    host,user,passwd = t[1],t[2],t[3]

        self.port = port
        self.archivers = []
        self.attributes = {}
        self.dedicated = {}
        self.status = fn.defaultdict(list)
        try:
            self.default_cursor = MySQLdb.cursors.SSCursor
        except:
            self.default_cursor = None
        try:
            ArchivingDB.__init__(self,db_name,host,user,passwd,
                             default_cursor=self.default_cursor)
            self.setLogLevel(log_level)
        except:
            self.db = None
            traceback.print_exc()
            print('Unable to connect to database')            
        try:
            self.get_manager()
            self.get_attributes()
        except:
            traceback.print_exc()
            print('Unable to get manager')
    
    def check(self,method=None):
        method = method or self.get_data_types
        if fn.isString(method):
            return self.Query(method)
        else:
            return method()
            
    #@staticmethod
    def get_hdbpp_libname(self):
        if getattr(self,'libname',None):
            return self.libname
        try:
            self.get_manager()
            conf = get_device_property(self.manager,'LibConfiguration')            
            conf = dict(t.split('=',1) for t in conf)
            r = conf['libname']
        except:
            print('Unable to parse %s.LibConfiguration' % self.manager)
            r = fn.shell_command('locate libhdb++mysql.so')
        return r.split()[0]
    
    @staticmethod
    def get_all_databases(regexp='*'):
        """
        Return all registered HDB++ databases
        """
        managers = HDBpp.get_all_managers()
        dbs = [HDBpp.get_db_config(m) for m in managers]
        dbs = [d for d in dbs if fn.clsearch(regexp,str(d))]
        return [t[0] for t in dbs]
            
    @staticmethod
    def get_db_config(manager='', db_name=''):
        
        #if not manager:
            #manager = self.get_manager(db_name).name()
            
        prop = get_device_property(manager,'LibConfiguration')

        if prop:
            print('getting config from %s/LibConfiguration' % manager)
            config = dict((map(str.strip,l.split('=',1)) for l in prop))
            db_name,host,user,passwd = \
                [config.get(k) for k in 'dbname host user password'.split()]
        else:
            db_name = get_device_property(manager,'DbName') or ''
            host = get_device_property(manager,'DbHost') or ''
            user = get_device_property(manager,'DbUser') or ''
            passwd = get_device_property(manager,'DbPassword') or ''
              
        return db_name,host,user,passwd
        
    @staticmethod
    def get_all_managers():
        return get_class_devices('HdbConfigurationManager')
    
    @staticmethod
    def get_all_archivers():
        return get_class_devices('HdbEventSubscriber')
      
    def get_manager(self, db_name='', prop=''):
        """ Returns manager proxy, initializes from Tango DB if missing"""
        if not getattr(self,'manager',None):
            self.manager,db_name = '',db_name or getattr(self,'db_name','')
            #print(self.schema,db_name,self.host)
            for m in self.get_all_managers():
                propdb = str(get_device_property(m,'DbName'))
                host = str(get_device_property(m,'DbHost'))
                conf = get_device_property(m,'LibConfiguration') #list

                if ((propdb == db_name or 'dbname=%s'%db_name in conf)
                    and (host == self.host or 'host=%s'%self.host in conf
                         or host == 'localhost')):

                    #print(self.schema,db_name,propdb,self.host,host,conf)
                    self.manager = m
                    
        dp = get_device(self.manager,keep=True) if self.manager else None
        return dp
    
    @Cached(expire=60.)
    def get_subscribers(self, from_db = True, exclude = '*/null'):
        """
        If not got from_db, the manager may limit the list available
        """
        if from_db:
            p = list(self.tango.get_device_property(
                self.manager,'ArchiverList')['ArchiverList'])
        elif self.manager: # and check_device(self.manager):
            self.get_manager().state()
            p = self.get_manager().ArchiverList
        #else:
            #raise Exception('%s Manager not running'%self.manager)

        return [d for d in p if d.strip() and (
            not exclude or not fn.clmatch(exclude,d))]

    def get_archivers(self, *args, **kwargs):
        """ alias to get_subscribers """
        return self.get_subscribers(*args,**kwargs)
    
    @Cached(expire=10.)
    def get_archiver_attributes(self, archiver, from_db=False, full=False):
        """
        get_archiver_attributes(self, archiver, from_db=False, full=False):
        
        Obtain archiver AttributeList, either from TangoDB or a running device
        if from_db = True or full = True; the full config is returned
        """
        if full or from_db or not check_device_cached(archiver):
            self.debug('getting %s attributes from database' % archiver)
            attrs = [str(l) for l in 
                fn.toList(get_device_property(archiver,'AttributeList'))]
            if not full:
                attrs = [str(l).split(';')[0] for l in 
                    attrs]
        else:
            try:
                attrs = get_device(archiver, keep=True).AttributeList or []
            except:
                print('Unable to get %s attributes' % archiver)
                traceback.print_exc()
                attrs = []
        
        self.debug('%s archives %d attributes' % (archiver,len(attrs)))
        self.dedicated[archiver] = attrs
            
        return attrs
    
    @Cached(expire=60.)
    def get_archivers_attributes(self,archs=None,from_db=True,full=False):
        """
        get_archivers_attributes(self,archs=None,from_db=True,full=False):
        
        If not got from_db, the manager may limit the list available
        """        
        archs = archs or self.get_archivers()
        dedicated = fn.defaultdict(list)
        if from_db:
            for a in archs:
                dedicated[a] = [str(l) for l in 
                    get_device_property(a,'AttributeList')]
                if not full:
                    dedicated[a] = [str(l).split(';')[0] for l in 
                        dedicated[a]]
        else:
            for a in archs:
                try:
                    dedicated[a].extend(get_device(a, keep=True).AttributeList)
                except:
                    dedicated[a] = []
                    
        self.dedicated.update(dedicated)
        return dedicated    
        
    def get_subscriber_errors(self,archiver):
        try:
            dp = fn.get_device(archiver,keep=True)
            al = dp.AttributeList or []
            er = dp.AttributeErrorList or []
            return dict((a,e) for a,e in zip(al,er) if e)
        except:
            print('Unable to get %s errors' % archiver)
            return {}
        
    get_archiver_errors = get_subscriber_errors
    
    def get_attribute_errors(self,attribute):
        """
        This method get attribute errors from its current archiver
        """
        archiver = self.get_attribute_archiver(attribute)
        errors = self.get_archiver_errors(archiver)
        return errors.get(attribute,None)
    
    def get_loads(self, use_freq=False):
        return dict((d,self.get_archiver_load(d,use_freq))
            for d in self.get_subscribers())
    
    def get_archiver_load(self,archiver,use_freq=True):
        """
        returns the estimated load of an archiver, in frequency of records or number
        of attributes
        
        if use_freq=True, returns attribute record frequency (60s period)
        if false, returns attribute list size
        the attribute list size counts for the time/stress needed
        to subscribe the attributes
        """
        if use_freq:
            return fn.tango.read_attribute(archiver+'/attributerecordfreq')
        else:
            return len(self.get_archiver_attributes(archiver,from_db=False))
        
    def get_attribute_freq(self,attribute, from_db=False, n=10):
        """
        This method get attribute frequency  (60s period) 
        from its current archiver and divides per 60.
        
        if from_db, it will query n values to calcullate the right frequency
        """
        if from_db:
            vals = self.load_last_values(attribute,n=n)[attribute]
            return float(n)/abs(vals[0][0]-vals[-1][0])
        else:
            attribute = self.is_attribute_archived(attribute)
            archiver = fn.get_device(self.get_attribute_subscriber(attribute),keep=True)
            freqs = dict(zip(archiver.AttributeList,archiver.AttributeRecordFreqList))
            return freqs.get(attribute,0.)/60.
    
    def get_next_archiver(self,errors=False,use_freq=False, attrexp=''):
        """
        errors/use_freq are used to compute the archiver load
        attrexp can be used to get archivers already archiving attributes
        """
        props = dict((a,fn.tango.get_device_property(a,'AttributeFilters'))
                     for a in self.get_archivers()) #get_archivers filters null
        if any(props.values()):
            archs = [a for a,v in props.items() if not v]
        else:
            archs = [a for a in props if fn.clmatch('*[0-9]$',a)]

        loads = dict((a,self.get_archiver_load(a,use_freq=use_freq))
            for a in archs)
        if errors:
            # Errors count twice as load
            for a,v in loads.items():
                errs = self.get_archiver_errors(a)
                loads[a] += 10*len(errs)

        if not len(loads):
            self.warning('No free archivers found!')
        elif attrexp:
            attrs = [a for a in self.get_attributes(True) 
                     if fn.clmatch(attrexp,a)]
            archs = [self.get_attribute_subscriber(a) for a in attrs]
            if any(a in loads for a in archs):
                loads = dict((k,v) for k,v in loads.items() if k in archs)

        loads = sorted((v,k) for k,v in loads.items())
        return loads[0][-1]

    @Cached(depth=2,expire=60.)
    def get_attributes(self,active=None,regexp=''):
        """
        Alias for Reader API
        """
        if active:
            r = self.get_archived_attributes()
        else:
            # Inactive attributes must be read from Database
            r = self.get_attribute_names(False)

        r = sorted(set(fn.tango.get_full_name(a,fqdn=True).lower()
                          for a in r))

        return sorted(fn.filtersmart(r,regexp) if regexp else r)
        
    def get_attribute_names(self,active=False,regexp=''):
        t0 = fn.now()
        if not active:
            attributes = [a[0].lower() for a in self.Query('select att_name from att_conf')]
            [self.get_attr_id_type_table(a) for a in attributes if a not in self.attributes]
            r = self.attributes.keys()
        else:
            r = self.get_archived_attributes()

        r = sorted(fn.filtersmart(r,regexp) if regexp else r)
        self.debug('get attribute names took %d ms' % (1e3*(fn.now()-t0)))
        return r
        
    @Cached(expire=86400)
    def get_data_types(self):
        return [l[0] for l in self.Query(
            "select data_type from att_conf_data_type")]
    
    def get_data_tables(self):
        return sorted('att_'+t for t in self.get_data_types())

    @Cached(depth=100, expire=60.)
    def get_attributes_by_table(self,table='',as_id=False):
        if table:
            table = table.replace('att_','')
            r = self.Query(
                "select att_name,att_conf_id from att_conf,att_conf_data_type where "
                "data_type like '%s' and att_conf.att_conf_data_type_id "
                "= att_conf_data_type.att_conf_data_type_id" % table)
            return [l[as_id] for l in r]
        else:
            types = self.Query("select data_type,att_conf_data_type_id "
                "from att_conf_data_type")
            w = 'att_conf_id' if as_id else 'att_name'
            return dict(('att_'+t,self.Query("select %s from att_conf"
                "  where att_conf_data_type_id = %s" % (w,i))) for t,i in types)        
        
    @Cached(depth=10,expire=60.)
    def get_subscribed_attributes(self,search=''):
        """
        It gets attributes currently assigned to subscribers and updates
        internal attribute/archiver index.
        
        DONT USE Manager.AttributeSearch, it is limited to 1024 attrs!
        """
        #print('get_archived_attributes(%s)'%str(search))
        attrs = []
        if self.db is not None:
            self.get_att_conf_table()
        [self.get_archiver_attributes(d,from_db=True) 
            for d in self.get_subscribers()] #/null is excluded here
        
        for d,dattrs in self.dedicated.items():
            for a in dattrs:
                if a not in self.attributes:
                    self.get_attr_id_type_table(a)
                self.attributes[a].archiver = d
                if not search or fn.clsearch(search,a):
                    attrs.append(a)
        return attrs        
    
    def get_stopped_attributes(self, errors=False, killed=False):
        r = []
        for d in self.get_subscribers(exclude='*null'): #get_subscribers filters null
            try:
                dp = fn.get_device(d,keep=True)
                l = dp.AttributeStoppedList
                if l:
                    r.extend(l)
                if errors:
                    self.debug('adding %s error list' % d)
                    r.extend(self.get_archiver_errors(d).keys())
            except:
                if not fn.check_device(d):
                    self.warning('%s not running!\n%s' % (
                        d,traceback.format_exc()))
                traceback.print_exc()
                if killed:
                    r.extend(self.get_archiver_attributes(d,from_db=True))
        return r
    
    def get_archived_attributes(self, *args, **kwargs):
        """
        alias to get_subscribed_attributes, to be overloaded in subclasses
        
        It gets attributes currently assigned to subscribers and updates
        internal attribute/archiver index.
        
        @param search: use it as a filter
        
        DONT USE Manager.AttributeSearch, it is limited to 1024 attrs!        
        """
        if self.db is not None:
            self.get_att_conf_table()
        return self.get_subscribed_attributes(*args, **kwargs)
 
    def get_attribute_ID(self,attr):
        # returns only 1 ID
        return self.get_attributes_IDs(attr,as_dict=0)[0][1]
      
    def get_attributes_IDs(self,name='%',as_dict=1):
        # returns all matching IDs
        name = name.replace('*','%')
        ids = self.Query("select att_name,att_conf_id from att_conf "\
            +"where att_name like '%s'"%get_search_model(name))
        if not ids: return None
        elif not as_dict: return ids
        else: return dict(ids)
    
    def get_attribute_by_ID(self,ID):
        try:
            ids = []
            ids = self.Query("select att_name,att_conf_id from att_conf "\
                +"where att_conf_id = %s" % ID)
            return ids[0][0]
        except:
            print(ids)
            raise Exception('wrong ID %s' % ID)
      
    def get_table_name(self,attr):
        return self.get_attr_id_type_table(attr)[-1]

    @Cached(expire=600)
    def get_att_conf_table(self):
        t0 = fn.now()
        #types = self.Query('select att_conf_data_type_id, data_type from att_conf_data_type')
        #types = dict(types)
        q = "select att_name,att_conf_id,att_conf.att_conf_data_type_id,data_type "
        q += " from att_conf, att_conf_data_type where "
        q += "att_conf.att_conf_data_type_id = att_conf_data_type.att_conf_data_type_id"
        ids = self.Query(q)
        #self.debug(str((q, ids)))
        #ids = [list(t)+[types[t[-1]]] for t in ids]
        for i in ids:
            attr,aid,tid,table = i
            self.attributes[attr] = fn.Struct()            
            self.attributes[attr].id = aid
            self.attributes[attr].tid = tid
            self.attributes[attr].type = table
            self.attributes[attr].table = 'att_'+table
            self.attributes[attr].modes = {'MODE_E':True}

        return ids

    @Cached(depth=20000,expire=3600)
    def get_attr_id_type_table(self,attr):
        if fn.isString(attr):
            attr = fn.tango.get_full_name(attr,True).lower()

        try:
            s = self.attributes[attr]
            return s.id, s.type, s.table
        except:
            self.get_att_conf_table.cache.clear()
            self.get_att_conf_table()

            if attr not in self.attributes:
                return None,None,''
            else:
                s = self.attributes[attr]
                return s.id, s.type, s.table
    
    @Cached(depth=1000,expire=60.)
    def get_attribute_subscriber(self,attribute):
        if not self.dedicated:
            [self.get_archiver_attributes(d) 
             for d in self.get_archivers(exclude='')]

        #m = parse_tango_model(attribute,fqdn=True).fullname
        m = get_full_name(attribute,fqdn=True)
        for k,v in self.dedicated.items():
            for l in v:
                if m in l.split(';'):
                    return k
        
        return None
    
    get_attribute_archiver = get_attribute_subscriber
    
    def is_attribute_archived(self,attribute,active=None,cached=True):
        # @TODO active argument not implemented
        model = parse_tango_model(attribute,fqdn=True)
        d = self.get_manager()
        if d and cached:
            self.get_archived_attributes()
            l = map(str.lower,self.attributes)
            ms =  map(str.lower,(attribute,model.fullname,model.normalname))
            if ms[0] in l or ms[1] in l:
                return model.fullname
            else:
                return False
        elif d:
            attributes = d.AttributeSearch(model.fullname)
            a = [a for a in attributes if a.lower().endswith(attribute.lower())]
            if len(attributes)>1: 
                raise Exception('MultipleAttributesMatched!')
            if len(attributes)==1:
                return attributes[0]
            else:
                return False
        else:
            for a in self.get_attributes():
                index = '['+attribute.split('[',1)[-1] if '[' in attribute else ''
                if a.endswith('/'+attribute.split('[')[0].lower()):
                    return a+index
                if a == model.fullname:
                    return a+index
            return False
        
    def is_attribute_subscribed(self,attribute,exclude='.*/null'):
        """
        checks if attribute is archived by a valid subscriber
        """
        s = self.get_attribute_subscriber(attribute)
        return s and not fn.clmatch(exclude,str(s))
    
    def start_servers(self,host='',restart=True):
        """
        this method starts all servers processes

        :param host:
        :param restart:
        :return:
        """
        import fandango.servers
        if not self.manager: self.get_manager()
        astor = fandango.servers.Astor(self.manager)
        if restart:
            astor.stop_servers()
            time.sleep(10.)
        print('Starting manager ...')
        astor.start_servers(host=(host or self.host))
        time.sleep(1.)
        
        astor = fandango.servers.Astor()
        devs = self.get_archivers()
        astor.load_from_devs_list(devs)
        if restart:
            astor.stop_servers()
            time.sleep(10.)
        print('Starting archivers ...')
        astor.start_servers(host=(host or self.host))
        time.sleep(3.)
        self.start_devices(force=True)
        
    def start_devices(self,regexp = '*', dev_list = [], force = False, 
                      do_init = False, do_restart = False):
        """
        this method starts servers if needed and launches command Start()

        :param regexp: filter archivers by regexp
        :param dev_list: list of devices to restart
        :param force: execute an Start() command
        :param do_init: execute an Init()
        :param do_restart: restart devices using Starter
        :return:
        """
        devs = dev_list if dev_list else self.get_archivers() #get_archivers filters null
        devs = fn.filtersmart(devs,regexp) if regexp else devs
        off = sorted(set(d for d in devs if not fn.check_device(d)))

        if off and do_restart:
            print('Restarting %s Archiving Servers ...'%self.db_name)
            astor = fn.Astor()
            astor.load_from_devs_list(list(off))
            astor.stop_servers()
            fn.wait(3.)
            astor.start_servers()
            fn.wait(3.)

        for d in devs:
            try:
                dp = fn.get_device(d, keep=True)
                if do_init:
                    dp.init()
                if force and dp.attributenumber != dp.attributestartednumber:
                    off.append(d)
                    print('%s.Start()' % d)
                    dp.start()
            except Exception,e:
                self.warning('start_archivers(%s) failed: %s' % (d,e))
                
        return off        
    
    def add_archiving_manager(self,srv,dev,libname=None):
        if '/' not in srv: srv = 'hdb++cm-srv/'+srv
        libname = libname or self.get_hdbpp_libname()
        add_new_device(srv,'HdbConfigurationManager',dev)
        prev = get_device_property(dev,'ArchiverList') or '' #Weird, but needed
        fn.put_device_property(dev,'ArchiverList',prev)
        #put_device_property(dev,'ArchiverList',prev)
        put_device_property(dev,'DbHost',self.host)
        put_device_property(dev,'DbName',self.db_name)
        #put_device_property(dev,'DbUser',self.user)
        #put_device_property(dev,'DbPassword',self.passwd)
        #put_device_property(dev,'DbPort','3306')
        #put_device_property(dev,'ArchiveName','MySQL')        
        put_device_property(dev,'LibConfiguration',[
            'libname='+libname,
            'lightschema=1',
            'user='+self.user,
            'password='+self.passwd,
            'port='+self.port,
            'host='+self.host,
            'dbname='+self.db_name,])
        self.get_manager()
        return dev

    def add_event_subscriber(self,srv,dev,libname=''):
        
        #if not fn.check_device(self.manager):
            #raise Exception('%s not running!' % self.manager)
        self.get_manager().state()
        
        if '/' not in srv: 
            srv = 'hdb++es-srv/'+srv
        libname = libname or self.get_hdbpp_libname()
        
        dev = parse_tango_model(dev,fqdn=True).fullname        
        add_new_device(srv,'HdbEventSubscriber',dev)
        manager,dp = self.manager,self.get_manager()
        props = Struct(get_matching_device_properties(manager,'*'))
        
        prev = get_device_property(dev,'AttributeList') or ''
        put_device_property(dev,'AttributeList',prev)
        put_device_property(dev,'DbHost',self.host)
        put_device_property(dev,'DbName',self.db_name)
        #put_device_property(dev,'DbUser',self.user)
        #put_device_property(dev,'DbPassword',self.passwd)
        #put_device_property(dev,'DbPort','3306')
        #put_device_property(dev,'DbStartArchivingAtStartup','true')
        
        put_device_property(dev,'LibConfiguration',[
          'user='+self.user,
          'password='+self.passwd,
          'port='+getattr(self,'port','3306'),
          'host='+self.host,
          'dbname='+self.db_name,
          'libname='+libname,
          'lightschema=1',
          ])
        if 'ArchiverList' not in props:
            props.ArchiverList = []
            
        #put_device_property(manager,'ArchiverList',
                            #list(set(list(props.ArchiverList)+[dev])))
        print(dev)
        dp.ArchiverAdd(dev)
        prev = fn.get_device_property(manager,'ArchiverList') #Weird, but needed
        prev = sorted(set(d for d in prev if d.strip()))
        fn.put_device_property(manager,'ArchiverList',prev)
        dp.init()
        return dev

    def add_attribute(self,attribute,archiver=None,period=0,
                      rel_event=None,per_event=None,abs_event=None,
                      code_event=False, ttl=None, start=False,
                      use_freq=True,clear=False,context='RUN'):
        """
        set _event arguments to -1 to ignore them and not modify the database
        
        code_event will be set to True if no other event is setup
        """
        attribute = parse_tango_model(attribute,fqdn=True).fullname
        if archiver:
            archiver = fn.tango.get_full_name(archiver,fqdn=True)
        archiver = archiver or self.get_next_archiver(
            use_freq=use_freq,attrexp=fn.tango.get_dev_name(attribute)+'/*')
        self.warning('add_attribute(%s, %s) to %s' 
                  % (attribute,archiver,self.db_name))
        config = get_attribute_config(attribute)
        #if 'spectrum' in str(config.data_format).lower():
            #raise Exception('Arrays not supported yet!')
        data_type = str(PyTango.CmdArgType.values[config.data_type])
        
        if str(self.get_attribute_subscriber(attribute)).endswith('/null'):
            self.stop_archiving(attribute)

        if not self.manager: 
            return False
      
        try:
            
            d = self.get_manager()
            d.lock()
            print('SetAttributeName: %s'%attribute)
            d.write_attribute('SetAttributeName',attribute)
            time.sleep(0.2)

            if period>0:
                d.write_attribute('SetPollingPeriod',period)

            if per_event not in (None,-1,0):
                d.write_attribute('SetPeriodEvent',per_event)

            if not any((abs_event,rel_event,code_event)):
                code_event = True

            if abs_event not in (None,-1,0):
                print('SetAbsoluteEvent: %s'%abs_event)
                d.write_attribute('SetAbsoluteEvent',abs_event)

            if rel_event not in (None,-1,0):
                d.write_attribute('SetRelativeEvent',rel_event)

            if ttl not in (None,-1):
                d.write_attribute('SetTTL',ttl)
                
            d.write_attribute('SetCodePushedEvent',code_event)
            
            d.write_attribute('SetStrategy',context)

            d.write_attribute('SetArchiver',archiver)
            time.sleep(.2)
            d.AttributeAdd()
          
            if start:
                try:
                    arch = archiver
                    self.info('%s.Start()' % (arch))
                    fn.get_device(arch, keep=True).Start()
                except:
                    traceback.print_exc()
                    
            if clear:
                self.clear_caches()
              
        except Exception,e:
            
            if 'already archived' not in str(e).lower():
                self.error('add_attribute(%s,%s,%s): %s'
                        %(attribute,archiver,period,
                            traceback.format_exc().replace('\n','')))
            else:
                self.warning('%s already archived!' % attribute)
            
            return False

        finally:
            #self.warning('unlocking %s ..'%self.manager)
            d.unlock()

        print('%s added'%attribute)
        
    def add_attributes(self,attributes,*args,**kwargs):
        """
        Call add_attribute sequentially with a 1s pause between calls
        :param start: True by default, will force Start() in related archivers
        See add_attribute? for more help on arguments
        """
        try:
            attributes = sorted(attributes)
            stops = []
            for a in attributes:
                if str(self.get_attribute_subscriber(a)).endswith('/null'):
                    stops.append(a)
            if stops:
                self.stop_archiving(stops)
                
            start = kwargs.get('start',True)
            devs = fn.defaultdict(list)
            [devs[fn.tango.get_dev_name(a)].append(a) for a in attributes]
            for dev,attrs in devs.items():
                arch = kwargs.get('archiver',None)
                arch = arch or self.get_next_archiver(attrexp=dev+'/*')
                for a in attrs:
                    kwargs['start'] = False #Avoid recursive start
                    try:
                        kwargs['clear'] = False
                        self.add_attribute(a,archiver=arch,*args,**kwargs)
                    except:
                        self.warning('add_attribute(%s) failed!\n%s' % 
                                    (a,traceback.format_exc()))
                    time.sleep(3.)

            self.clear_caches()
            
            if start:
                self.get_archivers_attributes();
                archs = set(map(self.get_attribute_subscriber,attributes))
                for h in archs:
                    try:
                        if h:
                            self.info('%s.Start()' % h)
                            fn.get_device(h, keep=True).Start()
                    except:
                        traceback.print_exc()
                
        except Exception,e:
            self.error('add_attributes(%s) failed!: %s'%(
                attributes,traceback.print_exc()))

        return        
          
    def start_archiving(self,attribute,archiver,period=0,
                      rel_event=None,per_event=None,abs_event=None,
                      code_event=False, ttl=None, start=False):
        """
        Method provided for compatibility with HDB/TDB API

        See HDBpp.add_attribute.__doc__ for a full description of arguments
        """
        try:
            if isSequence(attribute):
                for attr in attribute:
                    self.start_archiving(attr,archiver,period,rel_event,
                        per_event,abs_event,code_event,ttl,start)
                    time.sleep(1.)
            else:
                self.info('start_archiving(%s)'%attribute)
                d = self.get_manager()
                fullname = parse_tango_model(attribute,fqdn=True).fullname
                archiver = fn.tango.get_full_name(archiver,fqdn=True)
                
                if not self.get_attribute_subscriber(attribute):
                    
                    self.add_attribute(fullname,archiver=archiver,
                        period=period, rel_event=rel_event, 
                        per_event=per_event, abs_event=abs_event,
                        code_event=code_event, ttl=ttl, 
                        start=start,clear=True)
                    
                    if start:
                        time.sleep(5.)
                        fullname = self.is_attribute_archived(attribute,cached=0)
                if start:
                    d.AttributeStart(fullname)
                    
                return True
        except Exception,e:
            self.error('start_archiving(%s): %s'
                        %(attribute,traceback.format_exc().replace('\n','')))
        return False        
    
    def stop_archiving(self, attribute, clear=True):
        """
        This method will remove the attribute from an existing archiver
        """
        try:
            if fn.isSequence(attribute):
                [self.stop_archiving(a,clear=False) for a in attribute]
            else:
                attribute = self.is_attribute_archived(attribute)
                if attribute:
                    arch = self.get_attribute_subscriber(attribute)
                    self.warning('Removing %s from %s' % (attribute,arch))
                    self.get_manager().AttributeRemove(attribute)
                else:
                    self.warning('%s is not archived!' % attribute)
            if clear:
                self.clear_caches()
            return attribute
        except:
            self.warning('stop_archiving(%s) failed!: %s' %
                         (attribute, traceback.format_exc()))
            
    def set_attribute_context(self,attr,context):
        attr = self.is_attribute_archived(attr)
        curr = self.get_attribute_archiver(attr)
        dp = fn.get_device(curr,keep=True)
        dp.SetAttributeStrategy([attr,context])
        
    def get_attribute_context(self,attr):
        attr = self.is_attribute_archived(attr)
        curr = self.get_attribute_archiver(attr)
        dp = fn.get_device(curr,keep=True)
        return dp.GetAttributeStrategy(attr)  
        
    def get_archiver_context(self,archiver):
        dp = fn.get_device(archiver,keep=True)
        return dp.Context      
            
    def reassign_attribute(self,attr,subscriber,context=None,ttl=None):
        """
        moves an attribute from an existing subscriber to a new one
        """
        attr = self.is_attribute_archived(attr)
        curr = self.get_attribute_archiver(attr)
        dp = fn.get_device(curr,keep=True)
        if context is None:
            context = dp.GetAttributeStrategy(attr)
        if ttl is None:
            ttl = dp.GetAttributeTTL(attr)
        #dp.AttributeStop(attr)
        dp.AttributeRemove(attr)
        nw = fn.get_device(subscriber,keep=True)
        nw.AttributeAdd([attr,context,str(ttl)])
        nw.AttributeStart(attr)
        return True
    
    def restart_attribute(self,attr, d=''):
        """
        execute AttributeStop/Start on subscriber device
        """
        try:
            a = self.is_attribute_archived(attr)
            if not a:
                raise Exception('%s is not archived!' % attr)
            attr = a
            d = self.get_attribute_subscriber(attr)
            if d.endswith('/null'):
                print('%s archived by %s is ignored'% (attr,d))
                return False
                
            print('%s.restart_attribute(%s)' % (d,attr))
            dp = fn.get_device(d, keep=True)

            if not fn.check_device(dp):
                self.start_devices('(.*/)?'+d,do_restart=True)
                
            dp.AttributeStop(attr)
            fn.wait(3.)
            dp.AttributeStart(attr)
            return True
        except:
            print('%s.AttributeStart(%s) failed!'%(d,attr))
        
    def restart_attributes(self, attributes=None, from_db=False):
        if attributes is None:
            if from_db:
                attributes = self.get_attributes_not_updated()
            else:
                attributes = self.get_stopped_attributes()
        
        for a in attributes:
            try:
                self.restart_attribute(a)
            except Exception as e:
                print(e)
            
        print('%d attributes restarted' % len(attributes))

    def clear_caches(self,regexp='.*'): #'get*'
        self.info('Clear attribute lists caches ...')
        for m in dir(self):
            o = getattr(self,m)
            if fn.clmatch(regexp,m) and fn.isCallable(o) and hasattr(o,'cache'):
                #print('clearing %s cache' % str(m))
                getattr(self,m).cache.clear()
        self.dedicated = {}
        self.attributes = {}
    
    
    
    
