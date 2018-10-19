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
    model = clsub('[:][0-9]+','%:%',model)
    return model

class HDBpp(ArchivingDB,SingletonMap):
    
    def __init__(self,db_name='',host='',user='',
                 passwd='', manager='',
                 other=None, port = '3306'):
        """
        Configuration can be loaded from PyTangoArchiving.Schemas,
        an HdbConfigurationManager or another DB object.
        """
        assert db_name or manager, 'db_name/manager argument is required!'
        self.tango = get_database()

        if not all((db_name,host,user,passwd)):
            if other:
                print('HDBpp(): Loading from DB object')
                db_name,host,user,passwd = \
                    other.db_name,other.host,other.user,other.passwd
            elif manager:
                print('HDBpp(): Loading from manager')
                d,h,u,p = self.get_db_config(manager=manager,db_name=db_name)
                db_name = db_name or d
                host = host or h    
                user = user or u
                passwd = passwd or p                
            else:
                sch = Schemas.getSchema(db_name)
                if sch:
                    print('HDBpp(): Loading from Schemas')
                    db_name = sch.get('dbname',sch.get('db_name'))
                    host = host or sch.get('host')
                    user = user or sch.get('user')
                    passwd = passwd or sch.get('passwd')
                    port = port or sch.get('port')
                elif not manager:
                    print('HDBpp(): Searching for manager')
                    m = self.get_manager(db_name)
                    t = self.get_db_config(manager=m,db_name=db_name)
                    host,user,passwd = t[1],t[2],t[3]

        self.port = port
        self.archivers = []
        self.attributes = fn.defaultdict(fn.Struct)
        self.dedicated = {}
        ArchivingDB.__init__(self,db_name,host,user,passwd,)
        try:
            self.get_manager()
            self.get_attributes()
        except:
            traceback.print_exc()
            print('Unable to get manager')
            
    def keys(self):
        if not self.attributes:
            self.get_attributes()
        return self.attributes.keys()
    
    def has_key(self,k):
        self.keys();
        k = fn.tango.get_full_name(k).lower()
        return k in self.attributes
    
    def __contains__(self,k):
        return self.has_key(k)
    
    def __len__(self):
        self.keys();
        return len(self.attributes)
    
    def values(self):
        self.keys();       
        return self.attributes.values()
    
    def items(self):
        self.keys();       
        return self.attributes.items()
    
    def __getitem__(self,key):
        self.keys();
        key = fn.tango.get_full_name(key).lower()
        return self.attributes[key]
    
    def __iter__(self):
        self.keys();
        return self.attributes.__iter__()
            
    def get_db_config(self,manager='', db_name=''):
        if not manager:
            manager = self.get_manager(db_name).name()
            
        prop = get_device_property(manager,'LibConfiguration')
        if prop:
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
        if not getattr(self,'manager',None):
            db_name = db_name or getattr(self,'db_name','')
            self.manager = ''
            managers = self.get_all_managers()
            for m in managers:
                if db_name:
                    prop = get_device_property(m,'DbName')
                    if not prop:
                        prop = str(get_device_property(m,'LibConfiguration'))
                if not db_name or db_name in prop:
                    self.manager = m
                    break
                    
        dp = get_device(self.manager,keep=True) if self.manager else None
        if not check_device(dp):
            print('get_manager(%s): %s is not running!' 
                  % (db_name,self.manager))

        return dp
      
    @Cached(depth=10,expire=60.)
    def get_archived_attributes(self,search=''):
        #print('get_archived_attributes(%s)'%str(search))
        attrs = []
        archs = get_device_property(self.manager,'ArchiverList') 
            #self.get_manager().ArchiverList
        self.get_archivers_attributes(archs,full=False,from_db=False)
        for d,dattrs in self.dedicated.items():
            for a in dattrs:
                self.attributes[a].archiver = d
                if not search or fn.clsearch(search,a):
                    attrs.append(a)
        return attrs
    
        ## DB API
        ##r = sorted(str(a).lower().replace('tango://','') 
        ##THIS METHOD RETURNS ONLY 1000 ATTRIBUTES AS MUCH!!!
        #r = sorted(parse_tango_model(a,fqdn=True).normalname 
        #for a in self.get_manager().AttributeSearch(search))
    
    @Cached(depth=2,expire=60.)
    def get_attributes(self,active=None):
        """
        Alias for Reader API
        @TODO active argument not implemented
        """
        if active:
            return self.get_archived_attributes()
        else:
            return self.get_attribute_names(False)
    
    def get_attributes_failed(self,regexp='*',timeout=3600,from_db=True):
        if from_db:
            timeout = fn.now()-timeout
            attrs = self.get_attributes(True)
            attrs = fn.filtersmart(attrs,regexp)
            print('get_attributes_failed([%d])' % len(attrs))
            print(attrs)
            vals = self.load_last_values(attrs)
            return sorted(t for t in vals if not t[1] or
                         t[1][0] < timeout)
        else:
            # Should inspect the Subscribers Error Lists
            raise Exception('NotImplemented')
    
    @Cached(expire=60.)
    def get_archivers(self):
        #return list(self.tango.get_device_property(self.manager,'ArchiverList')['ArchiverList'])
        if self.manager and check_device(self.manager):
          return self.get_manager().ArchiverList
        else:
          raise Exception('%s Manager not running'%self.manager)
      
    @Cached(expire=60.)
    def get_archivers_attributes(self,archs=None,from_db=True,full=False):
        #print('get_archivers_attributes(%s)' % str(archs))
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
                    dedicated[a].extend(get_device(a).AttributeList)
                except:
                    dedicated[a] = []
                    
        self.dedicated.update(dedicated)
        return dedicated
    
    @Cached(depth=1000,expire=60.)
    def get_attribute_archiver(self,attribute):
        if not self.dedicated:
            self.get_archivers_attributes()

        #m = parse_tango_model(attribute,fqdn=True).fullname
        m = get_full_name(attribute,fqdn=True)
        for k,v in self.dedicated.items():
            for l in v:
                if m in l.split(';'):
                    return k
        return None
    
    def start_servers(self,host='',restart=True):
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
        
    def start_devices(self,regexp = '*', force = False, 
                      do_init = False, do_restart = False):
        #devs = fn.tango.get_class_devices('HdbEventSubscriber')
        devs = self.get_archivers()
        if regexp:
            devs = fn.filtersmart(devs,regexp)
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
                dp = fn.get_device(d)
                if do_init:
                    dp.init()
                if force or dp.attributenumber != dp.attributestartednumber:
                    off.append(d)
                    print('%s.Start()' % d)
                    dp.start()
            except Exception,e:
                self.warning('start_archivers(%s) failed: %s' % (d,e))
                
        return off        
    
    def add_archiving_manager(self,srv,dev):
        if '/' not in srv: srv = 'hdb++cm-srv/'+srv
        add_new_device(srv,'HdbConfigurationManager',dev)
        prev = get_device_property(dev,'ArchiverList') or []
        put_device_property(dev,'ArchiverList',prev)
        put_device_property(dev,'ArchiveName','MySQL')
        put_device_property(dev,'DbHost',self.host)
        put_device_property(dev,'DbName',self.db_name)
        put_device_property(dev,'DbUser',self.user)
        put_device_property(dev,'DbPassword',self.passwd)
        put_device_property(dev,'DbPort','3306')
        put_device_property(dev,'LibConfiguration',[
          'user='+self.user,
          'password='+self.passwd,
          'port='+self.port,
          'host='+self.host,
          'dbname='+self.db_name,])
        self.get_manager()
        return dev

    def add_event_subscriber(self,srv,dev,libpath=''):
        if '/' not in srv: srv = 'hdb++es-srv/'+srv
        add_new_device(srv,'HdbEventSubscriber',dev)
        manager,dp = self.manager,self.get_manager()
        props = Struct(get_matching_device_properties(manager,'*'))
        prev = get_device_property(dev,'AttributeList') or []
        put_device_property(dev,'AttributeList',prev)
        #put_device_property(dev,'DbHost',self.host)
        #put_device_property(dev,'DbName',self.db_name)
        #put_device_property(dev,'DbUser',self.user)
        #put_device_property(dev,'DbPassword',self.passwd)
        #put_device_property(dev,'DbPort','3306')
        #put_device_property(dev,'DbStartArchivingAtStartup','true')
        
        libpath = (libpath or \
                '/homelocal/sicilia/src/hdbpp.git/lib/libhdb++mysql.so')
        put_device_property(dev,'LibConfiguration',[
          'user='+self.user,
          'password='+self.passwd,
          'port='+getattr(self,'port','3306'),
          'host='+self.host,
          'dbname='+self.db_name,
          'libname='+libpath,
          'ligthschema=1',
          ])
        if 'ArchiverList' not in props:
            props.ArchiverList = []
            
        dev = parse_tango_model(dev,fqdn=True).fullname
        #put_device_property(manager,'ArchiverList',
                            #list(set(list(props.ArchiverList)+[dev])))
        print(dev)
        dp.ArchiverAdd(dev)
        return dev
    
    def add_attributes(self,attributes,*args,**kwargs):
        """
        Call add_attribute sequentially with a 1s pause between calls
        :param start: True by default, will force Start() in related archivers
        See add_attribute? for more help on arguments
        """
        try:
          start = kwargs.get('start',True)
          for a in attributes:
            kwargs['start'] = False #Avoid recursive start
            self.add_attribute(a,*args,**kwargs)
          time.sleep(3.)
            
          if start:
            archs = set(map(self.get_attribute_archiver,attributes))
            for h in archs:
                self.info('%s.Start()' % h)
                fn.get_device(h).Start()
                
        except Exception,e:
            print('add_attribute(%s) failed!: %s'%(a,traceback.print_exc()))
        return

    def add_attribute(self,attribute,archiver,period=0,
                      rel_event=None,per_event=300000,abs_event=None,
                      code_event=False, ttl=None, start=False):
        """
        set _event arguments to -1 to ignore them and not modify the database
        
        
        """
        attribute = parse_tango_model(attribute,fqdn=True).fullname
        self.info('add_attribute(%s)'%attribute)
        config = get_attribute_config(attribute)
        #if 'spectrum' in str(config.data_format).lower():
          #raise Exception('Arrays not supported yet!')
        data_type = str(PyTango.CmdArgType.values[config.data_type])
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
          if per_event not in (None,-1):
            d.write_attribute('SetPeriodEvent',per_event)

          if not any((abs_event,rel_event,code_event)):
            if re.search("short|long",data_type.lower()):
              abs_event = 1
            elif not re.search("bool|string",data_type.lower()):
              rel_event = 1e-2
          if abs_event not in (None,-1):
            print('SetAbsoluteEvent: %s'%abs_event)
            d.write_attribute('SetAbsoluteEvent',abs_event)
          if rel_event not in (None,-1):
            d.write_attribute('SetRelativeEvent',rel_event)
          if ttl not in (None,-1):
            d.write_attribute('SetTTL',ttl)
            
          d.write_attribute('SetCodePushedEvent',code_event)

          d.write_attribute('SetArchiver',archiver)
          time.sleep(.2)
          d.AttributeAdd()
          
          if start:
              try:
                arch = archiver # self.get_attribute_archiver(attribute)
                self.info('%s.Start()' % (arch))
                fn.get_device(arch).Start()
              except:
                traceback.print_exc()
              
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
        
    def is_attribute_archived(self,attribute,active=None,cached=True):
        # @TODO active argument not implemented
        model = parse_tango_model(attribute,fqdn=True)
        d = self.get_manager()
        if d and cached:
            self.get_archived_attributes()
            if any(m in self.attributes for m in (attribute,model.fullname,model.normalname)):
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
            return any(a.lower().endswith('/'+attribute.lower())
                                          for a in self.get_attributes())
          
    def start_archiving(self,attribute,archiver,period=0,
                      rel_event=None,per_event=300000,abs_event=None,
                      code_event=False, ttl=None, start=False):
        """
        See HDBpp.add_attribute.__doc__ for a full description of arguments
        """
        try:
            if isSequence(attribute):
                for attr in attribute:
                    self.start_archiving(attr,*args,**kwargs)
                    time.sleep(1.)
            else:
                self.info('start_archiving(%s)'%attribute)
                d = self.get_manager()
                fullname = parse_tango_model(attribute,fqdn=True).fullname
                if not self.is_attribute_archived(attribute):
                    self.add_attribute(fullname,archiver=archiver,
                        period=period, rel_event=rel_event, 
                        per_event=per_event, abs_event=abs_event,
                        code_event=code_event, ttl=ttl, 
                        start=start)
                    time.sleep(5.)
                    #fullname = self.is_attribute_archived(attribute,cached=0)
                d.AttributeStart(fullname)
                return True
        except Exception,e:
            self.error('start_archiving(%s): %s'
                        %(attribute,traceback.format_exc().replace('\n','')))
        return False        

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
      
    def get_attribute_names(self,active=False):
        if not active:
            [self.attributes[a[0].lower()] for a 
                in self.Query('select att_name from att_conf')]
            return self.attributes.keys()
        else:
            return self.get_archived_attributes()
    
    @Cached(depth=10000,expire=60.)
    def get_attribute_modes(self,attr,force=None):
        """ force argument provided just for compatibility, replaced by cache
        """
        aid,tid,table = self.get_attr_id_type_table(attr)
        r = {'ID':aid, 'MODE_E':fn.tango.get_attribute_events(attr)}
        r['archiver'] = self.get_attribute_archiver(attr)
        return r
      
    def get_table_name(self,attr):
        return get_attr_id_type_table(attr)[-1]
      
    def get_attr_id_type_table(self,attr):
        if fn.isNumber(attr):
            where = 'att_conf_id = %s'%attr
        else:
            where = "att_name like '%s'"%get_search_model(attr)
        q = "select att_name,att_conf_id,att_conf_data_type_id from att_conf"\
            " where %s"%where
        ids = self.Query(q)
        self.debug(str((q,ids)))
        if not ids: 
            return None,None,''
        
        attr,aid,tid = ids[0]
        table = self.Query("select data_type from att_conf_data_type "\
            +"where att_conf_data_type_id = %s"%tid)[0][0]

        self.attributes[attr].id = aid
        self.attributes[attr].type = table
        self.attributes[attr].table = 'att_'+table
        self.attributes[attr].modes = {'MODE_E':True}
        return aid,tid,'att_'+table
      
    def set_attr_event_config(self,attr,polling=0,abs_event=0,
                              per_event=0,rel_event=0):
        ac = get_attribute_config(attr)
        raise Exception('@TODO')
      
    #def get_default_archiving_modes(self,attr):
        #if isString(attr) and '/' in attr:
          #attr = read_attribute(attr)
          #pytype = type(attr)
          
    def get_attributes_by_table(self,table=''):
        if table:
          return self.Query("select att_name from att_conf,att_conf_data_type where data_type like '%s'"+
                  " and att_conf.att_conf_data_type_id = att_conf_data_type.att_conf_data_type_id")
        else:
          types = self.Query("select data_type,att_conf_data_type_id from att_conf_data_type")
          return dict((t,self.Query("select att_name from att_conf where att_conf_data_type_id = %s"%i))
                      for t,i in types)
          
    #@staticmethod
    #def decimate_values(values,N=540,method=None):
        #"""
        #values must be a sorted (time,...) array
        #it will be decimated in N equal time intervals 
        #if method is not provided, only the first value of each interval will be kept
        #if method is given, it will be applied to buffer to choose the value to keep
        #first value of buffer will always be the last value kept
        #"""
        #tmin,tmax = sorted((values[0][0],values[-1][0]))
        #result,buff = [values[0]],[values[0]]
        #interval = float(tmax-tmin)/N
        #if not method:
          #for v in values:
            #if v[0]>=(interval+float(result[-1][0])):
              #result.append(v)
        #else:
          #for v in values:
            #if v[0]>=(interval+float(result[-1][0])):
              #result.append(method(buff))
              #buff = [result[-1]]
            #buff.append(v)

        #print(tmin,tmax,N,interval,len(values),len(result),method)
        #return result
        
    def decimate_table(att_id,table):
        """
        @TODO
        """
        hours = [t0+i*3600 for i in range(24*30)]
        days = [t0+i*86400 for i in range(30)]
        dvalues = {}
        q = ("select count(*) from %s where att_conf_id = %d "
            "and data_time between '%s' and '%s'")
        for d in days:
            s = fn.time2str(d)
            q = hdbpp.Query(q%(table,att_id,s,fn.time2str(d+86400))
                            +" and (data_time %% 5) < 2;")
        sorted(values.items())
        3600/5
        for h in hours:
            s = fn.time2str(h)
            q = hdbpp.Query("select count(*) from att_scalar_devdouble_ro "
                "where att_conf_id = 1 and data_time between '%s' and '%s' "
                "and (data_time %% 5) < 2;"%(s,fn.time2str(h+3600)))

      
    def get_last_attribute_values(self,table,n=1,
                                  check_table=False,epoch=None):
        vals = self.get_attribute_values(
            table,N=n,human=True,desc=True,stop_date=epoch)
        if len(vals) and abs(n)==1: 
            return vals[0]
        else: 
            return vals
    
    def load_last_values(self,attributes,n=1,epoch=None):
        return dict((a,self.get_last_attribute_values(a,n=n,epoch=None)) 
                    for a in fn.toList(attributes))
        
    __test__['get_last_attribute_values'] = \
        [(['bl01/vc/spbx-01/p1'],None,lambda r:len(r)>0)] #should return array
            
    @CatchedAndLogged(throw=True)
    def get_attribute_values(self,table,start_date=None,stop_date=None,
                             desc=False,N=0,unixtime=True,
                             extra_columns='quality',decimate=0,human=False,
                             as_double=True,
                             **kwargs):
        """
        This method returns values between dates from a given table.
        If stop_date is not given, then anything above start_date is returned.
        desc controls the sorting of values
        
        unixtime = True enhances the speed of querying by a 60%!!!! 
            #(due to MySQLdb implementation of datetime)
        
        If N is specified:
        
            * Query will return last N values if there's no stop_date
            * If there is, then it will return the first N values (windowing?)
            * IF N is negative, it will return the last N values instead
            
        start_date and stop_date must be in a format valid for SQL
        """
        t0 = time.time()
        self.debug('HDBpp.get_attribute_values(%s,%s,%s,%s,decimate=%s,%s)'
              %(table,start_date,stop_date,N,decimate,kwargs))
        if fn.isSequence(table):
            aid,tid,table = table
        else:
            aid,tid,table = self.get_attr_id_type_table(table)
            
        if not all((aid,tid,table)):
            self.warning('%s is not archived' % table)
            return []
            
        human = kwargs.get('asHistoryBuffer',human)
            
        what = 'UNIX_TIMESTAMP(data_time)' if unixtime else 'data_time'
        if as_double:
            what = 'CAST(%s as DOUBLE)' % what
        if 'array' in table: what+=",idx"
        what += ',value_r' if 'value_r' in self.getTableCols(table) \
                                else ',value'
        if extra_columns: what+=','+extra_columns
        interval = 'where att_conf_id = %s'%aid if aid is not None \
                                                else 'where att_conf_id >= 0 '

        if start_date or stop_date:
          start_date,start_time,stop_date,stop_time = \
              Reader.get_time_interval(start_date,stop_date)
          if start_date and stop_date:
            interval += (" and data_time between '%s' and '%s'"
                            %(start_date,stop_date))
          elif start_date and fandango.str2epoch(start_date):
            interval += " and data_time > '%s'"%start_date
            
        query = 'select %s from %s %s order by data_time' \
                        % (what,table,interval)
                    
        if N == 1:
            human = 1
        if N<0 or desc: 
            query+=" desc" # or (not stop_date and N>0):
        if N: 
            query+=' limit %s'%abs(N) # if 'array' not in table else 1024)
        
        ######################################################################
        # QUERY
        self.debug(query)
        try:
            result = self.Query(query)
        except MySQLdb.ProgrammingError as e:
            if 'DOUBLE' in str(e) and "as DOUBLE" in query:
                return self.get_attribute_values((aid,tid,table),start_date,
                    stop_date,desc,N,unixtime,extra_columns,decimate,human,
                    as_double=False,**kwargs)
            
        self.debug('read [%d] in %f s'%(len(result),time.time()-t0))
        t0 = time.time()
        if not result or not result[0]: return []
        ######################################################################

        if 'array' in table:
            data = fandango.dicts.defaultdict(list)
            for t in result:
                data[float(t[0])].append(t[1:])
            result = []
            for k,v in sorted(data.items()):
                l = [0]*(1+max(t[0] for t in v))
                for i,t in enumerate(v):
                    if None in t: 
                        l = None
                        break
                    l[t[0]] = t[1] #Ignoring extra columns (e.g. quality)
                result.append((k,l))
            if N > 0: 
                #for k,l in result:
                    #print((k,l and len(l)))
                result = result[-N:]
            self.debug('array arranged [%d] in %f s'
                         % (len(result),time.time()-t0))
            t0 = time.time()
          
        # Converting the timestamp from Decimal to float
        # Weird results may appear in filter_array comparison if not done
        # Although it is INCREDIBLY SLOW!!!
        #result = []
        #nr = []
        #if len(result[0]) == 2: 
            #for i,t in enumerate(result):
                #result[i] = (float(t[0]),t[1])
        #elif len(result[0]) == 3: 
            #for i,t in enumerate(result):
                #result[i] = (float(t[0]),t[1],t[2])
        #elif len(result[0]) == 4: 
           #for i,t in enumerate(result):
                #result[i] = ((float(t[0]),t[1],t[2],t[3]))
        #else:
            #for i,t in enumerate(result):
                #result[i] = ([float(t[0])]+t[1:])
        
        self.debug('timestamp arranged [%d] in %f s'
                     % (len(result),time.time()-t0))
        t0 = time.time()
            
        # Decimation to be done in Reader object
        #if decimate:
            ## When called from trends, decimate may be the decimation method
            ## or the maximum sample number
            #try:
                #N = int(decimate)
                ##decimate = data_has_changed
                #decimate = 
                #result = PyTangoArchiving.reader.decimation(
                                        #result,decimate,window=0,N=N)                
            #except:
                ##N = 1080
                #result = PyTangoArchiving.reader.decimation(result,decimate) 
        
        if human: 
            result = [list(t)+[fn.time2str(t[0])] for t in result]

        if not desc and ((not stop_date and N>0) or (N<0)):
            #THIS WILL BE APPLIED ONLY WHEN LAST N VALUES ARE ASKED
            #self.warning('reversing ...' )
            result = list(reversed(result))
        else:
            # why?
            self.getCursor(klass=MySQLdb.cursors.SSCursor)

        self.debug('result arranged [%d]'%len(result))            
        return result
        
    def get_attributes_values(self,tables='',start_date=None,stop_date=None,
                desc=False,N=0,unixtime=True,extra_columns='quality',
                decimate=0,human=False):
        
        if not fn.isSequence(tables):
            tables = self.get_archived_attributes(tables)
            
        return dict((t,self.get_attribute_values(t,start_date,stop_date,desc,
                N,unixtime,extra_columns,decimate,human))
                for t in tables)

    def get_attribute_rows(self,attribute,start_date=0,stop_date=0):
        aid,tid,table = self.get_attr_id_type_table(attribute)
        if start_date and stop_date:
            dates = map(time2str,(start_date,stop_date))
            where = "and data_time between '%s' and '%s'" % dates
        else:
            where = ''
        r = self.Query('select count(*) from %s where att_conf_id = %s'
                          % ( table, aid) + where)
        return r[0][0] if r else 0
    
    def get_failed_attributes(self,t=7200):
        vals = self.load_last_values(self.get_attributes())
        nones = [k for k,v in vals.items() 
                    if (not v or v[1] is None)]
        nones = [k for k in nones if fn.read_attribute(k) is not None]
        lost = [k for k,v in vals.items() 
                if k not in nones and v[0] < fn.now()-t]
        lost = [k for k in lost if fn.read_attribute(k) is not None]
        failed = nones+lost
        return sorted(failed)
    
    def restart_attribute(self,attr, d=''):
        try:
            d = self.get_attribute_archiver(attr)
            print('%s.restart_attribute(%s)' % (d,attr))
            dp = fn.get_device(d,keep=True)

            if not fn.check_device(dp):
                self.start_devices('(.*/)?'+d,do_restart=True)
                
            dp.AttributeStop(attr)
            fn.wait(.1)
            dp.AttributeStart(attr)
        except:
            print('%s.AttributeStart(%s) failed!'%(d,attr))
        
    def restart_attributes(self,attributes=None):
        if attributes is None:
            attributes = self.get_failed_attributes()

        for a in sorted(attributes):
            self.restart_attribute(a)
            
        print('%d attributes restarted' % len(attributes))
    
    def check_attributes(self,attrs = '', load = False, t0 = 0):
        
        db,t0,result,vals = self,t0 or fn.now(),{},{}
        print('Checking %s' % str(db))

        if fn.isDictionary(attrs):
            attrs,vals = attrs.keys(),attrs
            if isinstance(vals.values()[0],dict):
                vals = dict((k,v.values()[0]) for k,v in vals.items())
        else:
            if fn.isString(attrs):
                attrs = fn.filtersmart(db.get_attributes(),attrs)
                load = True

        if load:
            [vals.update(db.load_last_values(a)) for a in attrs]

        print('\t%d attributes'%len(attrs))
        result['attrs'] = attrs
        result['vals'] = vals
        result['novals'] = [a for a,v in vals.items() if not v]
        result['nones'],result['down'],result['lost'] = [],[],[]
        for a,v in vals.items():
            if not v or [1] is None:
                if not fn.read_attribute(a): #USE read not check!!
                    result['down'].append(a)
                else:
                    result['novals' if not v else 'nones'].append(a)
            elif v[0] < (t0 - 7200):
                result['lost'].append(a)
        
        print('\t%d attributes have no values'%len(result['novals']))
        print('\t%d attributes are not readable'%len(result['down']))
        print('\t%d attributes are not updated'%len(result['lost']))
        print('\t%d attributes have None values'%len(result['nones']))
        
        return result
    
    
    
    
    
