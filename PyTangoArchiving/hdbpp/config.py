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


from PyTangoArchiving.dbs import ArchivingDB
from PyTangoArchiving.common import CommonAPI
from PyTangoArchiving.reader import Reader
from PyTangoArchiving.utils import CatchedAndLogged
import fandango as fn
from fandango.objects import SingletonMap
from fandango.tango import *
import MySQLdb,traceback,re
from PyTango import AttrQuality

__test__ = {}

def get_full_name(model):
    """ Returns full schema name as needed by HDB++ api
    """
    if ':' not in model:
      model = get_tango_host()+'/'+model
    if not model.startswith('tango://'):
      model = 'tango://'+model
    return model

def get_search_model(model):
    if model.count(':')<2:
        model = '%/'+model
    model = clsub('[:][0-9]+','%:%',model)
    return model

class HDBpp(ArchivingDB,SingletonMap):
    
    def __init__(self,db_name='',host='localhost',user='archiver',passwd='archiver',other=None):
        if other:
            db_name,host,user,passwd = other.db_name,other.host,other.user,other.passwd
        self.tango = get_database()
        if not db_name:
          mans = self.get_all_managers()
          if not mans or len(mans)>1: 
              print mans
              raise Exception('db_name argument is required!')
          else:
              db_name = get_device_property(mans[0],'DbName')
              host = get_device_property(mans[0],'DbHost')
              user = get_device_property(mans[0],'DbUser')
              passwd = get_device_property(mans[0],'DbPassword')
        ArchivingDB.__init__(self,db_name,host,user,passwd)
        try:
            assert self.get_manager(),'HdbConfigurationManager not found!!'
        except Exception,e:
            print(e)
        
    def get_all_managers(self):
        return get_class_devices('HdbConfigurationManager')
    
    def get_all_archivers(self):
        return get_class_devices('HdbEventSubscriber')
      
    def get_manager(self):
        if not getattr(self,'manager',None):
            self.manager = ''
            managers = self.get_all_managers()
            for m in managers:
                d = get_device_property(m,'DbName')
                if not d:
                    d = str(get_device_property(m,'LibConfiguration'))
                if self.db_name in d:
                    self.manager = m
                    break
                    
        return get_device(self.manager) if self.manager else None
      
    def get_archived_attributes(self,search=''):
        # DB API
        return sorted(str(a).lower().replace('tango://','') 
                      for a in self.get_manager().AttributeSearch(search))
    
    def get_attributes(self,active=None):
        """
        Alias for Reader API
        @TODO active argument not implemented
        """
        return self.get_archived_attributes()
    
    def get_archivers(self):
        #return list(self.tango.get_device_property(self.manager,'ArchiverList')['ArchiverList'])
        if self.manager and check_device(self.manager):
          return self.get_manager().ArchiverList
        else:
          raise Exception('%s Manager not running'%self.manager)
    
    def start_servers(self,host=''):
        import fandango.servers
        if not self.manager: self.get_manager()
        astor = fandango.servers.Astor(self.manager)
        astor.start_servers(host=(host or self.db_host))
        time.sleep(1.)
        astor.load_from_devs_list(self.get_archivers())
        astor.start_servers(host=(host or self.db_host))
    
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

    def add_event_subscriber(self,srv,dev):
        if '/' not in srv: srv = 'hdb++es-srv/'+srv
        add_new_device(srv,'HdbEventSubscriber',dev)
        manager = self.manager
        props = Struct(get_matching_device_properties(manager,'*'))
        prev = get_device_property(dev,'AttributeList') or []
        put_device_property(dev,'AttributeList',prev)
        put_device_property(dev,'DbHost',self.host)
        put_device_property(dev,'DbName',self.db_name)
        put_device_property(dev,'DbUser',self.user)
        put_device_property(dev,'DbPassword',self.passwd)
        put_device_property(dev,'DbPort','3306')
        put_device_property(dev,'DbStartArchivingAtStartup','true')
        put_device_property(dev,'LibConfiguration',[
          'user='+self.user,
          'password='+self.passwd,
          'port='+getattr(self,'port','3306'),
          'host='+self.host,
          'dbname='+self.db_name,])
        if 'ArchiverList' not in props:
            props.ArchiverList = []
        put_device_property(manager,'ArchiverList',list(set(list(props.ArchiverList)+[dev])))
        return dev
    
    def add_attributes(self,attributes,*args,**kwargs):
        """Call add_attribute sequentially with a 1s pause between calls"""
        try:
          for a in attributes:
            self.add_attribute(a,*args,**kwargs)
          time.sleep(10.)
          for a in attributes:
            self.start_archiving(a)
        except Exception,e:
            print('add_attribute(%s) failed!: %s'%(a,traceback.print_exc()))
        return

    def add_attribute(self,attribute,archiver,period=0,
                      rel_event=None,per_event=300000,abs_event=None):
        import fandango  as fn
        attribute = get_full_name(str(attribute).lower())
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
          d.write_attribute('SetAttributeName',attribute)
          time.sleep(0.2)
          if period>0:
            d.write_attribute('SetPollingPeriod',period)
          d.write_attribute('SetPeriodEvent',per_event)

          if not any((abs_event,rel_event)):
            if re.search("short|long",data_type.lower()):
              abs_event = 1
            elif not re.search("bool|string",data_type.lower()):
              rel_event = 1e-2
          if abs_event is not None:
            d.write_attribute('SetAbsoluteEvent',abs_event)
          if rel_event is not None:
            d.write_attribute('SetRelativeEvent',rel_event)

          d.write_attribute('SetArchiver',archiver)
          time.sleep(.2)
          #d.AttributeAdd()
          d.AttributeAdd()
        except Exception,e:
          if 'already archived' not in str(e).lower():
            self.error('add_attribute(%s,%s,%s): %s'
                       %(attribute,archiver,period,
                         traceback.format_exc().replace('\n','')))
            return False
        finally:
          #self.warning('unlocking %s ..'%self.manager)
          d.unlock()
        print('%s added'%attribute)
        
    def is_attribute_archived(self,attribute,active=None):
        # @TODO active argument not implemented
        d = self.get_manager()
        attribute = d.AttributeSearch(attribute.lower())
        if len(attribute)>1: 
          raise Exception('MultipleAttributesMatched!')
        if len(attribute)==1:
          return attribute[0]
        else:
          return False
          
    def start_archiving(self,attribute,*args,**kwargs):
        try:
            if isSequence(attribute):
                for attr in attribute:
                    self.start_archiving(attr,*args,**kwargs)
            else:
                self.info('start_archiving(%s)'%attribute)
                d = self.get_manager()
                fullname = self.is_attribute_archived(attribute)
                if not fullname:
                    self.add_attribute(attribute,*args,**kwargs)
                    time.sleep(10.)
                    fullname = self.is_attribute_archived(attribute)
                d.AttributeStart(fullname)
                return True
        except Exception,e:
            self.error('start_archiving(%s): %s'
                        %(attribute,traceback.format_exc().replace('\n','')))
        return False        
        
    def get_attribute_ID(self,attr):
        # returns only 1 ID
        return self.get_attribute_IDs(attr,as_dict=0)[0][0]
      
    def get_attribute_IDs(self,attr,as_dict=1):
        # returns all matching IDs
        ids = self.Query("select att_name,att_conf_id from att_conf "\
            +"where att_name like '%s'"%get_search_model(attr))
        if not ids: return None
        elif not as_dict: return ids
        else: return dict(ids)
      
    def get_attribute_names(self,active=False):
        return [a[0].lower() for a 
                in self.Query('select att_name from att_conf')]
      
    def get_table_name(self,attr):
        return get_attr_id_type_table(attr)
      
    def get_attr_id_type_table(self,attr):
        if fn.isNumber(attr):
            where = 'att_conf_id = %s'%attr
        else:
            where = "att_name like '%s'"%get_search_model(attr)
        q = "select att_conf_id,att_conf_data_type_id from att_conf where %s"\
                %where
        ids = self.Query(q)
        self.debug(str((q,ids)))
        if not ids: 
            return []
        aid,tid = ids[0]
        table = self.Query("select data_type from att_conf_data_type "\
            +"where att_conf_data_type_id = %s"%tid)[0][0]
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
          
    @staticmethod
    def decimate_values(self,values,N=540,method=None):
        """
        values must be a sorted (time,...) array
        it will be decimated in N equal time intervals 
        if method is not provided, only the first value of each interval will be kept
        if method is given, it will be applied to buffer to choose the value to keep
        first value of buffer will always be the last value kept
        """
        tmin,tmax = sorted((values[0][0],values[-1][0]))
        result,buff = [values[0]],[values[0]]
        interval = float(tmax-tmin)/N
        if not method:
          for v in values:
            if v[0]>=(interval+float(result[-1][0])):
              result.append(v)
        else:
          for v in values:
            if v[0]>=(interval+float(result[-1][0])):
              result.append(method(buff))
              buff = [result[-1]]
            buff.append(v)
        return result
      
    def get_last_attribute_values(self,table,n=1,check_table=False):
        #if N==1:
            #return result and result[0 if 'desc' in query else -1] or []
        vals = self.get_attribute_values(table,N=n,human=True)
        if abs(n)==1: return vals[0]
        else: return vals
    
    def load_last_values(self,attributes,n=1):
        return dict((a,self.get_last_attribute_values(a,n=n)) 
                    for a in fn.toList(attributes))
        
    __test__['get_last_attribute_values'] = \
        [(['bl01/vc/spbx-01/p1'],None,lambda r:len(r)>0)] #should return array
            
    @CatchedAndLogged(throw=True)
    def get_attribute_values(self,table,start_date=None,stop_date=None,
                             desc=False,N=-1,unixtime=True,
                             extra_columns='quality',decimate=0,human=False,
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
        self.setLogLevel('INFO')
        self.info('HDBpp.get_attribute_values(%s,%s,%s,%s,%s)'
              %(table,start_date,stop_date,N,kwargs))
        aid,tid,table = self.get_attr_id_type_table(table)
            
        what = 'UNIX_TIMESTAMP(data_time)' if unixtime else 'data_time'
        if 'array' in table: what+=",idx"
        what += ',value_r' if 'value_r' in self.getTableCols(table) \
                                else ',value'
        if extra_columns: what+=','+extra_columns
        interval = 'where att_conf_id = %s'%aid if aid is not None \
                                                else 'where att_conf_id >= 0 '
        if N<0:
            N = abs(N)
            desc =  True
        if start_date or stop_date:
          start_date,start_time,stop_date,stop_time = \
              Reader.get_time_interval(start_date,stop_date)
          if start_date and stop_date:
            interval += " and data_time between '%s' and '%s'"%(start_date,stop_date)
          elif start_date and fandango.str2epoch(start_date):
            interval += " and data_time > '%s'"%start_date
        if N == 1:
            human = 1
            
        query = 'select %s from %s %s order by data_time' \
                        % (what,table,interval)
        if desc or (not stop_date and N>0): query+=" desc"
        if N>0: query+=' limit %s'%N
        self.debug(query)

        result = self.Query(query)
        self.info('read [%d]'%len(result))
        if not result or not result[0]: return []
        #if len(result[0]) == 2: ## Just data_time and value_r
          #result = [(float(t[0]),t) for t in result]
          
        #result = [t for t in self.Query(query)]
        if 'array' in table:
            data = fandango.dicts.defaultdict(list)
            for t in result:
                data[float(t[0])].append(t[1:])
            result = []
            #print('array',data)
            for k,v in sorted(data.items()):
                l = [0]*len(v)
                for t in v:
                    if None in t: 
                        l = None
                        break
                    l[t[0]] = t[1] #Ignoring extra columns (e.g. quality)
                result.append((k,l))
            self.debug('arranged [%d]'%len(result))
            
        if N>1 and decimate!=0: 
          result = self.decimate_values(result,N=decimate)
        if human: 
          result = [list(t)+[fn.time2str(t[0])] for t in result]
        else:
          #Converting the timestamp from Decimal to float
          if len(result[0]) == 2: 
            result = [(float(t[0]),t[1]) for t in result]
          elif len(result[0]) == 3: 
            result = [(float(t[0]),t[1],t[2]) for t in result]
          elif len(result[0]) == 4: 
            result = [(float(t[0]),t[1],t[2],t[3]) for t in result]
          else:
            result = [[float(t[0])]+t[1:] for t in result]
        self.debug('decimated: [%d]'%len(result))

        if not desc and not stop_date and N>0:
            #THIS WILL BE APPLIED ONLY WHEN LAST N VALUES ARE ASKED
            return list(reversed(result))
        else:
            self.getCursor(klass=MySQLdb.cursors.SSCursor)
            return result
        
    def get_attributes_values(self,tables='',start_date=None,stop_date=None,desc=False,N=-1,
                              unixtime=True,extra_columns='quality',decimate=0,human=False):
        if not fn.isSequence(tables):
            tables = self.get_archived_attributes(tables)
        return dict((t,self.get_attribute_values(t,start_date,stop_date,desc,
                N,unixtime,extra_columns,decimate,human))
                for t in tables)

