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
CLASS FOR TANGO ARCHIVING MANAGEMENT
by Sergi Rubio Manrique, srubio@cells.es
ALBA Synchrotron Control Group
16 June 2007
"""

import time
import traceback
import os
import threading
import datetime
from random import randrange

import PyTangoArchiving
from PyTangoArchiving import ARCHIVING_CLASSES,ARCHIVING_TYPES,MAX_SERVERS_FOR_CLASS,MIN_ARCHIVING_PERIOD
from PyTangoArchiving.utils import *

import fandango
from fandango.dicts import CaselessDict,CaselessDefaultDict
from fandango.log import Logger
from fandango.objects import Object

from utils import PyTango,ServersDict
    
def int2DevState(n): return str(PyTango.DevState.values[n])    
def int2DevType(n): return str(PyTango.ArgType.values[n])    

class CommonAPI(Object,fandango.SingletonMap):
    """ This class provides common methods for managing a Soleil-like database (for either archiving or snapshoting)
    The methods starting by "get" retrieve values using ArchivingDSs
    The methods starting by "load" access directly to MySQL database
    """
    #ArchivingTypes = ARCHIVING_TYPES
    #ArchivingClasses = ARCHIVING_CLASSES
    
    MAX_SERVERS_FOR_CLASS=5
    MIN_ARCHIVING_PERIOD=10
    
    def __init__(self,schema,host=None,user='browser',passwd='browser',classes=[],LogLevel='info',load=True,logger=None):
        """
        """
        self.log = logger or Logger('ArchivingAPI(%s)'%schema,format='%(levelname)-8s %(asctime)s %(name)s: %(message)s')
        self.log.setLogLevel(LogLevel)
        self.log.debug('Logger streams initialized (error,warning,info,debug)')

        self.tango = fandango.get_database() #access to Tango database
        self.api = self #hook used by legacy packages
        self.servers = None
        self.schema = str(schema).lower()
        self.user,self.passwd = user,passwd
        
        if host is None:
            prop = self.tango.get_class_property('%sArchiver'%schema,['DbHost'])['DbHost']
            if not prop: 
                print('ERROR: %sArchiver.DbHost property not defined!'%schema)
                self.host = None
            else:
                self.host = prop[0]
            #if 'TANGO_HOST' in os.environ:
            #    self.host=os.environ['TANGO_HOST'].split(':')[0]
        else: self.host=host
        
        self.dbs={} #pointers to Archiving databases
        
        self.ArchivingClasses = classes or self.get_archiving_classes()
        self.ArchiverClass = (k for k in self.ArchivingClasses if 'Archiver' in k).next()
        self.ManagerClass = (k for k in self.ArchivingClasses if 'Manager' in k).next()
        self.ExtractorClass = (k for k in self.ArchivingClasses if 'Extractor' in k).next()
        try: self.WatcherClass = (k for k in self.ArchivingClasses if 'Watcher' in k).next()
        except: self.WatcherClass = None
        
        self.loads=CaselessDefaultDict(lambda k:0) #a dict with the archiving load for each device
        self.attributes=CaselessDict() #a dict of ArchivedAttribute objects        
        self.dedicated = CaselessDefaultDict(lambda k:set()) #Dictionary for keeping the attributes reserved for each archiver
        
        if load and self.host and self.ArchivingClasses: 
          self.load_servers()
          
    ## The ArchivingAPI is an iterator through archived attributes
    def __getitem__(self,k): 
        k = k if k.count('/')<=3 else fandango.tango.get_normal_name(k)
        return self.attributes.__getitem__(k)
    def __contains__(self,k): 
        k = k if k.count('/')<=3 else fandango.tango.get_normal_name(k)
        return self.attributes.__contains__(k)
    def get(self,k): 
        k = k if k.count('/')<=3 else fandango.tango.get_normal_name(k)
        return self.attributes.get(k)
    def has_key(self,k): 
        k = k if k.count('/')<=3 else fandango.tango.get_normal_name(k)
        return self.attributes.has_key(k)
    #[setattr(self,method,lambda k,meth=method:getattr(self.attributes,meth)(k)) for method in ('__getitem__','__contains__','get','has_key')]
    def __iter__(self): return self.attributes.__iter__()
    def iteritems(self): return self.attributes.iteritems()
    def keys(self): return self.attributes.keys()
    def values(self): return self.attributes.values()
    def __len__(self): return len(self.attributes.keys())
    def items(self): return self.attributes.items()
    #[setattr(self,method,lambda meth=method:getattr(self.attributes,meth)()) for method in ('__iter__','iteritems','items','keys','values')]          
        
    def load_servers(self,filters=None):
        if getattr(self,'servers',None) is None:
            self.servers = ServersDict()
        [self.servers.load_by_name(k) for k in (filters or self.ArchivingClasses)]
        self.servers.refresh()
        self.proxies = self.servers.proxies
        return self.servers
        
    def get_servers(self):
        if not getattr(self,'servers',None): return self.load_servers()
        return self.servers
        
    def get_archiving_classes(self):        
        self.ArchivingClasses = [k for k in ARCHIVING_CLASSES if self.schema in k.lower()]
        if self.schema!='snap': self.ArchivingClasses.append('ArchivingManager')
        return self.ArchivingClasses

    def __del__(self):
        try:
            self.log.debug( 'Deleting ArchivingAPI ...')
            for p in self.proxies.values():
                del p
            del self.tango
            for d in self.dbs.values():
                del d
        except:
            pass #print('Unable to delete API object')
            
    def __repr__(self):
        '''The status of Archiving device servers '''
        return '%s(%s[%s])' % (type(self),self.schema,len(self))
        #if self.servers:
            #report='The status of %s Archiving device servers is:\n'%self.schema
            #for k,v in self.servers.items():
                #report+='%s:\t%s\n'%(k,v.state)
            #if self.WatcherClass:
                #try: report+=self.proxies(
                    #self.servers.get_class_devices(self.WatcherClass)[0]
                    #).command_inout('GetReportCurrent')+'\n'
                #except: pass
            #return report         

    ####################################################################################################

    def get_random_device(self,klass,timeout=300000):
        device = None
        if not getattr(self,'servers',None): 
          self.load_servers(filters=[klass])
        remaining = self.servers.get_class_devices(klass)
        if not remaining: 
          self.servers.load_by_name(klass)
          remaining = self.servers.get_class_devices(klass)
        while remaining: #for i in range(len(self.extractors)):
            next = randrange(len(remaining))
            devname = remaining.pop(next)
            device = self.servers.proxies[devname]
            print devname
            try:
                device.ping()
                device.set_timeout_millis(timeout)
                break
            except Exception,e: 
                self.log.info('%s unreachable: %s'%(devname,str(e)))
        return device

    def get_manager(self):
        ''' returns a DeviceProxy object '''
        d = self.get_random_device(self.ManagerClass)
        return d
    
    def get_extractor(self):
        ''' returns a DeviceProxy object '''
        return self.get_random_device(self.ExtractorClass)  
    
    def get_extractors(self):
        ''' returns a list of device names '''
        return self.servers.get_class_devices(self.ExtractorClass)  
    
    def get_archiver(self,archiver=''):
        ''' returns a DeviceProxy object '''
        if archiver: return self.proxies[archiver]
        else: return self.get_random_device(self.ArchiverClass)
    
    def get_archivers(self):
        ''' returns a list of device names '''
        if not self.servers: self.load_servers()
        return self.servers.get_class_devices(self.ArchiverClass)
    
    def get_watcher(self):
        ''' returns a DeviceProxy object '''
        if not self.WatcherClass: return None
        else: return self.get_random_device(self.WatcherClass) 
        
    def restart_manager(self):
        """ restarts the manager device server """
        server = self.servers.get_device_server(self.get_manager().name())
        self.servers.stop_servers(server)
        time.sleep(1.)
        self.servers.start_servers(server)
        

    ####################################################################################################            
    
###############################################################################
# SPECIAL CHECKS
###############################################################################

def repair_dedicated_attributes(api,attrs=None,load=True,restart=False):
    api.load_attribute_modes()
    tdedi = api.load_dedicated_archivers()
    tdediattrs = dict((a,d) for d,v in tdedi.items() for a in v)
    newconfig = dict((a,tdediattrs[a]) for a in (attrs or tdediattrs) if a in tdediattrs and a in api and api[a].archiver and tdediattrs[a]!=api[a].archiver)
    #rows = dict((a,tdb.db.Query('select ID,archiver,start_date from amt where STOP_DATE is NULL and ID=%d'%api[a].ID)) for a in newconfig.keys() if a in api)
    if restart:
        astor = fandango.Astor('ArchivingManager/1')
        astor.load_from_devs_list(list(set([api[a].archiver for a in newconfig]+newconfig.values())))
        astor.stop_servers()
    if load:
        print 'Updating %d dedicated attributes in amt.'%len(newconfig)
        for a,d in newconfig.items():
            api.db.Query("update amt set archiver='%s' where ID=%d and STOP_DATE is NULL"%(d,api[a].ID))
    if restart:
        astor.start_servers()
    return newconfig
  
def check_archived_attribute_names(device='*',attribute='*',schema='hdb'):
    import PyTangoArchiving
    api = PyTangoArchiving.ArchivingAPI(schema)
    devs = sorted(set(a.rsplit('/',1)[0] for a in api))
    unmatch = []
    for d in devs:
      if clmatch(device,d):
        if check_device(d):
          arch = [a for a in api if a.startswith(d+'/')]
          curr = map(str.lower,get_matching_attributes(d+'/*'))
          un = list(a for a in arch 
              if clmatch(attribute,a.rsplit('/',1)[-1])
              and a not in curr)
          if un:
            print('%s: %s'%(d,len(un)))
            unmatch.extend(un)
    return sorted(unmatch)


def check_attribute_modes(old_value,new_value,modes,tolerance=0.):
    """ it returns a dictionary {'MODE': True/False } to verify if the value should have been archived for each mode
    old_value is a tuple (epoch,value)
    new_value is a tuple (epoch,value)
    modes is a dictionary: {'MODE':[args]}
    """
    result = {}
    ellapsed = (new_value[0]-old_value[0])
    if None in (new_value[1],old_value[1]): diff = 0
    elif fun.isNumber(new_value[1]): diff = abs(new_value[1]-old_value[1])
    else: diff = [0,1e9][new_value[1]!=old_value[1]] # If not a number then any difference must be archived

    for mode,args in modes.items():
        period = (1.+tolerance)*args[0]/1000.
        if 'MODE_P' in mode:
            result['MODE_P'] = ellapsed<period
        elif 'MODE_A' in mode:
            result['MODE_A'] = ellapsed<period or diff<=args[1]
        elif 'MODE_R' in mode:
            result['MODE_R'] = ellapsed<period or diff<=(fun.isNumber(new_value[1]) and abs(args[1]*old_value[1]) or 1e8)
    return result
        
DB_MODES = {'per_mod':'MODE_P','abs_mod':'MODE_A','rel_mod':'MODE_R',
        'thr_mod':'MODE_T','cal_mod':'MODE_C','dif_mod':'MODE_D','ext_mod':'MODE_E',
        }
        
def translate_attribute_modes(modes):
    """
    Translates between the modes names used in database and in the Java API
    """
    dct = DB_MODES if any(k in str(modes) for k in DB_MODES) else dict((v,k) for k,v in DB_MODES.items())
    replace_modes = lambda k: reduce((lambda x,t:x.replace(*t)),dct.items(),k)
    if isinstance(modes,str):
        return replace_modes(modes)
    if isinstance(modes,dict):
        return dict((replace_modes(k),v) for k,v in modes.items())
    
def modes_to_string(modestring,translate=True):
    """
    used to convert between the format used by the JAVA Api and a dictionary
          String="attribute:MODE_P,1,MODE_A,1,2,3,MODE_R,1,2,3,..."
          Dictionary={'MODE_P':[1],'MODE_A':[1,2,3],'MODE_R':[1,2,3]}
    """
    if translate and any(k in str(modestring) for k in DB_MODES): 
        modestring = translate_attribute_modes(modestring)
    modes,nmodes=modestring,[]
    #Building a sorted list of modes
    porder = ['MODE_P','MODE_A','MODE_R','MODE_T','MODE_C','MODE_D','MODE_E']
    snum = lambda n: (n>.01 and '%1.2f'%n or '%1.6f'%n) if isinstance(n,float) else str(n)
    for o in porder: 
        if o in modes: 
            nmodes+=[o]+['%d'%int(modes[o][0])]+[snum(n) for n in modes[o][1:]] #First argument is integer period, for the rest ints will be ints, floats will be floats
    return ','.join(nmodes)
    
def modes_to_dict(modestring,translate=True):
    """ 
    Converts an string of modes and params separated by commas in a dictionary 
          String="attribute:MODE_P,1,MODE_A,1,2,3,MODE_R,1,2,3,..."
          Dictionary={'MODE_P':[1],'MODE_A':[1,2,3],'MODE_R':[1,2,3]}    
    """
    if translate and any(k in str(modestring) for k in DB_MODES): 
        modestring = translate_attribute_modes(modestring)
    modestring=modestring.replace(' ','')
    if not ':' in modestring: params=modestring
    else: attrib,params=modestring.split(':')
    modes={}
    for m in params.split('MODE_'):
        if not m: continue
        modes['MODE_'+m.split(',')[0]]=[]
        for p in [v for v in m.split(',')[1:] if v]:
            modes['MODE_'+m.split(',')[0]].append(float(p))   
    return modes

def get_duplicated_archivings(schema='hdb'):
    api = PyTangoArchiving.ArchivingAPI(schema)
    check = dict((a,list()) for a in api)
    archis = dict((h,check_device(h,command='StateDetailed')) for h in sorted(api.get_archivers()))
    for h in [a for a,v in archis.items() if v]:
        sd = api.get_archiver(h).StateDetailed().lower()
        for c in check:
            if c.lower() in sd.split():
                check[c].append(h)
    return sorted([(c,v) for c,v in check.items() if len(v)>1])

def reportArchiving():
    db = Database()

    rateDS = 5
    nDS = 20
    a=0
    for m in range(1,nDS+1):
        for n in range(1,rateDS+1):
            member = 'archiving/hdbarchiver/'+'%02d'%m+'-'+'%02d'%n
            print 'Reporting HdbArchiver: ',member
            dp=DeviceProxy(member)
            #print '\tStatus is:\n\t', dp.command_inout('Status')
            #print '\tState detailed is:\n\t', dp.command_inout('StateDetailed')
            print '\tScalar Charge is:\n\t', dp.read_attribute('scalar_charge').value
            a=a+dp.read_attribute('scalar_charge').value
    
    print 'Total Scalar Charge is ... ',a

def KillAllServers(klass = 'HdbArchiver'):
    processes = linos.shell_command('ps uax').split('\n')
    archivers = [s for s in processes if '%s.%s'%(klass,klass) in s]
    for a in archivers:
        print 'Killing %s' % a[1:]
        pid = a.split()[1]
        linos.shell_command('kill -9 %s'%pid)
        
def force_stop_attributes(schema,attr_list):
    """
    This method will stop archivers, modify tables, and restart archivers to ensure that archiving is stop.
    """
    import fandango
    api = PyTangoArchiving.ArchivingAPI(schema)
    attr_list = [a for a in attr_list if a in api and api[a].archiver]
    arch = list(set(api[a].archiver for a in attr_list))
    astor = fandango.Astor()
    astor.load_from_devs_list(arch)
    astor.stop_servers()
    for s in attr_list:
        query = "update amt set stop_date=now() where ID = %s and stop_date is NULL"%api[s].ID
        print query
        api.db.Query(query)
    astor.start_servers()  
    
def restart_attributes_archivers(schema,attributes,action=False):
    import PyTangoArchiving
    api = PyTangoArchiving.api(schema)
    devs = fandango.defaultdict(list)
    [devs[api[a].archiver].append(a) for a in attributes]
    if not action:
      print('%d archivers to restart, call with action=True to execute it'%len(devs))
    else:
      print('Restarting %d archivers'%len(devs))
      astor = fandango.Astor()
      astor.load_from_devs_list(devs.keys())
      astor.stop_servers()
      time.sleep(10.)
      astor.start_servers()
    return dict((k,len(v)) for k,v in devs.items())

def tdb_to_hdb(attribute,start=0,stop=fun.END_OF_TIME,modes={},delete=False):
    """
    This method allows to copy an attribute from TDB to HDB, inserting the contents
    of the current TDB buffer into the HDB tables.
    @param start/stop allow to limit the dates for insertion
    @param delete will remove all existing values in HDB for the given interval
    """
    from PyTangoArchiving import Reader
    hdb,tdb = Reader('hdb'),Reader('tdb')
    assert attribute in tdb.get_attributes()
    values = tdb.get_attribute_values(attribute,start or time.time()-tdb.RetentionPeriod,stop)
    if attribute not in hdb.get_attributes(): 
        from PyTangoArchiving import ArchivingAPI
        api = ArchivingAPI('hdb')
        api.start_archiving(*((attribute,modes) if modes else (attribute,)))
        api.load_attribute_descriptions()
    db = hdb.get_database()
    table = db.get_table_name(db.get_attribute_ID(attribute))
    import_into_db(db,table,values,delete)    
    
    
def get_average_read_time(api='hdb',period=10*3600*24,N=100):
    if fandango.isString(api):
        import PyTangoArchiving
        api = PyTangoArchiving.ArchivingAPI(api)
    reader = api.get_reader()
    active = [a for a in api.get_archived_attributes() if api[a].data_type not in (1,8)]
    target = [active[i] for i in fandango.randomize(range(len(active)))][:int(2*N)]
    stats = []
    navg,tavg,count = 0,0,0
    print('testing %s %s attributes'%(len(target),api.schema))
    for t in target:
        if count == N: 
            break
        t0 = time.time()
        try: 
            vs = reader.get_attribute_values(t,time.time()-period-3600,time.time()-3600)
            if not len(vs): 
                continue
        except: 
            continue
        t1 = time.time()-t0
        if not count%10:
            print(count,':',t,len(vs),t1)
        navg += len(vs)
        tavg += t1
        count += 1
        stats.append((t1,len(vs),t))
    N = float(count)
    print('Worst tread were: \n%s'%'\n'.join(map(str,sorted(stats)[-10:])))
    return (N, (N>0 and navg/N),(N>0 and tavg/N))

def check_backtracking(api,schema='hdb',attributes=[],nvalues=10):
    import archiving
    backtrackers = []
    now = time.time()
    if not api: api = archiving.ArchivingAPI(schema)
    get_size = lambda a: api.db.Query('select count(*) from %s'%api[a].table)[0][0]
    schema = api.schema.lower()
    for aname in (attributes or api.keys()):
        try:
            #print 'Checking %s'%aname
            attr = api[aname]
            if not attr.archiver: continue
            api.load_last_values(attr.name)
            if not attr.last_date: continue
            if attr.last_date<(now-3600): continue
            size = get_size(aname)
            if not size: continue
            limits = (size-nvalues,nvalues) if schema=='hdb' else (0,nvalues)
            query = 'select time from %s limit %d,%d'%(attr.table,limits[0],limits[1])
            print "%s.db.Query('%s')"%(schema,query)
            values = api.db.Query(query)
            #print values
            iterator = values if schema=='hdb' else list(reversed(values))
            for i,v in enumerate(iterator[1:]):
                if fun.date2time(v[0])<fun.date2time(iterator[i][0]):
                    backtrackers.append(aname)
                    print '%s ATTRIBUTE IS BACKTRACKING!!!:'# %s < %s'%(aname,v[0],iterator[i][0])
                    try: print '\t%d: %s'%(i,iterator[i+2][0])
                    except: pass
                    print '\t%d: %s'%(i,iterator[i+1][0]) #v[0]
                    print '\t%d: %s'%(i,iterator[i][0])
                    if i: print '\t%d: %s'%(i,iterator[i-1][0])
                    if i>1: print '\t%d: %s'%(i,iterator[i-2][0])
                    break
        except:
            print 'check_backtracking(%s,%s): Failed! \n%s'%(schema,aname,traceback.format_exc())
    return list(set(backtrackers))

#########################################################################################    
    
###############################################################################

def getSingletonAPI(*args,**kwargs):
    return CommonAPI.get_singleton(*args,**kwargs)
