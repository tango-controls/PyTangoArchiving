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

        self.tango = PyTango.Database() #access to Tango database
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
        self.log.debug( 'Deleting ArchivingAPI ...')
        for p in self.proxies.values():
            del p
        del self.tango
        for d in self.dbs.values():
            del d
            
    def __repr__(self):
        '''def server_Report(self): The status of Archiving device servers '''
        report='The status of %s Archiving device servers is:\n'%self.schema
        for k,v in self.servers.items():
            report+='%s:\t%s\n'%(k,v.state)
        if self.WatcherClass:
            try: report+=self.proxies(self.servers.get_class_devices(self.WatcherClass)[0]).command_inout('GetReportCurrent')+'\n'
            except: pass
        self.log.debug(report)
        return report         

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

def getSingletonAPI(*args,**kwargs):
    return CommonAPI.get_singleton(*args,**kwargs)
