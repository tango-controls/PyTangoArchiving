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
PyTangoArchiving methods for loading/exporting into text files
"""

import sys,time,os,re,traceback
from collections import defaultdict
from xml.dom import minidom
from os.path import abspath

# by Sergi Rubio Manrique, srubio@cells.es
# ALBA Synchrotron Control Group

import fandango
from fandango.arrays import CSVArray
import fandango.functional as fun
import PyTangoArchiving
from PyTangoArchiving.utils import PyTango
import PyTangoArchiving.utils as utils
from PyTangoArchiving.common import modes_to_string, modes_to_dict


ARCHIVING_CONFIGS =  os.environ.get('ARCHIVING_CONFIGS','/data/Archiving/Config')
RESULT = {} # Last result of each test is stored for convenience

def GetConfigFiles(folder=ARCHIVING_CONFIGS,mask='.*.csv'):
    print 'In GetConfigFiles(%s,%s)' % (folder,mask)
    return sorted(['%s/%s'%(folder,f) for f in os.listdir(folder) if fun.matchCl(fun.toRegexp(mask),f)])

def getAPI(schema,dedicated=False):
    """ 
    :param schema: hdb/tdb
    """
    api = PyTangoArchiving.ArchivingAPI(schema,load=False) #lightweight api
    api.load_attribute_descriptions()
    api.load_attribute_modes()    
    api.load_servers()
    return api

def LoadArchivingConfiguration(filename, schema,launch=False,force=False,stop=False,dedicated=False, check=True,overwrite=True, centralized=None,failed=None,unavailable=None,hosts=None,api=None,filters=None,exclude=None,silent=True):
    """
    This is the function used to load an archiving configuration from a CSV file and start archiving. 
    **NOTE**: The function must be called separately for each archiving type LoadArchivingConfiguration(..,schema='hdb' or 'tdb')
    
    - There's a single mandatory argument:
        :param filename:
        :param schema: HDB or TDB; the objects that do not match the schema will be discarded; it forces the script to be called separately for TDB and HDB
    
    - Several flags help to control the process:
        :param launch: the configuration will be launched or simply reviewed; False by default
        :param force: if forced then all the errors are summarized at the end, if not configuration is interrumpted at first error; False by default
        :param overwrite: whether the attributes already archived are modified or not; True by default
        :param check: it pruns all attributes not available
        :param stop: the configuration will be used to STOP the archiving instead of starting it; False by default
        :param dedicated: the attributes will be assigned to specific host archivers instead of using generic ones; False by default
        :param centralized: the archivers created will be started in the central server instead of distributed
    
    - Other arguments help to control where the information is stored
        :param failed: attributes that couldn't be configured
        :param hosts: {hosts:[attributes]} dictionary
        :param unavailable: list of unavailable attributes
        :param api: ArchivingAPI object to use
    
    :return: if param silent is False a dictionary with {modes:[attributes],FAILED:[attributes]} is returned
    """
    RESULT = {} #PyTangoArchiving.files.RESULT
    #RESULT.clear()
    
    ###########################################################################
    # Loading a list of schemas
    if fun.isRegexp(schema):
        print '>>> %s is a regexp, Loading each matching schema separately'%schema
        for s in PyTangoArchiving.ArchivingAPI.SCHEMAS:
            if not fun.matchCl(fun.toRegexp(schema),s): continue
            else: RESULT[s]=LoadArchivingConfiguration(filename,s,launch,force,stop,dedicated,check,overwrite,centralized,failed,unavailable,hosts,api,filters,exclude)
        return RESULT

    ###########################################################################
    # Loading a single schema
    failed,unavailable,hosts = fun.notNone(failed,[]),fun.notNone(unavailable,[]),fun.notNone(hosts,defaultdict(list))
    centralized = centralized if centralized is not None else (True if schema.lower().strip()=='tdb' else False)
    filters,exclude = fun.notNone(filters,{}),fun.notNone(exclude,{})
    tstart = time.time()
    
    if schema and schema.lower() not in filters.get('type','').lower(): 
        filters['type'] = filters.get('type') and '(%s|%s)'%(filters['type'],schema) or schema
    if 'stop' not in exclude.get('type','').lower(): 
        exclude['type'] = exclude.get('type') and '(%s|%s)'%(exclude['type'],'stop') or 'stop'
                
    print '>>> In LoadArchivingConfiguration(%s,%s,launch=%s,dedicated=%s,force=%s,overwrite=%s,filters=%s,exclude=%s)'\
        %((filename,schema,launch,dedicated,force,overwrite,filters,exclude,))
    
    if dedicated:
        raise Exception('Launch DedicateArchiversFromConfiguration(config,schema,hosts,centralized) first!')
        #DedicateArchiversFromConfiguration(config,schema=schema,launch=launch,filters=filters,exclude=exclude,hosts=hosts,centralized=centralized)
    
    config = ParseCSV(filename,filters=filters,exclude=exclude) # Attributes not for this schema will be pruned    
    n_all = len(config)
    print '>>> %d attributes read from %s file'%(len(config),filename)
    api = api or getAPI(schema)
    
    if check:
        print '>>> Pruning attributes not available ...'
        unavailable.extend([attr for attr in config if not utils.check_attribute(attr,readable=False)])#True)])
        if unavailable:
            print '\n%d attributes are not available!!!\n' % len(unavailable)
            if force: 
                [config.pop(att) for att in unavailable]
            else: raise Exception, 'Attributes not available: %s'%fun.list2str(unavailable)    
    
    #Attributes classified by Mode config
    modes = defaultdict(list)    
    for k,v in sorted(config.items()): 
        mode = modes_to_string(api.check_modes(schema,v[schema.upper()]))
        modes[mode].append(k)
    
    #The active part
    archivers_to_restart = set()
    if launch or stop:
        #Archiving started in groups of 10 attributes
        for mode in sorted(modes.keys()):
            alist = modes[mode] #We will modify the list on the run
            done = []
            if not overwrite: 
                print 'NOTE: Only those attributes not already archived will be modified in the database.'
                alist = [a for a in alist if a not in api or not api.attributes[a].archiver]
            else:
                archs = set(api[a].archiver for a in alist if a in api and api[a].archiver)
                archivers_to_restart.update(api.servers.get_device_server(d) for d in archs)
            devs = defaultdict(list)
            [devs[a.rsplit('/',1)[0]].append(a) for a in alist] #The devices will be inserted separately for each device
            for dev,attributes in sorted(devs.items()):
                attributes = sorted(attributes)
                for i in range(1+int((len(attributes)-1)/10)): 
                    attrs = attributes[i*10:(i+1)*10]
                    if stop:
                        if api.stop_archiving(attrs,load=False):
                            done.extend(attrs)
                        else:
                            if force: failed.extend(attrs)
                            else: raise Exception,'Archiving stop failed for: %s'%(attrs)
                    if launch:
                        if api.start_archiving(attrs,modes_to_dict(mode),load=False,retries=1):
                            done.extend(attrs)
                        else:
                            if force: failed.extend(attrs)
                            else: raise Exception,'Archiving start failed for: %s'%(attrs)
            modes[mode] = done
        api.load_all(values=False,dedicated=dedicated)
        
    ##Final Report
    if unavailable: 
        print 'Attributes not available: %s'%fun.list2str(unavailable)
    if failed: 
        print 'Attributes unable to start/stop archiving: %s' % fun.list2str(failed)
        RESULT['FAILED'] = failed
    #failed.extend(unavailable)
    if not launch: 
        print ('THE ARCHIVING OF THE ATTRIBUTES HAS NOT BEEN PROCESSED, EXECUTE LoadArchivingConfiguration(%s,launch=True) TO DO IT'%filename)
    else: 
        print '-'*80
        print ('%d attributes requested, %d have been introduced into archiving, %d failed, %d unavailable' %(n_all,n_all-len(unavailable)-len(failed),len(failed),len(unavailable)))
        if archivers_to_restart:
            print '-'*80
            print 'Restarting %d archivers that have been modified ...'%len(archivers_to_restart)
            api.servers.stop_servers(archivers_to_restart)
            time.sleep(5.)
            api.servers.start_servers(archivers_to_restart)
            print 'DONE'
            print '-'*80
    
    print 'LoadArchivingConfiguration finished in %f minutes.' % ((time.time()-tstart)/60.)
    RESULT.update(modes)
    return None if silent else RESULT
    
def DedicateArchiversFromConfiguration(filename, schema,launch=True,restart=False,force=False,filters=None,exclude=None,hosts=None,centralized=False,load=True):
    ''' This is the function used to load a list of attributes and hosts 
    **NOTE**: The function must be called separately for each archiving type LoadArchivingConfiguration(..,schema='hdb' or 'tdb')
    
    - Filename argument is mandatory, it can be a dictionary like {'attribute':{'host':'hostname'}} to override .csv assignation
        :param filename: it could be a file name or a config already parsed using ParseCSV
        :param schema: HDB or TDB; the objects that do not match the schema will be discarded; it forces the script to be called separately for TDB and HDB
        :param launch: if True device servers will be configured 
        :param restart: by default all modified device servers will be restarted
    
    - Other arguments help to control where the information is stored
        :param hosts: {hosts:[attributes]} dictionary
        :param centralized: if host is set it will start all servers in a single host (dedicated but not distributed)
    
    :return: A dictionary with {modes:[attributes]} is returned
    '''
    
    # Setting Arguments ###############################################################################
    hosts = fun.notNone(hosts,defaultdict(list))
    assert type(hosts)==defaultdict,'DedicateArchiversFromConfigurationException: hosts argument must be a collections.defaultdict(list) object'
    if hosts: hosts.clear()
     
    filters,exclude = fun.notNone(filters,{}),fun.notNone(exclude,{})
    tstart = time.time()
    
    all_devs = map(str.lower,fandango.get_all_devices())
    api = PyTangoArchiving.ArchivingAPI(schema,load=False) #lightweight api
    api.load_servers()
    if centralized and not fun.isString(centralized):
        centralized = api.host.split('.')[0]    
    
    if filename is not None:
        if isinstance(filename,dict):
            config,filename = filename,'user_values'
        else:
            if schema and schema.lower() not in filters.get('type','').lower(): 
                filters['type'] = filters.get('type') and '(%s|%s)'%(filters['type'],schema) or schema
            if 'stop' not in exclude.get('type','').lower(): 
                exclude['type'] = exclude.get('type') and '(%s|%s)'%(exclude['type'],'stop') or 'stop'
            config = ParseCSV(filename,filters=filters,exclude=exclude) # Attributes not for this schema will be pruned        
    ###################################################################################################
        
    print ('In DedicateArchiversFromConfiguration(%s,schema=%s,centralized=%s,filters=%s,exclude=%s)'%(filename,schema,centralized,filters,exclude))        
    print '>>> Configuring dedicated archiving, creating new archiver servers and starting them if needed ...'
    assigned = api.load_dedicated_archivers()
    
    #Filtering already assigned or unexistent attributes
    for k,v in sorted(config.items()): 
        if any(k.lower() in atts for dev,atts in assigned.items()): continue
        dev,attr = k.lower().rsplit('/',1)
        if dev not in all_devs or fandango.check_device(dev) and attr not in map(str.lower,fandango.get_device(dev).get_attribute_list()):
            if not force:
                print '%s attribute doesnt exist!'%k
                return
        else:
            hosts[v['host'].lower().split('.')[0]].append(k.lower())
        
    if not any(hosts.values()):
        print 'Dedicated archiving not changed ...'
        if not force: return

    if launch:
        ########################################################
        assigned = api.set_dedicated_archivers(hosts,20 if schema.lower()=='tdb' else 30,create=launch) #<----------------All the properties changes are done here!
        ########################################################
        
        # Restarting modified servers and archiving manager
        try:
            if restart:
                manager = api.servers.get_device_server(api.get_manager().name())
                api.servers.stop_servers(manager)
                print 'ArchivingManager stop, restarting archivers ...'
                all_servers = list(set([api.servers.get_device_server(archiver) for archiver,attrs in assigned.items() if attrs]))
                
                for host,vals in hosts.items():
                    servers = [s for s in all_servers if host.split('.')[0].lower() in s.lower()]
                    print 'Restarting the Dedicated archiving servers in host %s: %s' % (host,servers)
                    for server in servers:
                        try:
                            if api.servers[server].ping() is not None: api.servers.stop_servers(server)
                        except Exception,e: print 'The server may be not running: %s'%str(e)
                        print 'waiting some seconds for stop_servers ...'
                        time.sleep(10.)
                    for server in servers:
                        try:
                            api.servers.start_servers(server,centralized or host,wait=30.)
                            print 'waiting some seconds for start_servers ...'
                            time.sleep(10.)
                            api.servers.set_server_level(server,centralized or host,4)
                            time.sleep(1.)
                        except Exception,e:
                            if not force:
                                raise e
                            else:
                                print 'UNABLE TO RESTART %s'%server
                                [[failed.append(a) for a in assigned.get(d,[]) if a not in failed] for d in api.servers[server].get_device_list()]
                time.sleep(10.)
                api.servers.start_servers(manager,wait=30.)
                print 'waiting some seconds after ArchivingManager restart ...'
                time.sleep(10.)
            else:
                print '!!! %sArchiver and ArchivingManager devices must be restarted to finish the setup !!!' % schema
        except Exception,e:
            print traceback.format_exc()
            print 'Dedicated Archiving failed! ... restarting the ArchivingManager'
            if launch: api.servers.start_servers(manager,wait=60.)
            raise Exception('Dedicated Archiving failed!')
        if load: api.load_all(values=False,dedicated=True)
        print 'Dedicated archiving configuration finished ...'
    else:
        print 'Dedicated archiving have been verified but not executed; you must repeat command with launch=True'
    return hosts
    
class ModeCheckNotImplemented(Exception): 
    pass    
    
    
REPORT_LEGEND = {
    'ok':'attributes properly archived',
    'unavailable':'attributes properly archived',
    'late':'attributes not updated in the last periodic archiving interval',
    'hung':'attributes not archived in the last hours', #archivers will be restarted
    'lost':'attributes not assigned to any archiver', #It will be retried
    'retried':'attributes which archiving have been restarted in the last cycle',
    'diff':'attributes which configuration differ from files', 
    'missing':'attributes never added to archiving',
    'triable':'missing attributes ready to be added',
    'polizon':'attributes from same devices that are being archived but not declared in this file.',
    
    }
def CheckArchivingConfiguration(filename,schema,api=None,check_modes=False,check_conf=True,period=3600,filters=None,exclude=None,restart=False):
    """
    This function checks all attribute archiving configurations in the file and verifies their actual status.
    
    :param schema: hdb/tdb
    :return dict: Stats
    
    Returns a dictionary of lists tagged as follows:
        'ok':'attributes properly archived',
        'unavailable':'attributes properly archived',
        'late':'attributes not updated in the last periodic archiving interval',
        'hung':'attributes not archived in the last hours',
        'lost':'attributes not assigned to any archiver',
        'retried':'attributes which archiving have been restarted in the last cycle',
        'diff':'attributes which configuration differ from files', 
        'missing':'attributes never added to archiving',
        'triable':'missing attributes ready to be added',
        'polizon':'attributes from same devices that are being archived but not declared in this file.',
    """
    print "In CheckArchivingConfiguration(%s,restart=%s)" % ((filename,schema,period,filters,exclude),restart)
    result,now = {},time.time()    
    tload,t0 = 0,0
    filters = fun.notNone(filters,{})
    exclude = fun.notNone(exclude,{})
    
    if schema and schema.lower() not in filters.get('type','').lower(): 
        filters['type'] = filters.get('type') and '(%s|%s)'%(filters['type'],schema) or schema
    attributes = dict((k.lower(),v[schema.upper()]) for k,v in ParseCSV(filename,schema=schema.lower(),filters=filters,exclude=exclude).items())
    if not len(attributes):
        return {} #No attributes in the file match the given filter

    api = api or PyTangoArchiving.ArchivingAPI(schema)    
    api.load_attribute_modes()
    ## A dictionary gets statistics of attributes archived by each archiver. If the rate is negative the archiver will be restarted.
    archivers = dict.fromkeys(list(set(api[a].archiver for a in attributes if a in api and api[a].archiver)),0)
    idles = [k for k,v in api.check_archivers(archivers.keys()).items() if not v] #Getting all archivers not running properly
    
    devices = [a.rsplit('/',1)[0] for a in attributes]
    STATS = defaultdict(list)
    STATS['all'] = attributes.keys()
    #ok,unavailable,late,missing,triable,diff,retried,lost,hung,dedicated = [],[],[],[],[],[],[],[],[],[]
    print "Checking %s attributes ..." % len(attributes)
    retriable = defaultdict(list)
    valuable = []
    for att,modes in attributes.items():
        try:
            available = utils.check_attribute(att,readable=False,timeout=2*3600)#True) #I do not want to exclude piranis!!!
            attIsNone = not available or isinstance(available,PyTango.DevFailed) or getattr(available,'value',None) is None
            archived = att in api and api[att].archiver
            
            if not available: 
                STATS['unavailable'].append(att) 
            elif archived:
                valuable.append((att,modes,attIsNone))
            elif att in api: 
                #Was archived in the past
                STATS['lost'].append(att)
                print '%s is LOST, no archiver assigned!'%(att)
                if restart:
                    #Dedicated configuration is not done here!! ... this is just for restarting temporarily unavailable attributes
                    retriable[modes_to_string(api.check_modes(api.schema,modes))].append(att)
            else:
                #Never archived before
                STATS['missing'].append(att)
                if available: STATS['triable'].append(att)
        except ModeCheckNotImplemented,e: 
                raise e
        except Exception,e: 
                print 'In CheckArchivingConfiguration(...): %s check failed!: %s' % (att,e)
                STATS['missing'].append(att)
    
    t0 = time.time()
    all_values = api.load_last_values([t[0] for t in valuable],cache=period)
    tload+=time.time()-t0
    print '\t%2.2f seconds loading values.'%tload
    for att,modes,attIsNone in valuable:
        try:
            if api[att].dedicated: STATS['dedicated'].append(att)
            max_period = max([2*600]+[period]+[mode[0]/1000. for mode in api[att].modes.values()]) 
            vals = all_values[att] #api.load_last_values(att,cache=max_period)
            date = utils.date2time(vals[0][0]) if vals else 0
            value = vals[0][1] if vals else None

            if date>=(now-max_period) and (attIsNone or value is not None): 
                STATS['ok'].append(att)
                archivers[api[att].archiver]+=1
            else:
                # Sometimes it's better to restart the archiver device, it will be done if its rate is <0
                # It it is not, then the hung attributes will be added to retry list (see end of this method)
                if date>=(now-5*max_period) and value is not None: #(24*3600) 
                    STATS['late'].append(att)
                else: 
                    STATS['hung'].append(att)
                    #print '%s is HUNG, %s not updated since %s, archiver = %s'%(att,value,time.ctime(date),api[att].archiver)
                archivers[api[att].archiver]-=1
                
            if check_conf:
                mode_to_str = lambda m: (
                    modes_to_string(api.check_modes(api.schema,
                        m if 'MODE_A' not in m and 'MODE_R' not in m else dict((k,v) for k,v in m.items() if k!='MODE_P'),
                        )))
                m1,m2 = mode_to_str(api.check_modes(api.schema,modes)),mode_to_str(api[att].modes)
                if m1!=m2:
                    #print '%s.modes differ: file:"%s"!=db:"%s"' % (att,m1,m2)
                    STATS['diff'].append(att)
            if check_modes:
                raise ModeCheckNotImplemented
        except ModeCheckNotImplemented,e: 
                raise e
        except Exception,e: 
                print 'In CheckArchivingConfiguration(...): %s check failed!: %s' % (att,e)
                STATS['missing'].append(att)
                
    STATS['polizon'] = [a for a in api.attributes if a.rsplit('/',1)[0] in devices and api[a].archiver and a not in attributes]
    if STATS['polizon']: print '%d Attributes not in list but archived from same devices'%len(STATS['polizon'])
    
    summary = ', '.join(['%s:%s'%(k.upper(),len(v)) for k,v in sorted(STATS.items()) if v])
    STATS = dict((k,sorted(l)) for k,l in STATS.items())        
    STATS['rate'] = (float(len(STATS['ok'])+len(STATS.get('unavailable')))/len(STATS['all'])) if (STATS.get('ok') and STATS.get('all')) else 0.
    print ('CheckArchivingConfiguration(%s,%s): attribute check in %2.2f seconds:'%(filename,schema,time.time()-now))+'\n\t'+summary
    
    if restart and (idles or STATS.get('hung',[]) or retriable):
        STATS['retried'] = []
        now = time.time()
        api.load_servers()
        api.load_attribute_modes()
        idles.extend([a for a,v in archivers.items() if v<0 and a not in idles])
        
        if idles: 
            print '---> Restarting %d faulty archivers: %s' % (len(idles),idles)
            servers = list(set(api.servers.get_device_server(d) for d in idles))
            print '------> Restarting %d faulty servers: %s' % (len(servers),servers)
            api.servers.kill_servers(servers)
            time.sleep(5.)
            api.servers.start_servers(list(set(api.servers.get_device_server(d) for d in idles)))
            
            print '%s ---> Waiting for archivers to restart ...'%time.ctime()
            nn = time.time()
            while time.time()<(nn+150):
                try: 
                    api.servers.proxies[idles[-1]].state()
                    break
                except: pass
                finally: time.sleep(10.)
            api.load_attribute_modes()
        
        ## hung attributes will be retried depending on their archiver rate (if rate is <0 the archiver will be restarted instead)
        for att in STATS.get('hung',[]):
            if not api[att].archiver or api[att].archiver not in idles: #Adding not-idle attributes to retriable list
                modes = attributes[att]
                retriable[modes_to_string(api.check_modes(api.schema,modes))].append(att)
        print '%s ---> Restarting %d archiving modes'%(time.ctime(),len(retriable))
        
        for modes,attrs in retriable.items():
            print '%s ---> Restarting %s archiving for %d attributes' % (time.ctime(),modes,len(attrs))
            try: 
                modes = modes_to_dict(modes)
                targets = [a for a in attrs if not api[a].archiver or api[a].archiver not in idles]
                if targets: 
                    if not api.start_archiving(targets,modes,load=False):
                        '--------> start_archiving(%s) failed with no exception'%targets
                    STATS['retried'].extend(targets)
            except: print traceback.format_exc()

        print '%s: %s[%s] restart finished after %s seconds'%(time.ctime(),filename,len(STATS['retried']),time.time()-now)
        
    return STATS
   
def CheckConfigFilesForSchema(schema):
    import PyTangoArchiving as pta
    tables = pta.dbs.get_table_updates()
    api = pta.ArchivingAPI(schema)
    csvs = pta.files.GetConfigFiles()
    csvapi = [a for f in csvs for a in pta.ParseCSV(f,schema.upper())]
    notloaded = [a for a in csvapi if a not in api]
    oknotloaded = [a for a in notloaded if pta.check_attribute(a)]
    notloaded = [a for a in csvapi if a not in api or not api[a].archiver]
    oknotloaded = [a for a in notloaded if pta.check_attribute(a)]
    updated = [a for a in api if api[a].table in tables and tables[api[a].table] > time.time()-24*3600]
    csvupdated = [a for a in csvapi if a in updated]
    for k,v in sorted(vars().items()):
      if fandango.isSequence(v):
        print('%s : %s'%(k,len(v)))
    return

def SummarizeStats(STATS):
    return ', '.join(['%s:%s'%(k.upper(),len(v) if hasattr(v,'__len__') else v) for k,v in sorted(STATS.items()) if v])
    
def CheckAttributesAvailability(filename='',attributes=[],readable=False):
    """ 
    Returns for each attribute in the file the actual status:
    { a: None, #attribute not available
    { a: a, #attribute available and readable
    { a: exception, #attribute exists but is not readable
    """
    if not (attributes or filename): raise Exception('ArgumentRequired')
    if fun.isSequence(filename): filename,attributes = '',filename
    attributes = [str(a).lower() for a in (attributes or ParseCSV(filename))]
    RESULT = dict((a,bool(utils.check_attribute(a,readable))) for a in attributes)
    return RESULT
    
def StopArchivingConfiguration(filename,schema,all=True,api=None):
    """
    Stops the archiving for the attributes of all the devices appearing in a config file
    :param schema: hdb/tdb
    """
    if not api:
        api = PyTangoArchiving.ArchivingAPI(schema,load=False) #lightweight api
        api.load_attribute_descriptions()
        api.load_attribute_modes()    
    config = ParseCSV(filename,schema=api.schema)
    print 'In StopArchivingConfiguration(%s): %d attributes found' % (filename,len(config))
    devices = set([c.rsplit('/',1)[0].lower() for c in config])
    for dev in devices:
        attrs = [a for a in api if a.startswith(dev) and api[a].archiver]
        if attrs: api.stop_archiving(attrs,load=False)
        else: print 'No attributes currently archived for %s' % dev
        time.sleep(3.)
    api.load_all(values=False)
    return
    
import re

def ExportToCSV(api,mask,filename):
    #mask = '.*/vc/.*/.*'
    #filename = '/homelocal/sicilia/vacuum.csv'
    
    vcs = dict([(k,v) for k,v in api.attributes.items() if re.match(mask,k)])
    
    f = open(filename,'w')
    
    devices = defaultdict(dict)
    for a,v in vcs.items():
        try: devices[a.rsplit('/',1)[0]][a.rsplit('/',1)[-1]+';'+str(v.extractModeString(v.modes))]=v
        except Exception,e: '%s failed: %s' % (a,str(e))
    
    modes = defaultdict(dict)
    for d in devices:
        modes[str(sorted(devices[d].keys()))][d]=devices[d]
    
    values = []
    values.append(['#dserver host','domain/family/member','attribute','double/long/short/boolean/string/spectrum','periodic/absolute/relative'])
    for mode,devices in sorted(modes.items()):
        model = sorted(devices.keys())[0]
        default = devices[model].values()[0].modes.get('MODE_P',[300000])[0] /1000
        values.append(['',model,'@DEFAULT','','periodic',default])
        for a,v in sorted(devices[model].items()):
            for m,p in sorted(v.modes.items()):
                if m=='MODE_P' and len(v.modes)>1:continue
                line = ['','',v.name.rsplit('/',1)[-1],str(v.data_type)]
                line.append({'MODE_P':'periodic','MODE_R':'relative','MODE_A':'absolute'}[m])
                line.append(str(p[0]/1000))
                line.extend([str(s) for s in p[1:]])
                values.append(line)
        for d in sorted(devices.keys())[1:]:
            values.append(['',d,'@COPY:%s'%model])
        values.append([])
    
    f.write('\n'.join(['\t'.join([str(s) for s in v]) for v in values]))
    f.close()
    RESULT = values
    return len(values)
        
#############################################################################################################
# Methods used by the show_history widget

def get_data_filename(var,data=None,fileformat='pck',dateformat='epoch'):
    fname = var.replace('/','.')
    if data is not None and len(data): 
        if dateformat == 'epoch' or True:
            date = '%d-%d'%(int(data[0][0]),int(data[-1][0]))
        elif dateformat == 'human':
            time2human = lambda t: re.sub('[^0-9]','',fun.time2str(t))
            date = '%s-%s'%(time2human(data[0][0]),time2human(data[-1][0]))
        fname += '--%s'%date
    fname += '.'+fileformat
    return fname

def load_data_file(var,folder=''):
    if '.pck' not in var and '.csv' not in var: var = get_filename(var)
    varname = var.rsplit('.',1)[0].split('/')[-1].split('--')[0]
    if folder and '/' not in var: var = folder+'/'+var
    if 'pck' in var:
        data = pickle.load(open(var))
    elif 'csv' in var:
        data = []
        for l in open(var).readlines():
            while l:
                try: 
                    float(l[0])
                    break
                except: l = l[1:]
            data.append(map(float,map(str.strip,l)))
    return {varname:data}

def save_data_file(var,data,filename='',folder='',format='pck',**kwargs): #format in 'pck' or 'csv'
    """
    This method will be use to export archived data to 'csv' or 'pck' formats.
    Kwargs can be used to pass arguments to Reader.export_to_text
    """
    path = folder+'/'+(filename or get_data_filename(var,data,format))
    kwargs = kwargs or {'arrsep':' '}
    print 'Saving %d registers to %s ...'%(len(data),path)
    if format == 'csv':
        text = PyTangoArchiving.Reader.export_to_text({var:data},**kwargs)
        open(path,'w').write(text)
    else:
        pickle.dump(data,open(path,'w'))
    return path            
            
#############################################################################################################
#ParseCSV

def ParseCSV(filename,schema='',filters=None,exclude=None,dedicated=None,deletelist=None,context=None,check_modes=True,log=False):
    ''' 
    Extracts all information from an achiving configuration .csv file
    
    :param filename: is the file from where the Array is loaded
    :param schema: can be used to filter the 'Type' column in the CSVs
    :param filters: is a dictionary {name/type/host/modes:filter} that will add only those attributes where column matches the filter (lowercase)
    :param exclude: dictionary with regexp for elements to exclude, keys may be host,name or type(schema)
    
    :param dedicated: is a dictionary with key=attribute and value=archiver 
    :param deletelist: is a list, contains the attributes to be removed from archiving
    :param context: is a dictionary, contains the information about the authority of the configuration    
    
    :return: a dictionary like {attribute:{host:str,$MODE:{str:[int]},type:str}} # Where MODE in (HDB,TDB,STOP)
    In addition the dedicated, deletelist and context argumens can be used to get more detailed information.
    '''
    dedicated,deletelist,context = \
        fun.notNone(dedicated,{}),fun.notNone(deletelist,[]),fun.notNone(context,{})
    
    filters,exclude = fun.notNone(fandango.CaselessDict(filters),{}),fun.notNone(fandango.CaselessDict(exclude),{'type':'stop'})
    if schema and schema.lower() not in filters.get('type','').lower(): 
        filters['type'] = filters.get('type') and '(%s|%s)'%(filters['type'],schema) or schema
    if filters.get('device') and not filters.get('name'): filters['name'] = filters['device']
    if 'name' not in filters and 'attribute' in filters: filters['name'] = filters['attribute']
    
    def trace(msg):
        if log: print msg
    trace('In ParseCSV(%s,%s,%s,-%s) ...'%(filename,schema,filters,exclude))
    
    config = CSVArray()
    config.trace = log
    config.load(filename,comment='#')
    assert len(config.rows)>1, 'File is empty!'
    
    headers=['Device','Host','Attribute','Type','ArchivingMode','Periode','MinRange','MaxRange']
    trace('Searching headers ...')
    head=config.get(0)
  
    def checkHeaders():
        if not all(h in ''.join(head) for h in headers):
            print 'WRONG FILE HEADERS!'
            exit()
    
    for h in head:
      if not h: 
        trace('In ParseCSV, empty headers!?: %s'%head)
        break
      else:
        trace('In ParseCSV(...): fill column "%s"'%h)
        config.fill(head=h)
    config.setOffset(1)
  
    trace('Getting attributes from the file ...')
    # it returns the list of device names and the lines that matches for each
    hosts=config.getAsTree(lastbranch='ArchivingMode')#config.get(head='Device',distinct=True)
    if not hosts: 
        print 'NO HOSTS FOUND IN %s!!!' % filename
        return {}
    
    ## Parsing the params to create a Context
    #-------------------------------------------------
    defaultparams = {'@LABEL':'User_Code-0X','@AUTHOR':'Who?','@DATE':'When?','@DESCRIPTION':'What?',}#'@REASON':'Why?'} ## Reason became deprecated
    transparams = {'@LABEL':'name','@AUTHOR':'author','@DATE':'time','@DESCRIPTION':'description',}#'@REASON':'reason'} ## Reason became deprecated
    for p,v in defaultparams.items():
        if not fun.inCl(p,hosts): raise Exception('PARAMS_ERROR','%s NOT FOUND'%p) 
        elif not hosts[p]:  raise Exception('PARAMS_ERROR','%s IS EMPTY'%p) 
        elif hosts[p].keys()[0]==v: raise Exception('PARAMS_ERROR','%s NOT INITIALIZED (%s)'%(p,v)) 
        defaultparams[p]=hosts.pop(p).keys()[0]
        context[p]=defaultparams[p]
        if p=='@DATE':
            t,time_fmts = None,['%Y-%m-%d', '%Y-%m-%d %H:%M', '%y-%m-%d', '%y-%m-%d %H:%M', 
                                '%Y/%m/%d','%Y/%m/%d %H:%M','%y/%m/%d','%y/%m/%d %H:%M',
                                '%d-%m-%Y' ,'%d-%m-%Y %H:%M' ,'%d-%m-%y' ,'%d-%m-%y %H:%M' ,
                                '%d/%m/%Y','%d/%m/%Y %H:%M','%d/%m/%y','%d/%m/%y %H:%M',
                                '%m-%d-%Y' ,'%m-%d-%Y %H:%M','%m-%d-%y' ,'%m-%d-%y %H:%M',
                                '%m/%d/%Y','%m/%d/%Y %H:%M','%m/%d/%y','%m/%d/%y %H:%M',
                                ]
            for tf in time_fmts:
                try:
                    #print 'trying format %s'%str(tf)
                    t = time.strftime('%Y-%m-%d',time.strptime(context[p],tf))
                    break
                except: pass
            if t is not None: context[transparams[p]]=t
            else: raise Exception('PARAMS_ERROR','@DATE format cannot be parsed!: %s'%str(context[p]))

    ##Reading the archiving modes
    #-------------------------------------------------
    attrslist = {}
    archmodes={'PERIODIC':'MODE_P','ABSOLUTE':'MODE_A','RELATIVE':'MODE_R','THRESHOLD':'MODE_T','CALC':'MODE_C','EXTERNAL':'MODE_E'}
    all_devs = fandango.CaselessDict() #{}
    [all_devs.update(devs) for devs in hosts.values()] #Used for @COPY tag
    host = ''
    pops = []
    for khost,devs in sorted(hosts.items()):
        #print 'khost,devs: %s,%s'%(khost,devs)
        for dev,attributes in sorted(devs.items()):
            #print 'dev,attributes: %s,%s'%(dev,attributes)
            #print 'reading device %s:%s'%(dev,str(attributes))
            '''Doing all the checks needed before adding any attribute'''
            dev = dev.lower()
            if dev.strip() == 'device': continue
            elif '/' not in dev:
                dev,alias = utils.translate_attribute_alias(dev).rsplit('/',1)[0],dev
                all_devs[dev] = all_devs[alias]
            if khost.upper() == '@HOST': 
                try: host = utils.get_device_host(dev).split('.')[0]
                except Exception,e: 
                    print e
                    host = ''
            elif khost!=host:
                trace('Getting devices from host %s'%khost)
                host = khost
            if not host: 
                trace('Unable to get host for %s!!!' % dev)
            
            for a in [t.strip() for t in attributes if fun.inCl('@COPY',t)]: #COPYing attributes from another device
                    if ':' in a: 
                        dev2 = a.split(':')[1].strip()
                    else:
                        raise Exception('COPY macro must be declared in the way @COPY:a/tango/device')
                    if dev2:
                        if dev2 not in all_devs: 
                            raise Exception('AttributesNotDefinedFor:%s'%dev2)
                        #print '%s: copying attributes from %s: %s'%(dev,dev2,all_devs[dev2])
                        [attributes.__setitem__(k,v) for k,v in all_devs[dev2].items() if k and k not in attributes]
                        attributes.pop(a)
            
            if any('@DELETE' in a or '@STOP' in a for a in attributes): #If a @DELETE or @STOP is found as single attribute all dev. attributes are stopped
                deletelist.append(dev)
            
            DEFAULT_MODE = {'MODE_P':[300000]} ##seconds in CSV's are converted to milliseconds
            DEFAULT_CONFIG = defaultdict(lambda:dict(DEFAULT_MODE.items()))
            # Getting attributes with @DEFAULT clause
            for a,tipus in attributes.items():
                #print 'a,tipus:  %s,%s'%(a,tipus)
                if '@DEFAULT' not in a: continue
                if not tipus or not tipus.values()[0]: 
                    print 'Wrong format assigning defaults for %s device' % dev
                    continue
                for schema,modes in tipus.items():
                    trace('schema,modes: %s,%s'%(schema,modes))
                    for mode,params in modes.items():
                        trace('mode,params:  %s,%s'%(mode,params))
                        mode =  archmodes.get(mode.upper(),mode)
                        DEFAULT_CONFIG[schema.upper()][mode] = [float(p) for p in params if p]
            if 'HDB' not in DEFAULT_CONFIG: DEFAULT_CONFIG['HDB'] = DEFAULT_CONFIG['']
            if 'TDB' not in DEFAULT_CONFIG: DEFAULT_CONFIG['TDB'] = DEFAULT_CONFIG['']
            if DEFAULT_CONFIG: trace('DEFAULT_CONFIG for %s archiving is: %s' % (dev,DEFAULT_CONFIG.items()))
        
            for attribute,modes in sorted(attributes.items()):
                #print 'attribute,modes:  %s,%s'%(attribute,modes)
                if '@DEFAULT' in attribute: continue
                attribute = attribute.lower()
                
                # applying filters to obtained attributes
                if filters or exclude:
                    if ( ('name' in filters and not fun.matchCl(fun.toRegexp(filters['name']),dev+'/'+attribute)) or
                        ('name' in exclude and fun.matchCl(fun.toRegexp(exclude['name']),dev+'/'+attribute)) ):
                        #print '%s filtered by %s'%(attribute,filters)
                        continue
                
                if not modes and DEFAULT_CONFIG: #No schema or modes defined
                    trace('\treading attribute %s: using default modes %s'%(attribute,str(DEFAULT_CONFIG)))
                    #attrslist[dev+'/'+attribute]={'host':host,'type':tipus,'modes':config}
                    attrslist[dev+'/'+attribute]=dict([('host',host)]+list(DEFAULT_CONFIG.items()))
                else:
                    attrslist[dev+'/'+attribute]={'host':host}
                    for mode,mode_params in modes.items():
                        #print '%s,%s'%(mode,mode_params)
                        tipus=mode.upper()
                        if not mode_params: #Schema defined but not modes
                            config = DEFAULT_CONFIG.get(tipus,DEFAULT_CONFIG.get('',{})).copy()
                            if config:
                                new_config = PyTangoArchiving.ArchivingAPI.check_modes(tipus,config)
                                if new_config!=config: 
                                    #print('Modes corrected from %s to %s'%(sorted(config.items()),sorted(new_config.items())))
                                    config = new_config
                                trace('\treading attribute %s: using %s default modes %s'%(attribute,tipus,config))
                                attrslist[dev+'/'+attribute]={'host':host,tipus:config}
                            else:
                                trace('\treading attribute %s: no %s default mode declared'%(attribute,tipus))
                        else: #Both schema and modes defined
                            config = DEFAULT_CONFIG.get(tipus,DEFAULT_MODE).copy()
                            #And modes is overriden by its own member
                            #modes=modes.values()[0]
                            modes = mode_params #{'relative': ['15', '1', '1']}
                            try:
                                firstmode,firstparam=modes.keys()[0],mode_params.values()[0][0]
                            except Exception,e:
                                print attribute,modes,mode,mode_params
                                raise e
                            if any(a.startswith('@') for a in [attribute,firstmode,tipus]):
                                    if attribute=='@DEDICATED':
                                        dedicated[dev]=tipus
                                    elif any(c in ['@STOP','@DELETE'] for c in [firstmode,tipus]):
                                        deletelist.append(dev+'/'+attribute)
                            else:
                                    #Adding always a default value to the list of modes.
                                    #if 'MODE_P' not in config: config.update(DEFAULT_MODE.items())
                                    for mode,params in mode_params.items():
                                        if not mode: continue
                                        #print '\treading mode %s:%s'%(mode,str(params))
                                        mode=mode.upper() #MODE_P,MODE_R,MODE_A,...
                                        if mode in archmodes:
                                                mode=archmodes[mode]
                                        elif mode not in archmodes.values():
                                                print 'Unknown mode!: ',mode
                                                continue
                                        params = [float(p) for p in params if p]
                                        if params: params[0] = 1000.*params[0] #Converting periods from seconds to milliseconds
                                        config[mode]=params
                                    #attrslist[dev+'/'+attribute]={'host':host,'type':tipus,'modes':config}
                                    new_config = PyTangoArchiving.ArchivingAPI.check_modes(tipus,config)
                                    if new_config!=config: 
                                        trace('Modes corrected from %s to %s'%(sorted(config.items()),sorted(new_config.items())))
                                        config = new_config
                                    attrslist[dev+'/'+attribute][tipus.upper()]=config
                #print '\t\tattrslist[%s/%s]=%s'%(dev,attribute,attrslist[dev+'/'+attribute])
                                    
    trace('applying filters (%s / %s) to obtained attributes'%(filters,exclude))
    if filters or exclude:
        def_keys = ('host',) #Keys unfilterable
        keys = attrslist.keys()
        pops = []
        for attribute in keys:
            if ( ('name' in filters and not fun.matchCl(fun.toRegexp(filters['name']),attribute)) or
                 ('name' in exclude and fun.matchCl(fun.toRegexp(exclude['name']),attribute)) ):
                print '%s filtered by %s'%(attribute,filters)
                pops.append(attribute)
            else:
                modes = attrslist[attribute].keys()
                #print 'filter check: attribute,modes: %s,%s'%(attribute,modes)
                for mode in modes:
                    if mode in def_keys: continue
                    if ( ('type' in filters and not fun.matchCl(filters['type'],mode)) or
                         (mode in filters and not fun.matchCl(filters[mode],str(modes[mode]))) ):
                            #print '%s/%s filtered by filters %s'%(attribute,mode,filters)
                            attrslist[attribute].pop(mode,None)
                    if ( ('type' in exclude and fun.matchCl(exclude['type'],mode)) or
                         (mode in exclude and fun.matchCl(filters[mode],str(modes[mode]))) ):
                            #print '%s/%s excluded by filters %s'%(attribute,mode,exclude)
                            attrslist[attribute].pop(mode,None)
                if not [k for k in attrslist[attribute] if k not in def_keys]:
                    trace('%s filtered by %s'%(attribute,filters))
                    pops.append(attribute)
        trace('%d attributes filtered out by %s/%s'%(len(pops),filters,exclude))
        for attribute in pops:
            attrslist.pop(attribute)
            if attribute in dedicated: dedicated.pop(attribute)
            if attribute in deletelist: deletelist.remove(attribute)
                    
    
    trace('Specified %d attributes from %d hosts'%(len(attrslist),len(hosts)))
    if dedicated: trace('%d devices are dedicated'%(len(dedicated)))
    if deletelist: trace('%d attributes to delete'%(len(deletelist)))
    RESULT = attrslist
    return RESULT
    
#END OF ParseCSV
#############################################################################################################            
# Import/export from mambo

HDBModes={'MODE_P':4,'MODE_R':5,'MODE_A':0,'MODE_D':2,'MODE_T':6}

def export2csv(attrslist,filename):
    raise Exception,'NotImplemented'

def attrs2ac(attrslist,acname):
    '''Converts and attribute list in an AC (mambo xml) file
    attrslist: list of attributes/archiving_modes to be added in the file
    acname: XMLDocument where the configuration is going to be stored
    '''
    impl = minidom.getDOMImplementation()
    newdoc = impl.createDocument(None,'archivingConfiguration',None)
    HDBModes={'MODE_P':4,'MODE_R':5,'MODE_A':0,'MODE_D':2,'MODE_T':6}

    def createAttributeXMLNode(xmldoc,attrname,archmodes,archtype='hdb'):
        '''Creates the appropiated XML structure for an AC file
        xmldoc: xml implemented document, used to generate the nodes
        attrname: name of the attribute
        archtype: HDB or TDB
        modes: dictionary containing the archiving modes and arguments of each one
        result: the AttributeNode element created
        '''
        att=xmldoc.createElement('attribute')
        att.setAttribute('completeName',attrname)
        modes=xmldoc.createElement('%sModes'%archtype.upper())
        modes.setAttribute('dedicatedArchiver','')
        if archtype.lower()=='tdb': modes.setAttribute('exportPeriod',"1800000")
        modes.appendChild(xmldoc.createTextNode('\n'))
        for key,value in archmodes.items():
            if key not in HDBModes: continue
            mode=xmldoc.createElement('mode')
            if key in ['MODE_P','periodic']:
                mode.setAttribute('type',str(HDBModes['MODE_P']))
                mode.setAttribute('period',str(value[0]))
            elif key in ['MODE_R','relative']:
                mode.setAttribute('type',str(HDBModes['MODE_R']))
                mode.setAttribute('period',str(value[0]))
                mode.setAttribute('percent_sup',str(value[2]))
                mode.setAttribute('percent_inf',str(value[1]))
            elif key in ['MODE_A','absolute']:
                mode.setAttribute('type',str(HDBModes['MODE_A']))
                mode.setAttribute('period',str(value[0]))
                mode.setAttribute('val_sup',str(value[2]))
                mode.setAttribute('val_inf',str(value[1]))
            elif key in ['MODE_D','differential']:
                mode.setAttribute('type',str(HDBModes['MODE_D']))
                mode.setAttribute('period',str(value[0]))
            elif key in ['MODE_T','threshold']:
                mode.setAttribute('type',str(HDBModes['MODE_T']))
                mode.setAttribute('period',str(value[0]))
                mode.setAttribute('threshold_sup',str(value[2]))
                mode.setAttribute('threshold_inf',str(value[1]))
            modes.appendChild(mode)
            modes.appendChild(xmldoc.createTextNode('\n'))
        modes.appendChild(xmldoc.createTextNode('\n'))
        
        att.appendChild(xmldoc.createTextNode('\n'))
        att.appendChild(modes)
        att.appendChild(xmldoc.createTextNode('\n'))
        return att

    newdoc.version=u'1.0'
    newdoc.encoding=u'ISO-8859-1'
    args={  u'creationDate': time.strftime('%Y-%m-%d %H:%M:%S.000'),
        u'isHistoric': u'true',
        u'isModified': u'false',
        u'lastUpdateDate': time.strftime('%Y-%m-%d %H:%M:%S.000'),
        u'name': acname,
        u'path': os.path.abspath('')+'/'+acname+'.ac'}
    for k,v in args.items(): newdoc.firstChild.setAttribute(k,v)
    
    fc=newdoc.firstChild
    fc.appendChild(newdoc.createTextNode('\n'))
    for attribute,modes in attrslist.items():
        fc.appendChild(createAttributeNode(newdoc,attribute,modes))
        fc.appendChild(newdoc.createTextNode('\n'))
        
    print newdoc.toxml()
    return newdoc

def readArchivingConfigurationFromDB(htype='hdb'):
    ''' Using the ArchivingAPI this function reads the Status of each attribute from the amt table of the database and returns a dictionary with the actual configurations
    The structure of the dictionary is {start_date_epoch:{attrname:[archivingmodes],...},...}
    '''
    pass            

####################################################################################
# Other file utilities

def parse_raw_file(filename,section='',is_key=fandango.tango.parse_tango_model):
    """
    Method used to parse "freely" defined files with very poor syntax on them.
    I don't use CSVArray to avoid comments to force format on the rest of the file.
    """
    rows = open(filename).readlines() if not fandango.isSequence(filename) else filename
    comment = '//' if any(r.startswith('//') for r in rows) else '#'
    if section:
        #Assuming a file separated in subsections
        values = {}
        while rows:
            r = rows.pop(0)
            rv = []
            if r.startswith(section):
                while rows and not rows[0].startswith(section):
                    rv.append(rows.pop(0))
            if rv:
                values[r.replace(section,'').strip()] = parse_raw_file(rv)
    else:
        #Assuming a plain rows/columns file
        rows = filter(bool,(r.split(comment)[0].strip() for r in rows))
        sep = '\t' if all('\t' in r for r in rows) else ',' if all(',' in l for l in rows) else ' '
        header = 'column' if all('/' in r for r in rows) else 'row0'
        split_row = lambda r: map(str.strip,r.split(sep))
        if header == 'row0':
            values = defaultdict(list)
            row0 = rows.pop(0).split(sep)
            for r in rows:
                for i,v in enumerate(split_row(r)):
                    values[row0[i]].append(fandango.str2type(v))
        else:
            # I assume that at least 1 column will contain tango names, and other will contain non-string values
            weights = defaultdict(int)
            for r in map(split_row,rows)[:10]:
                for i,v in enumerate(r):
                    if is_key(v): #String to be used as key
                        weights[i]-=1
                    elif type(fandango.str2type(v))!=str: #A raw value
                        weights[i]+=1
                if len(r)==2: break
            index = [t[-1] for t in sorted((v,i) for i,v in weights.items())]
            values = dict((r[index[0]],fandango.str2type(r[index[-1]])) for r in map(split_row,rows))
    return values
        
####################################################################################

def exxit():
    print('Usage:\n\tPyTangoArchiving/files.py check/load/start/stop '
                'filename/attribute [schema]\n')
    sys.exit(-1)
    
def main(args):
    #@todo: this script features can be part of ctarchiving script
    if not args: 
        exxit()
        
    action = args[0]
    filenames = args[1:]
    if len(args)>2:
        schema = filenames.pop(-1)
    else:
        schema = raw_input('Schema?').strip() or ''
    
    if all(map(os.path.isfile,filenames)):
    
        if action in ('load','check'):
            attrs = ParseCSV(filename,schema,log=False)
            if action in 'check' and schema: 
                from PyTangoArchiving import ArchivingAPI
                api = ArchivingAPI(schema)
                for a in sorted(attrs):
                    t = ArchivingAPI(schema).get(a)
                    print((a,t,t and api.load_last_values(a)))
                
        if action in ('load',):
            LoadArchivingConfiguration(filename,schema,launch=True,
                                       force='force' in args)
            
    else:
        import fandango.tango as ft
        if schema and all(map(ft.parse_tango_model,filenames)):
            api = PyTangoArchiving.ArchivingAPI(schema)

            if 'start' in action.lower():
                modes = eval(raw_input('Archiving modes?'))
                api.start_archiving(filenames,modes)

            if 'stop' in action.lower():
                api.stop_archiving(filenames)
                
            if 'check' in action.lower():
                print(api.load_last_values(filenames))
        else:
            print('Unknown args: %s' % filenames)
            exxit()
        

if __name__ == '__main__':
    print sys.argv
    main(sys.argv[1:])
