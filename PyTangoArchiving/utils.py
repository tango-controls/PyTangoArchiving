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
utilities for managing tango and mysql datatypes within PyTangoArchiving
"""


import time,datetime,os,re,traceback,xml,sys,functools
from random import randrange
import MySQLdb

TRACE = False

import fandango
from fandango.db import FriendlyDB
import fandango.functional as fun
import fandango.linos as linos
from fandango.functional import date2time,date2str,mysql2time,ctime2time,\
    time2str,isNumber,clmatch,isCallable
from fandango.functional import reldiff,absdiff,seqdiff
from fandango.arrays import decimate_custom,decimate_array

try:
    import PyTango
    from fandango.servers import ServersDict
    from fandango.device import get_matching_attributes,check_device,check_attribute,get_distinct_devices
    from fandango.device import get_distinct_domains,get_distinct_members, \
            get_distinct_families,get_distinct_attributes
    from fandango.device import get_device_host
    from fandango.tango import parse_tango_model,cast_tango_type
except:
    PyTango=ServersDict=check_attribute=None
    
class CatchedAndLogged(fandango.objects.Decorator):
    """
    based on fandango.objects.Cached
    in the future it should replace Catched decorator
    """
    def __init__(self,target=None,log=True,throw=False,default=None):
        self.log = log
        self.throw = throw
        self.default = default
        self.decorate(target)
          
    def __call__(self,*args,**kwargs):
        """
        This method will either decorate a method (with args) or execute it
        """
        if self.f is None: # Deferred decorator
            self.decorate(args[0])
            return self
        else: # Instantiated decorator
            return self.execute(*args,**kwargs)
          
    def _log(self,msg):
        if isCallable(self.log): self.log(msg) 
        elif self.log: print(msg)
          
    def decorate(self,target):
        if isCallable(target):
            self.f = target
            self.f_name = target.__name__
            #self.call = wraps(self.f)(self.__call__) #Not for methods!!
            functools.update_wrapper(self,self.f)
        else:
            self.f = None
            
    def execute(self,*args,**kwargs):
        try:
           return self.f(*args,**kwargs)
        except:
            self._log('%s(*%s,**%s) failed!'%(self.f_name,args,kwargs))
            self._log(traceback.format_exc())
            if self.throw: raise
        return self.default
        
    def __get__(self,obj,objtype=None):
        """
        This bounding method will be called only when decorating an
        instance method
        """
        from types import MethodType
        return MethodType(self,obj,objtype)        
        
        

###############################################################################
# Numpy based methods for decimation/filtering
            
def to_array(l):
    """
    returns l as a numpy array; replacing None values by NaN
    """
    import numpy as np
    return np.array(l,dtype=float)

def get_col(array,col):
    #This method allows the next methods to use 1D or 2D arrays
    if len(array.shape)==1:
        return array
    else:
        return array[:,col]
            
def sort_array(arg0,arg1=None,decimate=True,as_index=False):
    """
    Args can be an (N,2) array or a tuple with 2 (times,values) arrays
    Takes two arrays of times and values of the same length and sorts the (time,value) 
    The decimate argument just removes repeated timestamps, not values
    """
    import numpy as np
    t0=time.time()
    #times = np.random.random_integers(N,size=(N,))
    #values = np.random.random_integers(3000,4000,size=(N,))
    data = arg0 if arg1 is None else (arg0,arg1)
    if len(data)==2:
        times,values = data
        data = np.array((times,values)).T #Build a new array for sorting
    #Sort the array by row index (much faster than numpy.sort(order))
    time_index = get_col(np.argsort(data,0),0)
    if as_index:
        if not decimate:
            return index
        else:
            return np.compress(get_array_steps(get_col(data,0).take(time_index)),time_index,0)
    else:
        sdata = data.take(time_index,0)
        if decimate:
            sdata = np.compress(get_array_steps(get_col(sdata,0)),sdata,0)
        print time.time()-t0
        return sdata
    
def get_array_steps(array,minstep=0.001,as_index=False):
    #It calcullates steps and adds True at the beginning
    import numpy as np
    diff = np.insert(np.abs(array[1:]-array[:-1])>minstep,0,True)
    if not as_index: return diff
    else: return diff.nonzero()[0]
    
def get_bigger_step(array,minstep=0.001,as_index=False):
    import numpy as np
    diff = np.insert(np.abs(array[1:]-array[:-1]),0,minstep)
    idiff = np.argsort(diff)
    maxgap = diff[idiff[-1]]
    if maxgap<minstep: 
        return None
    else: 
        return idiff[-1] if as_index else maxgap
    
def interpolate_array(array,mint=None,maxt=None,step=None,nsteps=None):
    import numpy as np
    #dtype = array.dtype
    if None in (mint,maxt): mint,maxt = np.min(get_col(array,0)),np.max(get_col(array,0))
    if step is None: step  = float(maxt-mint)/nsteps
    xs = np.arange(mint,maxt+step,step)
    ys = np.interp(xs,get_col(array,0),get_col(array,1))
    return np.array((xs,ys)).T
    
#def decimate_array(array,step=None,nsteps=None,diff=None,interpolate=False):
    #"""
    # DEPRECATED!!! Use fandango.arrays.decimate_array instead!
    #It decimates an array taking into account values in the first column
    #"""
    #import numpy as np
    #if not isinstance(array,np.ndarray):
        #array = np.array(array)
    #if interpolate:
        #return interpolate_array(array,step=step,nsteps=nsteps)
    #if any((step,nsteps)):
        #mint,maxt = np.min(get_col(array,0)),np.max(get_col(array,0))
        #if step is None: step  = float(maxt-mint)/nsteps
        #tround = np.array(get_col(array,0)/step,dtype='int')
        #array = array[get_array_steps(tround)]
    #elif diff is None:
        #diff = 0
    #if diff is not None:
        #if diff is True: diff = 0
        #vs = array[:,1] #[:,1]
        #nans = get_array_steps(np.isnan(vs))
        #try:
            ### But this step crashes with nans!!
            #diff1 = np.insert(np.abs(vs[:-1]-vs[1:])>diff,0,True) #Different from next value
            #diff2 = np.append(np.abs(vs[1:]-vs[:-1])>diff,True) #Different from previous value
        #except TypeError,e:
            #diff1 = np.insert(np.abs(vs[:-1]!=vs[1:]),0,True)
            #diff2 = np.append(np.abs(vs[1:]!=vs[:-1]),True)
        ##print 'diff1,diff2,nans: %d,%d,%d'%tuple(len(np.nonzero(a)[0]) for a in (diff1,diff2,nans))
        #array = array[diff1|diff2|nans]
    #return array
        
def patch_booleans(history,trace=TRACE):
    if trace: print 'In patch_booleans(%d,%s)'%(len(history),history and history[0] or '')
    fromHistoryBuffer = history is not None and len(history) and hasattr(history[0],'time')
    patch = 0
    for h in history: 
        v = h[1] if not fromHistoryBuffer else h.value
        if v is None: continue
        if isinstance(v,int) or isinstance(v,float): break
        if isinstance(v,basestring) and (not isNumber(v) and v.lower() not in ('true','false','none')): break
        if isinstance(v,basestring):
            if isNumber(v) and v in ('1','0','1.0','0.0'): patch=1
            if v.lower() in ('true','false','none'): patch=2
            break
        if isinstance(v,bool):
            patch = 1
            break
    if patch:
        #print '\tpatching ...'
        for i,h in enumerate(history):
            v = h.value if fromHistoryBuffer else h[1]
            if patch==2: v = v.lower().strip() in ('true','1')
            if fromHistoryBuffer: h.value = int(v)
            else: history[i] = (h[0],int(v))
    return history

###############################################################################
# Reporting

def get_attributes_as_event_list(attributes,start_date=None,stop_date=None,formula=None):
    """
    This method returns attribute changes ordered by time (event_list format)
    Attributes can be passed as a list or as a formula (TangoEval) or both. 
    If a formula is available the evaluated value will be added at each row of the list.
    """
    from PyTangoArchiving import Reader
    from fandango import isSequence,isString,TangoEval
    rd = Reader()
    te = fandango.TangoEval()

    if isString(attributes) and formula is None:
        try:
            formula = attributes
            attributes = sorted(set('%s/%s'%t[:2] for t in te.parse_variables(formula)))
            if len(attributes)==1: formula = None
        except:
            formula,attributes = None,[]

    if isSequence(attributes):
        assert start_date, 'start_date argument is missing!'
        attributes = rd.get_attributes_values(attributes,start_date,stop_date)
    
    avals = dict((k,decimate_array(v)) for k,v in attributes.items())
    buffer = sorted((v[0],k,v[1]) for k,l in avals.items() for i,v in enumerate(l) if not i or v[1]!=l[i-1][1])
    
    if formula is not None:
        cache,parsed = {},te.parse_formula(formula)
        for i,event in enumerate(buffer):
            cache[event[1]] = event[2]
            f = te.eval(parsed,cache) if all(k in cache for k in attributes) else None
            buffer[i] = (event[0],event[1],event[2],f)
            
    return buffer

###############################################################################
# DB Methods

SCHEMAS = ('hdb','tdb','snap')

from fandango import time2date,str2time
    
def decimate_db_table(db,table,host='',user='',passwd='',start=0,end=0,period=300,iteration=1000,condition='',cols=None,us=True,test=False, repeated = False):
    """ 
    This method will remove all values from a MySQL table that seem duplicated 
    in time or value.
    All values with a difference in time lower than period will be kept.
    
    To use it with hdb++:
    
    decimate_db_table('hdbpp',user='...',passwd='...',
      table = 'att_scalar_devdouble_ro',
      start = 0,
      end = now()-600*86400,
      period = 60, #Keep a value every 60s
      condition = 'att_conf_id = XX',
      iteration = 1000,
      columns = ['data_time','value_r'],
      us=True,
      )
    """
    print('Decimating all repeated values in %s(%s) with less '
      'than %d seconds in between.'%(table,condition,period))
    
    db = FriendlyDB(db,host,user,passwd) if not isinstance(db,FriendlyDB) else db
    #rw = 'write_value' in ','.join([l[0] for l in db.Query("describe %s"%table)]).lower()
    #date,column = 'read_value,write_value' if rw else 'value'
    columns = cols or ['time','value']
    date,column = columns[0],columns[1:]
    start = time2date(start) if isNumber(start) else time2date(str2time(start))
    t0,vw0,now = start,None,time2date(time.time())
    end = time2date(end) if isNumber(end) else time2date(str2time(end))
    removed,pool,reps = 0,[],[]
    count = 0
    
    ## WHY T0 AND END ARE DATES!?!? : to be easy to compare against read values

    while t0<(end or now):

        query = "select %s,%s from %s where" %(date,','.join(column),table)
        query += " '%s' < %s"%(date2str(t0,us=True),date)#,date2str(end))
        if condition: query+=' and %s'%condition
        query += ' order by %s'%date
        query += ' limit %d'%iteration
        values = db.Query(query)
        #print(query+': %d'%len(values))
        #print('inspecting %d values between %s and %s'%(len(values),date2str(t0),date2str(end)))
        
        if not values: 
            break
          
        for i,v in enumerate(values):
            count += 1
            t1,vw1 = v[0],v[1:1+len(column)] #v[1],(rw and v[2] or None)
            #print((i,count,t1,vw0,vw1))
            e0,e1 = 1e-3*int(1e3*date2time(t0)),1e-3*int(1e3*date2time(t1)) #millisecs
            tdelta = e1-e0
            is_last = i >= (len(values)-1) or t1 >= end
            buff = len(pool)

            if is_last or tdelta>=period or vw0!=vw1:
                #if tdelta>=period: print('%s >= %s'%(tdelta,period))
                #elif vw0!=vw1: print('%s != %s'%(vw0,vw1))
                #else: print('i = %s/%s'%(i,len(values)))
                # End of repeated values, apply decimation ...
                if buff:
                    # Dont apply remove on windows < 1 second
                    e1 = date2time(values[i-1][0]) #previous value
                    if True: #(int(e1)-int(e0))>1:
                        #print('remove %d values in pool'%len(pool))
                        if not test:
                            #Don't use the between syntax!!
                            q = "delete from %s where "%table
                            if condition:
                                q+= condition+' and '
                            #e0,e1 = e0+1,e1-1 #t0 should not be removed!
                            q+= "%s > '%s' and "%(date,time2str(e0,us=us)) 
                            q+= "%s < '%s'"%(date,time2str(e1,us=us))
                            #print(q)
                            #removed += buff
                            db.Query(q)

                        #print('t0: %s; removed %d values' % (date2str(t0),buff-1))
                        #print('pool:%s'%str(pool))
                        
                if reps:
                    if not test:
                        #print('repeated timestamp: %s,%s == %s,%s'%(t0,vw0,t1,vw1))
                        q = "delete from %s where "%(table)
                        if condition:
                            q+= condition+' and '
                        q+= "%s = '%s' limit %d" % (
                          date,date2str(reps[-1],us=us),len(reps))
                        #print(q)
                        db.Query(q)                
 
                pool,reps = [],[]
                #print('%s => %s'%(t0,t1))
                t0,vw0 = t1,vw1

            else:
                # repeated values with tdiff<period will be removed in a single query
                    
                # This should apply only if values are different and timestamp equal?
                # if timestamp is repeated the condition t < d < t is useless
                # repeated timestamps are removed directly
                #print(tdelta)
                if repeated and not tdelta:
                    reps.append(t1)
                    #print(('reps',t1))
                        
                elif vw0 == vw1:
                    #if buff and not buff%100:
                    #    print('%s repeated values in %s seconds'%(buff,tdelta))
                    pool.append(t1)

                    #removed +=1  
                
                else: pass
                #print((vw0,vw1))                  
                    
            if is_last: break
    
    query = "select count(*) from %s where" %(table)
    query += " '%s' < %s and %s < '%s'"%(date2str(start,us=us),date,date,date2str(end,us=us))
    if condition: query+=' and %s'%condition   
    cur =  db.Query(query)[0][0]
    removed = count-cur

    print('decimate_db_table(%s,%s) took %d seconds to remove %d = %d - %d values'%(
      table,condition,time.time()-date2time(now),removed,count,cur))

    return removed

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
                    
###############################################################################

def repair_attribute_name(attr):
    """
    Remove "weird" characters from attribute names
    """
    import re
    return re.sub('[^a-zA-Z-_\/0-9\*]','',attr)

def translate_attribute_alias(attribute):
    if ':' in attribute.split('/')[0]: 
        attribute = attribute.split('/',1)[-1] #removing tango_host
    if attribute.count('/') in (0,1):
        dev = fandango.get_device_for_alias(attribute.split('/')[0]) or ''
        attribute = dev if '/' not in attribute else dev+'/'+attribute.split('/')[1]
    if attribute.count('/') == 2: 
        if any(attribute.lower().startswith(s+'/') for s in ('ioregister','pc','expchan',)):
            attribute+='/value'
        elif any(attribute.lower().startswith(s+'/') for s in ('motor','mg','pm',)):
            attribute+='/position'
        else:
            attribute+='/state'
    return attribute
            
def get_table_name(ID):
    ID = int(ID)
    return 'att_%05d'%ID if ID<10000 else 'att_%06d'%ID

def get_table_updates(api='hdb'):
    import PyTangoArchiving
    if fun.isString(api): 
        api = PyTangoArchiving.ArchivingAPI(api)
    if isinstance(api,PyTangoArchiving.ArchivingAPI):
        db = api.db
    if isinstance(api,fandango.db.FriendlyDB):
        db = api
    updates = db.Query('select table_name,update_time from information_schema.tables where table_schema like "%s"'%api.schema)
    updates = dict((a,fun.date2time(t) if t else 0) for a,t in updates)    
    return updates
  
def check_archiving_performance(schema='hdb',attributes=[],period=24*3600*90,\
    exclude=['*/waveid','*/wavename','*/elotech-*'],action=False,trace=True):
    import PyTangoArchiving as pta
    import fandango as fn

    ti = fn.now()
    api = pta.api(schema)
    check = dict()
    period = 24*3600*period if period < 1000 else (24*period if period<3600 else period)
    attributes = fn.get_matching_attributes(attributes) if fn.isString(attributes) else map(str.lower,attributes)
    tattrs = [a for a in api if not attributes or a in attributes]
    excluded = [a for a in tattrs if any(fn.clmatch(e,a) for e in exclude)]
    tattrs = [a for a in tattrs if a not in excluded]

    #Getting Tango devices currently not running
    alldevs = set(t.rsplit('/',1)[0] for t in tattrs if api[t].archiver)
    tdevs = filter(fn.check_device,alldevs)
    nodevs = [d for d in alldevs if d not in tdevs]

    #Updating data from archiving config tables
    if not attributes:
      tattrs = sorted(a for a in api if a.rsplit('/',1)[0] in tdevs)
      tattrs = [a for a in tattrs if not any(fn.clmatch(e,a) for e in exclude)]
    print('%d attributes will not be checked (excluded or device not running)'%(len(api)-len(tattrs)))
    
    tarch = sorted(a for a in api if api[a].archiver)
    tnoread = sorted(t for t in tarch if t not in tattrs)
    check.update((t,None) for t in tnoread)

    #Getting attributes archived in the past and not currently active
    tmiss = [t for t in tattrs if not api[t].archiver]
    check.update((t,fn.check_attribute(t,readable=True)) for t in tmiss)
    tmiss = [t for t in tmiss if check[t]]
    tmarray = [t for t in tmiss if fn.isString(check[t].value) or fn.isSequence(check[t].value)]
    tmscalar = [t for t in tmiss if t not in tmarray]
    
    #Getting updated tables from database
    tups = pta.utils.get_table_updates(schema)
    # Some tables do not update MySQL index tables
    t0 = [a for a in tarch if a in tattrs and not tups[api[a].table]]
    check.update((t,check_attribute(a,readable=True)) for t in t0 if not check.get(t))
    t0 = [t for t in t0 if check[t]]
    print('%d/%d archived attributes have indexes not updated ...'%(len(t0),len(tarch)))
    if t0 and len(t0)<100: 
      vs = api.load_last_values(t0);
      tups.update((api[t].table,api[t].last_date) for t in t0)
    tnotup = [a for a in tarch if tups[api[a].table]<fn.now()-1800]
    check.update((t,1) for t in tarch if t not in tnotup)
    
    #Updating readable attributes (all updated are considered as readable)
    tread = sorted(t for t in tattrs if t not in tnoread)
    for t in tattrs:
      if t not in check:
        check[t] = fn.check_attribute(t,readable=True)
    tread = sorted(t for t in tattrs if check[t])
    tnoread.extend(t for t in tread if not check[t])
    tnoread = sorted(set(tnoread))
          
    #tread contains all readable attributes from devices with some attribute archived
    #tnoread contains all unreadable attributes from already archived

    #Calcullating all final stats
    #tok will be all archivable attributes that are archived
    #tnotup = [a for a in tnotup if check[a]]
    #tok = [t for t in tread if t in tarch and t not in tnotup]
    tok = [t for t in tarch if t not in tnotup]
    readarch = [a for a in tread if a in tarch]
    treadnotup = [t for t in readarch if t in tnotup] #tnotup contains only data from tarch
    tokread = [t for t in readarch if t not in tnotup] #Useless, all archived are considered readable
    tarray = [t for t in tarch if check[t] and get_attribute_pytype(t) in (str,list)]
    removed = [a for a in tattrs if not api[a].archiver and tups[api[a].table]>fn.now()-period]
    
    result = fn.Struct()
    result.Excluded = excluded
    result.Schema = schema
    result.All = api.keys()
    result.Archived = tarch
    result.Readable = tread
    result.ArchivedAndReadable = readarch
    result.Updated = tok #tokread
    result.Lost = treadnotup
    result.Removed = removed
    result.TableUpdates = tups
    result.NotUpdated = tnotup
    result.Missing = tmiss
    result.MissingScalars = tmscalar
    result.MissingArrays = tmarray
    result.ArchivedArray = tarray
    result.Unreadable = tnoread
    result.DeviceNotRunning = nodevs
    
    get_ratio = lambda a,b:float(len(a))/float(len(b))
    
    result.ArchRatio = get_ratio([t for t in readarch if t not in tnotup],readarch)
    result.ReadRatio = get_ratio(result.Readable,tattrs)
    result.LostRatio = get_ratio([a for a in tread if a in tnotup],tread)
    result.MissRatio = get_ratio([a for a in tread if a not in tarch],tread)
    result.OkRatio = 1.0-result.LostRatio-result.MissRatio
    
    result.Summary = '\n'.join((
      ('Checking archiving of %s attributes'%(len(attributes) if attributes else schema))
      ,('%d attributes in %s, %d are currently active'%(len(api),schema,len(tarch)))
      ,('%d devices with %d archived attributes are not running'%(len(nodevs),len([a for a in api if a.rsplit('/',1) in nodevs])))
      ,('%d archived attributes (%2.1f %%) are unreadable! (check and remove)'%(len(tnoread),1e2*get_ratio(tnoread,tarch)))
      ,('%d readable attributes are not archived'%(len(tmiss)))
      ,('%d attributes (readable or not) are updated (%2.1f %% of all readables)'%(len(tok),1e2*result.OkRatio))
      ,('-'*80)
      ,('%d archived attributes (readable or not) are not updated!'%len(tnotup))
      ,('%d archived and readable attributes are not updated! (check and restart?)'%len(treadnotup))
      ,('-'*80)
      ,('%d readable attributes have been removed in the last %d days!'%(len(removed),period/(24*3600)))
      ,('%d readable scalar attributes are not being archived (not needed anymore?)'%len(tmscalar))
      ,('%d readable array attributes are not being archived (Ok)'%len(tmarray))
      ,('%d readable array attributes are archived (Expensive)'%len(tarray))
      ,('')))
    
    if trace: print(result.Summary)
    print('%d readable lost,Ok = %2.1f%%, %2.1f %% over all Readables (%2.1f %% of total)'%\
        (len(treadnotup),1e2*result.ArchRatio,1e2*result.OkRatio,1e2*result.ReadRatio))

    if action:
        print('NO ACTIONS ARE GONNA BE EXECUTED, AS THESE ARE ONLY RECOMMENDATIONS')
        print("""
        api = PyTangoArchiving.ArchivingAPI('%s')
        lostdevs = sorted(set(api[a].archiver for a in result.NotUpdated))
        print(lostdevs)
        if lostdevs < a_reasonable_number:
          astor = fn.Astor()
          astor.load_from_devs_list(lostdevs)
          astor.stop_servers()
          fn.time.sleep(10.)
          astor.start_servers()
        """%schema)
        
    if trace: print('finished in %d seconds'%(fn.now()-ti))
        
    return result
  
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
  
def get_attribute_pytype(attribute=None,value=None):
    assert attribute or value
    v = value or check_attribute(attribute,readable=True)
    if not v: return None
    v  = getattr(v,'value',v)
    if fun.isString(v): 
       return str
    if fun.isSequence(v): 
       return list
    if fun.isBool(v): 
       return bool
    if any(s in str(type(v).__name__.lower()) for s in ('int','short','long')):
       return int
    return float
  
def get_only_scalar_attributes(model,exclude_strings=True):
    """
    This method will filter out all attributes that are either Arrays or Strings.
    Model can be a device name, a regexp expression or a list of attributes.
    """
    if fun.isSequence(model):
        attrs = model
    elif fun.isRegexp(model):
        attrs = fandango.get_matching_attributes(model)
    else:
        attrs = fandango.get_matching_attributes(model+'/*')
    
    exclude = [list] + ([str] if exclude_strings else [])
    return [a for a in attrs if get_attribute_pytype(a) not in exclude]
    
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

def create_attribute_tables(attribute):
    raise 'Method moved to PyTangoArchiving.dbs module'

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

def import_into_db(db,table,data,delete=False,offset=0):
    """
    db = a FriendlyDB instance
    table = table name
    data = [(time,value)] array
    offset = offset to apply to time values
    delete = boolean, if True the data between t0 and t-1 will be deleted from db before inserting.
    """
    #raise '@TODO:TEST THIS IN ARCHIVING02 BEFORE COMMIT'
    from fandango import time2str,date2str,date2time
    print 'import_into_db(%s,%s,[%s],%s,%s)'%(db,table,len(data),delete,offset)
    if delete: 
        limits = data[0][0],data[-1][0]
        t = db.Query("select count(*) from %s where time between '%s' and '%s'"%(table,time2str(limits[0]),time2str(limits[1])))[0]
        print('deleting %s values from %s'%(t,table))
        db.Query("delete from %s where time between '%s' and '%s'"%(table,time2str(limits[0]),time2str(limits[1])))
    if not db.Query('SHOW INDEX from %s'%table):
        try: db.Query('create index time on  %s (time)'%table)
        except: pass
    print('inserting %d values into %s ...'%(len(data),table))
    #for i,d in enumerate(data):
        #t = (fandango.time2str(d[0]+offset),d[1])
        #q = "INSERT INTO %s VALUES('%s',%s)"%(table,t[0],t[1])
        #db.Query(q)
    l,total = [],0
    for i,d in enumerate(data):
        l.append(d)
        if not (len(data)-(i+1))%100:
            q = "INSERT INTO `%s` VALUES %s ;"%(table,', '.join("('%s',%s)"%(fun.time2str(d[0]+offset),d[1] if 'none' not in str(d[1]).lower() else 'NULL') for d in l))
            #print q[:160]
            db.Query(q)
            total += len(l)
            print i,len(l),total
            l = []
    return total,len(data)

    #net = fandango.db.FriendlyDB('net6020a',user='...',passwd='...')
    #hdb = PyTangoArchiving.archiving.ArchivingAPI('hdb')
    #dev_table = dict((t[1].lower()[:4],t[0].lower()) for t in net.Query('select device,id from devices'))
    #def insert_data(r,offset,delete=''):
    #def get_data(r,dt=dev_table):
        #i = r.split('/')[-1]
        #if i[:4] not in dt:
            #print('no device found for %s'%i)
            #return
        #d = dt[i[:4]]
        #t = 'val_10sec_%s_0913'%d
        #a = 'NeutronDRMean' if r.endswith('n') else 'AccDRMean'
        #data = [(fandango.date2time(d[0]),d[1]) for d in net.Query("select time,%s from %s"%(a,t))]
        #return data

#########################################################################################

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

def rename_archived_attributes(attribs,load=False,restart=False,modes={'MODE_P':[10000]},schemas=('hdb','tdb')):
    """
    Renaming attributes in archiving 
    PyTangoArchiving.utils.rename_archived_attributes({oldname:newname}) 
    The following actions must be automated for both HDB and TDB
    """
    import archiving
    attribs = dict((k.lower(),v.lower()) for k,v in attribs.items())
    for schema in schemas:
        api = archiving.ArchivingAPI(schema)
        api.load_dedicated_archivers()
        #Get the list of old names 
        targets = dict((a,api[a].ID) for a in api if a in attribs)
        #Search for archivers 
        archivers = fandango.dicts.defaultdict(set)
        servers = fandango.dicts.defaultdict(set)
        for a in targets:
            arch = api[a].archiver
            if arch:
                servers[fandango.tango.get_device_info(arch).server].add(arch)
                archivers[arch].add(a)
        astor = fandango.Astor()
        if load: astor.load_from_devs_list(archivers.keys())
        
        #Check if they are dedicated 
        dedicated = dict((a,api[a].dedicated.lower()) for a in targets if api[a].dedicated)
        print('>> update dedicated')
        properties = []
        for arch in set(dedicated.values()):
            prop = map(str.lower,api.tango.get_device_property(arch,['reservedAttributes'])['reservedAttributes'])
            nprop = [attribs.get(p,p) for p in prop]
            properties.append((arch,nprop))
        print properties
        if load: [api.tango.put_device_property(arch,{'reservedAttributes':nprop}) for arch,nprop in properties]
            
        #Store the list of modes, 
        #NOP!, instead we will try to use the new modes provided as argument.
        #modes = dict.fromkeys(modes_to_string(api[a].modes) for a in targets)
        #[modes.__setitem__(k,[attribs[a] for a in targets if modes_to_string(api[a].modes)==k]) for k in modes.keys()]
        
        for server,archs in servers.items():
            if restart or modes is not None:
                for arch in archs:
                    atts = archivers[arch]
                    print('>> stopping archiving: %s'%atts)
                    if load: api.stop_archiving(atts)
            print('>> stopping archiver %s: %s'%(server,archs))
            if load: astor.stop_servers(server)
            for arch in archs:
                atts = archivers[arch]
                print('>> modifying adt table for %s attributes (%d)'%(arch,len(atts)))
                queries = []
                for name in atts:
                    ID = targets[name]
                    name = attribs[name]
                    device,att_name = name.rsplit('/',1)
                    domain,member,family = device.split('/')
                    queries.append("update adt set full_name='%s',device='%s',domain='%s',family='%s',member='%s',att_name='%s' where ID=%d" % (name,device,domain,family,member,att_name,ID))
                print '\n'.join(queries[:10]+['...'])
                if load: [api.db.Query(query) for query in queries]
            print('>> start %s archivers '%server)
            if load: 
                time.sleep(10)
                astor.start_servers(server)
                
        if load:
            fandango.Astor("ArchivingManager/*").stop_servers()
            time.sleep(15)
            fandango.Astor("ArchivingManager/*").start_servers()
            time.sleep(20)
        if restart or modes:
            print('>> start archiving: %s'%modes)
            if load: 
                api.start_archiving(attribs.values(),modes)
                #for m,atts in modes.items():
                    #m = modes_to_dict(m)
                    #api.start_archiving(atts,m)
    return archivers

def repair_attribute_names(db,attrlist=None,upper=False,update=False):
    """ 
    This method sets all domain/family/member names to upper case in the ADT table 
    db must be a FriendlyDB object like, db = FriendlyDB(db_name,host,user,passwd)
    """
    allnames = db.Query('SELECT full_name,device,att_name,ID FROM adt ORDER BY full_name',export=True)
    failed = 0
    device,attrs = '',[]
    if attrlist: attrlist = [a.lower() for a in attrlist]
    for line in sorted(allnames):
        fname,dev,att_name,ID = line
        if attrlist and fname.lower() not in attrlist:
            continue
        if dev.lower()!=device.lower(): #Device changed
            try:
                dp = PyTango.DeviceProxy(dev)
                device = dp.name()
                attrs = dp.get_attribute_list() #Getting real attribute names
            except:
                attrs = []
            
        try:
            if attrs: #If attribute list is not available we should not modify attribute names
                eq = [a for a in attrs if a.lower()==att_name.lower()]
                if eq: att_name = eq[0]
            #full_name = device+'/'+str(att_name) #Using real device name does not solve the problem when attributes are being re-inserted
            device = dev.upper() if upper else dev.lower()
            full_name = device+'/'+str(att_name)
            if full_name.rsplit('/',1)[0] == fname.rsplit('/',1)[0]: continue #Nothing to update
            domain,family,member = device.split('/')
            q = "update adt set domain = '%s',family = '%s',member = '%s',device = '%s',full_name = '%s', att_name = '%s' where id=%s" % (domain,family,member,device,full_name,att_name,ID)
            print "%s: %s"%(fname,q)
            if update: db.Query(q) 
        except Exception,e:
            print '%s: %s'%(fname,e)
            print traceback.format_exc()
            break
            failed += 1
    
    if update: db.Query('COMMIT')
    ok = len(allnames)-failed
    if update: print '%d names updated' % (len(attrlist or allnames)-failed)
    return ok
    
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
    
###############################################################################

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

###############################################################################
# SPECIAL CHECKS
###############################################################################

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

## @name Methods for repairing the databases
# @{
        
def RemoveWrongValues(db,table,column,null_value,ranges,dates,extra_clauses='',check=False):
    ''' Sets the specified null_value for all values in columnd out of specified ranges
    Usage (for removing all temperatures above 200 degrees): 
     * RemoveWrongValues('hdb','att_00001','value',None,[0,200])
    @remark Values cannot be deleted from archiving tables, NULL values must be inserted instead
    
    #EXAMPLE: 
    #In [42]:tables = [v.table for k,v in api.attributes.items() if re.match('ws/ct/plctest3/.*',k)]
    #In [44]:[PyTangoArchiving.utils.RemoveWrongValues('hdb',t,'value',None,[0,500],['2009-03-26','2009-04-07']) for t in tables]
    #In [48]:[PyTangoArchiving.utils.RemoveWrongValues('hdb',t,'value',None,[50,150],['2009-03-30 19:00:00','2009-04-01 19:00:00']) for t in tables]
    '''
    result = False
    start,stop=dates
    if type(start) is not str: start=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(start))
    if type(stop) is not str: stop=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(stop))
    ranges = type(ranges) in (list,set,tuple,dict) and ranges or list(ranges)
    max_,min_ = max(ranges),(len(ranges)>1 and min(ranges) or None)
       
    db = MySQLdb.connect(db='hdb',user='root')
    q = db.cursor()
    if check: query = "SELECT count(*) FROM %s" % (table)
    else: query = "UPDATE %s SET %s=%s" % (table,column,null_value is None and 'NULL' or null_value)
    where = " WHERE (%s" % ("%s > %s"%(column,max_))
    where += min_ is not None and " OR %s < %s)" % (column,min_) or ")"
    where += " AND (time BETWEEN '%s' AND '%s')" % (start,stop)
    if extra_clauses:
        where = " AND (%s)" % extra_clauses

    print 'the query is : ', query+where
    q.execute(query+where)
    if check:
        result = q.fetchone()[0]
        print 'result is %s; type is %s'%(result,type(result))
        print 'Values to remove: %d'%int(result)
    else:
        result = True
    db.close()
    return result
    
    
    #adt=q.fetchall()    
    #print 'There are %d attribute_ID registered in the database'%len(adt)
    #done=0
    #for line in adt:
        #ID = line[0]
        #full_name = line[1]
        #writable = line[2]
        #print 'ID %05d: %s, w=%d'%(ID,full_name,writable)
        #q.execute('describe att_%05d'%ID)
        #describe=q.fetchall()
        #col_name=describe[1][0]
        #col_type=describe[1][1]
        #if writable==int(PyTango.AttrWriteType.READ) and col_name!='value':
            #query='ALTER TABLE att_%05d CHANGE COLUMN %s value %s AFTER time'%(ID,col_name,col_type)
            #print 'query: ',query
            #q.execute(query)
            #done+=1
            
def RepairColumnNames():
    db = MySQLdb.connect(db='hdb',user='root')
    q = db.cursor()
    
    q.execute('select ID,full_name,writable from adt')
    adt=q.fetchall()
    
    print 'There are %d attribute_ID registered in the database'%len(adt)
    done=0
    for line in adt:
        ID = line[0]
        full_name = line[1]
        writable = line[2]
        print 'ID %05d: %s, w=%d'%(ID,full_name,writable)
        q.execute('describe %s'%get_table_name(ID))
        describe=q.fetchall()
        col_name=describe[1][0]
        col_type=describe[1][1]
        if writable==int(PyTango.AttrWriteType.READ) and col_name!='value':
            query='ALTER TABLE %s CHANGE COLUMN %s value %s AFTER time'%(get_table_name(ID),col_name,col_type)
            print 'query: ',query
            q.execute(query)
            done+=1
            
    print 'Attributes repaired: %d'%done    
                
##@}

def listLastTdbTime():
    db = MySQLdb.connect(db='tdb')
    q = db.cursor()
    q.execute('show tables')
    #It returns a TUPLE of TUPLES!!!, not a list!
    alltables = q.fetchall()
    
    q.execute('select ID,archiver from amt where stop_date is NULL')
    attribs = q.fetchall()
    
    attrtables = [ get_table_name(i[0]) for i in attribs ]
    print str(len(attribs))+' attributes being archived.'
    
    print 'Searching newest/oldest timestamps on attribute tables ...'
    results = []
    tmin,tmax = None,None
    
    for i,a in enumerate(attrtables):
        q.execute('select max(time),min(time) from '+a);
        #q.execute('select time from '+a+' order by time desc limit 1')
        #type returned is datetime.datetime
        row = q.fetchone()
        date,date2 = row[0],row[1]
        if tmax is None or date>tmax:
            tmax = date
        if tmin is None or date2<tmin:
            tmin = date2
        results.append((date,date2,a))
        print '\r%05d/%05d:\tOldest:%s;\tNewest:%s'%(i,len(attrtables),str(tmin),str(tmax)),
        sys.stdout.flush()
        
    results.sort()
    """
    print 'The last updated time found in database is '+str(results[0][1])+'-'+str(results[0][0])
    print 'Difference with newest is '+str(results.pop()[0]-results[0][0])
    
    print '\n'
    """

def KillAllServers(klass = 'HdbArchiver'):
    processes = linos.shell_command('ps uax').split('\n')
    archivers = [s for s in processes if '%s.%s'%(klass,klass) in s]
    for a in archivers:
        print 'Killing %s' % a[1:]
        pid = a.split()[1]
        linos.shell_command('kill -9 %s'%pid)
