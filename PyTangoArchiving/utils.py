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

from fandango.functional import ( isString,isSequence,isCallable,
    str2time, str2epoch, clmatch, time2str, epoch2str, now,
    ctime2time, mysql2time, NaN ) # Mandatory imports!!!

from fandango.functional import ( date2time,date2str,mysql2time,ctime2time,
    time2str,isNumber,clmatch,isCallable ) # Mandatory imports!!!

import fandango.linos as linos
from fandango.dicts import SortedDict, CaselessDict # Mandatory imports!!!

from fandango.functional import reldiff,absdiff,seqdiff
from fandango.arrays import decimate_custom,decimate_array

###############################################################################
# Conditional imports to avoid PyTango dependency

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
    
###############################################################################    
    
class FakeAttributeHistory():
    def __init__(self,date,value):
        self.value = value
        self.time = PyTango.TimeVal(date) if not isinstance(date,PyTango.TimeVal) else date
    def __repr__(self): 
        return 'fbHistory(value=%s,time=%s)'%(self.value,self.time)
    
###############################################################################    


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
    
def get_alias_file(schema = ''):
    from fandango.tango import get_free_property,get_class_property
    if not schema or schema in ('*',''):
        alias_file = get_free_property('PyTangoArchiving','AliasFile')
    else:
        alias_file = get_class_property('%sextractor'%schema,'AliasFile')
        if isSequence(alias_file) and len(alias_file):
            alias_file = alias_file[0]
        if not alias_file:
            alias_file = get_alias_file()

    return alias_file
    
    
def read_alias_file(alias_file,trace=False):
    # Reading the Alias file
    # The format of the file will be:
    #   Alias                                   Attribute
    #   sr01/vc/eps-plc-01/sr01_vc_tc_s0112     sr01/vc/eps-plc-01/thermocouples[11]
    alias = CaselessDict()
    if alias_file:
        try:
            csv = fandango.arrays.CSVArray(alias_file)
            csv.setOffset(1)
            for i in range(csv.size()[0]):
                line = csv.getd(i)
                try: alias[line['Alias']] = line['Attribute']
                except: pass
            if trace: print('%d Attribute alias loaded from %s' % (len(alias),alias_file))
        except Exception,e:
            print('Unable to parse AliasFile: %s\n%s'%(alias_file,traceback.format_exc()))
            alias.clear()
    return alias        
        
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

def parse_property(name,value):
    """
    Parses properties in the format 'name(;|\n)key=value(;|\n)key=value'
    Used by HDB++ config and archivers
    """
    if '\n' in value:
        value = value.split('\n')
    elif ';' in value:
        value = value.split(';')
    else:
        value = [value]

    r = {'name': (value.pop(0) if '=' not in value[0] else name) }
    value = [v.strip().split('=',1) for v in value]
    r.update((v[0],v[-1]) for v in value)

    return r

WILDCARDS = '^$*+?{\|' #r'[]()
def is_regexp(seq,wildcards=WILDCARDS):
    """ 
    This function is just a hint, use it with care. 
    This function is an overload of the one in fandango, for convenience
    """
    return any(c in wildcards for c in seq)
    
def translate_attribute_alias(attribute):
    full = 'tango://' in attribute
    attribute = attribute.replace('tango://','')
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
    #if full: attribute = 'tango://' + attribute
    return attribute
  
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
    

###############################################################################

##############################################################################
# Methods used on Reader Class for Decimation/conversion

###############################################################################
# Decimation/Conversion methods

isNaN = lambda f: 'nan' in str(f).lower()
RULE_LAST = lambda v,w: sorted([v,w])[-1]
RULE_MAX = lambda v,w: (max((v[0],w[0])),max((v[1],w[1])))
START_OF_TIME = time.time()-10*365*24*3600 #Archiving reading limited to last 10 years.
MAX_RESOLUTION = 2*1080.

def overlap(int1,int2):
    """
    returns whether two date intervals overlap
    both intervals should be tuples of dates (start,stop)
    """
    int1,int2 = map(str2time,int1),map(str2time,int2)
    if int1[0] < int2[0] < int1[1]:
        return True
    elif int1[0] < int2[1] < int1[1]:
        return True
    elif int2[0] < int1[0] < int2[1]:
        return True
    elif int2[0] < int1[1] < int2[1]:
        return True
    else:
        return False

def get_days_in_interval(*args):
    """
    args may be a list of timestamps or just start,stop dates
    """
    if len(args)==2:
        if fun.isString(args[0]):
            start,stop = str2time(args[0]),str2time(args[1])
        else:
            start,stop = args[0],args[1]
        times = range(int(start),int(stop+86400),86400)
    else:
        times = args[0]
    days = sorted(set(list(d.split()[0] for d in map(time2str,times))))
    return days

def get_months_in_interval(*args):
    """
    args may be a list of timestamps or just start,stop dates
    """
    days = get_days_in_interval(*args)
    return sorted(set(d.rsplit('-',1)[0]+'-01' for d in days))

def get_next_month(date):
    if not fun.isString(date):
        date = time2str(date)
    year,month,day = map(int,date.split()[0].split('-'))
    if month<12:
        return '%04d-%02d-%02d' % (year,month+1,1)
    else:
        return '%04d-%02d-%02d' % (year+1,1,1)

def split_interval_in_months(start,stop,cut=True):
    """
    returns a list of start,stop tuples for each month in the interval
    if cut is True, then first and last interval are adjusted to start,stop
    """
    
    months = get_months_in_interval(start,stop)
    months.append(get_next_month(months[-1]))
    months = zip(months,months[1:])
    months = list(list((str2time(b),str2time(e)-1)) for b,e in months)
    if cut:
        months[0][0],months[-1][-1] = start,stop
    return months

def split_interval(start,stop,jump):
    starts = range(int(start),int(stop),int(jump))
    print(starts)
    return [(s,starts[i+1]) for i,s in enumerate(starts[:-1])]
    
#def get_jumps(values):
    #"""
    ###DEPRECATED by get_gaps
    #return time in ctime format whenever two points are separated 
    #for more than 2 minutes
    #"""
    #jumps = [(values[i][0],values[i+1][0]) for i in range(len(values)-1) 
             #if 120<(values[i+1][0]-values[i][0])]
    #return [[time.ctime(d) for d in j] for j in jumps]

def get_gaps(values, min_gap=5*24*3600, start=None, stop=None):
    """
    return time intervals in buffer where time distance is bigger than min_gap
    """
    if start and stop and not len(values):
        print('get_gaps, no values in interval')
        return [(start,stop)]
    elif not any((len(values),start,stop)):
        print('get_gaps, no values')
        return []        
        
    gaps = [(values[i][0],v[0]) for i,v in enumerate(values[1:])
                if min_gap < (v[0]-values[i][0])]
    
    if start and values[0][0] >  start + min_gap:
        gaps.insert(0,(start,values[0][0]))
    if stop and values[-1][0] < stop - min_gap:
        gaps.append((values[-1][0],stop))
    
    return gaps

def get_failed(values):
    i,failed = 0,[]
    while i<len(values)-1:
        if not isNaN(values[i][1]) and isNaN(values[i+1][1]):
            print 'found error at %s' % time.ctime(values[i+1][0])
            try:
                nxt = (j for j in range(i+1,len(values)) if not isNaN(values[j][1])).next()
                failed.append((values[i][0],values[i+1][0],values[nxt][0]))
                i = nxt
            except StopIteration: #Unable to find the next valid value
                print 'no more values found afterwards ...'
                failed.append((values[i][0],values[i+1][0],-1))
                break
        i+=1
    return [[time.ctime(d) for d in j] for j in failed]
        
def data_has_changed(val,prv,nxt=None,t=300):
    """ 
    Method to calculate if decimation is needed, 
    any value that preceeds a change is considered a change
    any time increment above 300 seconds is considered a change
    """
    return (val[1]!=prv[1] 
                    or (nxt is not None and nxt[1]!=prv[1]) 
                    or val[0]>(prv[0]+t))

def decimation(history,method,window='0',logger_obj=None, N=MAX_RESOLUTION):
    """
    Nones and NaNs are always removed if this method is called
    
    history: array of data
    method: method, callable or interval, from fandango.arrays
    window: string for time
    logger_obj: ArchivedTrendLogger or similar
    N: max array size to return
    """
    t0 = time.time()
    l0 = len(history)
    if not l0:
        return history
    
    trace = getattr(logger_obj,'warning',fandango.printf)
    try:
        if not isinstance(window,(int,float)):
            window = str2time(window or '0') 
    except: 
        window = 0

    if isNumber(method) and not window:
        method, window = fandango.arrays.pickfirst, float(method)
        
    start_date,stop_date = float(history[0][0]),float(history[-1][0])

    ## Decimation by data_has_changed is ALWAYS done
    if len(history): #method is not None
        nv = []
        #sq = isSequence(history[0][1])
        for i,v in enumerate(history):
            if (v[1] not in (None,NaN)# is not None and (sq or not isNaN(v[1]))
                    and (i in (0,l0-1,l0-2) or 
                        data_has_changed(history[i-1],v,history[i+1]))
                    ):
                nv.append(v)
        t1 = time.time()
        trace('Removed %d (None,NaN, Rep) values in %fs'
              %(l0-len(nv),t1-t0))

        t0,i,c,lh = t1,0,0,len(history)
        while i<len(history):
            if history[c] in (None,NaN):
                history.pop(c)
            else:
                c+=1
            i+=1
        t1 = time.time()
        trace('Removed %d (None,NaN, Rep) values in %fs'
              %(l0-len(history),t1-t0))
        history = nv   
        
    if (method and isCallable(method) and len(history)):
        # Data is filtered applying an averaging at every "window" interval.
        # As range() only accept integers the minimum window is 1 second.
        # It means that filtering 3 hours will implicitly prune millis data.        
        #DATA FROM EVAL IS ALREADY FILTERED; SHOULD NOT PASS THROUGH HERE
        
        if type(history[0][-1]) not in (int,float,bool):
            method = fandango.arrays.pickfirst
            trace('Decimation forced to %s' % method)
        
        wmin = max(1.,(stop_date-start_date)/(2*MAX_RESOLUTION))
        wauto = max(1.,(stop_date-start_date)/(10*N))
        trace('WMIN,WUSER,WAUTO = %s,%s,%s'%(wmin,window,wauto))
        window = wauto if not window else max((wmin,window))
        
        if len(history) > float(stop_date-start_date)/window:
            history = fandango.arrays.filter_array(
                data=history,window=window,method=method)
            t2 = time.time()
            trace('Decimated %d values to %d in %f seconds '
                  '(%s,%s)'
                  %(l0,len(history),t2-t1,method,window))
    else:
        trace('Decimation(%s) is not callable' % method)
            
    return history

def choose_first_value(v,w,t=0,tmin=-300):
    """ 
    Args are v,w for values and t for point to calcullate; 
    tmin is the min epoch to be considered valid
    """  
    r = (0,None)
    t = t or max((v[0],w[0]))
    if tmin<0: tmin = t+tmin
    if not v[0] or v[0]<w[0]: r = v #V chosen if V.time is smaller
    elif not w[0] or w[0]<v[0]: r = w #W chosen if W.time is smaller
    if tmin>0 and r[0]<tmin: r = (r[0],None) #If t<tmin; value returned is None
    return (t,r[1])

def choose_last_value(v,w,t=0,tmin=-300):
    """ 
    Args are v,w for values and t for point to calcullate; 
    tmin is the min epoch to be considered valid
    """  
    r = (0,None)
    t = t or max((v[0],w[0]))
    if tmin<0: tmin = t+tmin
    if not w[0] or v[0]>w[0]: r = v #V chosen if V.time is bigger
    elif not v[0] or w[0]>v[0]: r = w #W chosen if W.time is bigger
    if tmin>0 and r[0]<tmin: r = (r[0],None) #If t<tmin; value returned is None
    return (t,r[1])
    
def choose_max_value(v,w,t=0,tmin=-300):
    """ 
    Args are v,w for values and t for point to calcullate; 
    tmin is the min epoch to be considered valid
    """  
    r = (0,None)
    t = t or max((v[0],w[0]))
    if tmin<0: tmin = t+tmin
    if tmin>0:
        if v[0]<tmin: v = (0,None)
        if w[0]<tmin: w = (0,None)
    if not w[0] or v[1]>w[1]: r = v
    elif not v[0] or w[1]>v[1]: r = w
    return (t,r[1])
    
def choose_last_max_value(v,w,t=0,tmin=-300):
    """ 
    This method returns max value for epochs out of interval
    For epochs in interval, it returns latest
    Args are v,w for values and t for point to calcullate; 
    tmin is the min epoch to be considered valid
    """  
    if t>max((v[0],w[0])): return choose_max_value(v,w,t,tmin)
    else: return choose_last_value(v,w,t,tmin)

###############################################################################  
    
""" 
CONVERSION METHODS FROM MYSQL

This is how data looks like in the MySQL database tables:
    
Boolean spectrums in HDBArchivingReader
    In [9]:hdb.db.Query('select * from att_06339 limit 10')
    ((datetime.datetime(2014, 2, 23, 8, 41, 50),5,'false, false, false, false, false'),
Simple boolean (stored as strings!!)
    ((datetime.datetime(2014, 2, 21, 18, 39, 17), '0'),
DevShort
    ((datetime.datetime(2013, 2, 11, 14, 20, 15), 1.0, 0.0),
DevLong
    ((datetime.datetime(2013, 1, 12, 0, 58, 48), 2999.0),
DevLongArray
    (datetime.datetime(2013, 1, 11, 17, 6, 22),124,'1.0, 1.0, 1.0, 1.0, 3.0, 1.0, 1.0, 0.0, 1.0, 2.0, 2.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 2.0, 4.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0,
DevDouble
    ((datetime.datetime(2013, 1, 1, 0, 0, 6), 26.583766937255898),
DevDoubleArray
    (datetime.datetime(2013, 1, 11, 21, 17, 9),
    82,
    '24.3, 22.6, 21.7, 21.4, 19.4, 20.9, 20.1, 20.7, 21.8, 21.5, 20.3, 19.1, 19.8, 20.0, 20.2, 20.0, 20.1, 19.4, 20.0, 20.6, 20.8, 19.8, 19.8, 19.0, 19.7, 20.4, 21.1, 20.4, 20.2, 18.7, 20.3, 20.5, 20.4, 20.2, 21.0, 19.2, 20.6, 19.8, 20.8, 20.3, 21.5, 20.0, 19.8, 19.0, 19.3, 20.4, 20.0, 19.9, 19.7, 18.6, 19.2, 20.5, 20.6, 20.5, 19.2, 20.1, 19.4, 20.5, 20.7, 19.4, 19.8, 19.0, 19.9, 20.1, 20.7, 20.1, 21.6, 20.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0'))
DevState
    (datetime.datetime(2013, 1, 11, 18, 9, 41), 3.0),
"""    

def h_to_tuple(T):
    #if data_format == PyTango.AttrDataFormat.SPECTRUM:
    return (T.time.tv_sec,T.value if not hasattr(T.value,'__len__') 
            else tuple(T.value)) 

def get_mysql_value(v):
    #Date and read value (excluding dimension?!?)
    return (mysql2time(v[0]),v[1 if len(v)<4 else 2]) 

def listToHistoryBuffer(values):
    return [FakeAttributeHistory(*v) for v in values]

def mysql2array(v,data_type,default=None):
    #lambda v: [(s.strip() and data_type(s.strip()) 
    #  or (0.0 if data_type in (int,float) else None)) 
    #  for s in str(v[1]).split(',')]
    return [data_type(x) if x else default 
            for x in map(str.strip,str(v).split(','))]

def mysql2bool(v):
    v = str(v)
    if v in ('','None','none','null','NULL'): return None 
    if v in ('1','1.0','True','true'): return 1 
    return 0

        
def extract_array_index(values,array_index,decimate=False,asHistoryBuffer=False):
    """
    Applying array_index to the obtained results, it has to be applied after 
    attribute loading to allow reusing cache in array-indexed attributes
    
    decimate is just evaluated as True/False
    """
    last,l0 = (0,None),len(values)
    
    # Check if it has been already parsed
    for r in values:
        if r[1] is not None:
            if not fandango.isSequence(r[1]):
                return values
            break
    
    #print('extract_array_index(%s) to the obtained results'%array_index)
    array_index = int(array_index)
    new_values = [] # We create a new list on purpose to not modify the cached values
    fin = ((lambda v: (v.time,v.value[array_index] if v.value is not None and len(v.value)>array_index else None)) 
            if asHistoryBuffer else 
                (lambda v: (v[0],v[1][array_index] if v[1] is not None and len(v[1])>array_index else None)))
    fcmp = (lambda l: (l[0].tv_sec,l[1])) if asHistoryBuffer else (lambda l: l)
    fout = (lambda vv: FakeAttributeHistory(*(vv))) if asHistoryBuffer else (lambda vv: vv)
    for i,v in enumerate(values):
        try:
            if v is None: continue
            vv = fin(v)
            nxt = ((values[i+1] and fin(values[i+1]) or None) if i+1<l0 else None)
            if not decimate or nxt is None or not new_values or data_has_changed(fcmp(vv),fcmp(last),fcmp(nxt)):
                new_values.append(fout(vv))
                last = vv
        except Exception,e:
            print('extract_array_index(...,asHistoryBuffer=%s): Unable to parse %d[%s]:(%s); %s'%(asHistoryBuffer,i,array_index,v,traceback.format_exc()))
    if decimate:
        print('\tIn extract_array_index(...).raw: decimated repeated values in spectrum ... %s -> %s'%(l0,len(new_values)))
    return new_values


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
            
def sort_array(arg0,arg1=None,decimate=True,as_index=False,minstep=1e-3):
    """
    Args can be an (N,2) array or a tuple with 2 (times,values) arrays
    Takes two arrays of times and values of the same length and sorts them
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
        if decimate:
            return np.compress(
                get_array_steps(get_col(data,0).take(time_index), 
                                minstep = minstep, as_index = as_index),
                                time_index,0)
        else:
            return time_index
            
    else:
        sdata = data.take(time_index,0)
        if decimate:
            sdata = np.compress(
                get_array_steps(get_col(sdata,0), 
                                minstep = minstep, as_index = as_index),
                                sdata,0)
            
        print time.time()-t0
        return sdata
    
def get_array_steps(array,minstep=0.001,as_index=False):
    # Gets an integer with all differences > minstep
    # It calcullates steps and adds True at the beginning
    as_index = False
    print('get_array_steps(%s,%s,%s)'%(len(array),minstep,as_index))
    if not len(array): return array
    import numpy as np
    last,diff = array[0], np.zeros((len(array),), dtype = np.bool)
    #print('diff[%d]' % len(diff))
    diff[0] = True
    for i,a in enumerate(array[1:]):
        d = (a - last) > minstep
        last = d and a or last
        diff[i+1] = d
    #print('diff[%d]' % len(diff))
    
    ### THIS METHOD WAS NUMB!!! IT MATCHES HOLES, NOT DATA
    #   diff = np.abs(array[1:]-array[:-1])>minstep
    #   print('diff[%d]' % len(diff))
    #   diff = np.insert(diff,0,True)
    #   print('diff[%d]' % len(diff))
    
    diff = diff.nonzero()[0] if as_index else diff
    #print('diff[%d]' % len(diff))
    return diff
    
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
    if trace: 
        print('In patch_booleans(%d,%s)'%(len(history),(history or ('',))[0]))
    fromHistoryBuffer = len(history) and hasattr(history[0],'time')
    patch = 0

    for h in history: 
        v = h[1] if not fromHistoryBuffer else h.value
        if v is None: 
            continue
        if isinstance(v,int) or isinstance(v,float): 
            break
        if isinstance(v,basestring) and \
            (not isNumber(v) and v.lower() not in ('true','false','none')): 
            break
        if isinstance(v,basestring):
            if isNumber(v) and v in ('1','0','1.0','0.0'): patch=1
            if v.lower() in ('true','false','none'): patch=2
            break
        if isinstance(v,bool):
            patch = 1
            break

    if patch:
        for i,h in enumerate(history):
            v = h.value if fromHistoryBuffer else h[1]
            if patch==2: 
                v = v.lower().strip() in ('true','1')
            if fromHistoryBuffer: 
                h.value = int(v)
            else: 
                history[i] = (h[0],int(v))

    return history









