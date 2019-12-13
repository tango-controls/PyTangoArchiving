# -*- coding: utf-8 -*-
#
# This file is part of the SchemaManager project
#
# www.tango-controls.org
#
# Distributed under the terms of the LGPL license.
# See LICENSE.txt for more info.

""" SchemaManager

SchemaManager

On = Currently Archived
Off = Archived in the past

From AttributeOnList:

Ok = Have values updated, different from None
Lost = Values are not updated
Nok = Value is not readable (None in DB)
Err = Value is readable but None in DB!?
"""

# PyTango imports
import PyTango
from PyTango import DebugIt
from PyTango.server import run
from PyTango.server import Device, DeviceMeta
from PyTango.server import attribute, command
from PyTango.server import device_property
from PyTango import AttrQuality, DispLevel, DevState
from PyTango import AttrWriteType, PipeWriteType
# Additional import
# PROTECTED REGION ID(SchemaManager.additionnal_import) ENABLED START #
import pickle, traceback, re, os, signal, time
import fandango as fn
import PyTangoArchiving as pyta

@fn.Cached(depth=10000,expire=600,catched=True)
def check_attribute_value(attribute):
    #check_attribute may return AttrValue, Exception or None if not found
    return fn.tango.check_attribute(attribute)

@fn.Cached(depth=10000,expire=600,catched=True)
def check_attribute_events(attribute):
    return bool(fn.tango.check_attribute_events(attribute))
    
# PROTECTED REGION END #    //  SchemaManager.additionnal_import

__all__ = ["SchemaManager", "main"]


class SchemaManager(Device):
    """
    SchemaManager
    
    On = Currently Archived
    Off = Archived in the past
    
    From AttributeOnList:
    
    Ok = Have values updated, different from None
    Lost = Values are not updated
    Nok = Value is not readable (None in DB)
    Err = Value is readable but None in DB!?
    """
    __metaclass__ = DeviceMeta
    # PROTECTED REGION ID(SchemaManager.class_variable) ENABLED START #
    
    ## Threaded behaviour methods --------------------------------
    
    def initThreadDict(self):
        """
        It creates a ThreadDict, a dictionary of modbus commands 
        which results will be updated periodically.
        All registers used by Mappings are by default added as keys.
        For each key it will execute a ReadHoldingRegisters modbus command,
        """
        self.info_stream('initThreadDict()')
        def read_method(attr,comm=self.get_last_value,
                        log=self.debug_stream):
            try:
                #Cache renewal, avoid unnecessary queries
                if not check_attribute_value(attr):
                    result = Exception('Unreadable!')
                elif self.is_hpp and not check_attribute_events(attr):
                    result = Exception('NoEvents!')
                else:
                    result = comm(attr)
                    
                return result
            except PyTango.DevFailed,e:
                print('Exception in ThreadDict.read_method!!!'
                    +'\n'+str(e).replace('\n','')[:100])
            except Exception,e:
                print('#'*80+'\n'+'Exception in ThreadDict.read_method!!!'
                      +'\n'+traceback.format_exc()+'\n'+'#'*80)
                # Arrays shouldnt be readable if communication doesn't work!
                return [] 
            
        self.threadDict = fn.ThreadDict(
            read_method = read_method, timewait=0.1) #trace=True)

        signal.signal(signal.SIGINT, self.threadDict.stop)
        signal.signal(signal.SIGTERM, self.threadDict.stop)
        self.threadDict.start()
        self.info_stream('out of initThreadDict()')
        return self.threadDict

    def get_last_value(self,attribute,value=-1):
        if value == -1:
            self.debug_stream('load_last_values(%s)' % attribute)
            value = self.api.load_last_values(attribute)
        if hasattr(value,'values'): 
            value = value.values()[0]
        if (fn.isSequence(value) and len(value) and 
                fn.isSequence(value[0]) and len(value[0])>1):
            value = value[0]
        if value and isinstance(value[0],fn.datetime.datetime):
            value = (fn.date2time(value[0]),value[1])
        return value
    
    def get_cached_value(self,attr,val=None):
        if self.threadDict._updates.get(attr,0):
           val = self.threadDict[attr]
            #if isinstance(val,Exception):
                #raise Exception('Exception: %s: %s.' % (attr,str(val)) )
        return val
    
    def get_database(self):
        if hasattr(self.api,'getTables'):
            return self.api
        else:
            return self.api.db
    
    def state_machine(self):
        try:
            s = self.get_state()
            if len(self.attributes):
                if len(self.arch_off):
                    ns = PyTango.DevState.FAULT
                elif len(self.attr_ok)<=0.75*len(self.attr_on):
                    ns = PyTango.DevState.ALARM
                elif not self.threadDict.cycle_count:
                    ns = PyTango.DevState.MOVING
                else:
                    ns = PyTango.DevState.ON
            else:
                ns = PyTango.DevState.INIT
            if ns != s:
                self.set_state(ns)
                self.push_change_event('State')
                self.info_stream('%s => %s'%(s,ns))
                
            s = '%s status is %s, updated at %s' % (
                self.schema, ns, fn.time2str(self.update_time))
            for a in ('ArchiverOnList','ArchiverOffList','AttributeList',
                    'AttributeOnList','AttributeOffList','AttributeOkList',
                    'AttributeNokList','AttributeLostList','AttributeWrongList',
                    'AttributeNoevList','AttributeStalledList'):
                try:
                    v = str(len(getattr(self,'read_%s'%a)()))
                except Exception,e:
                    v = str(e)
                s+='\n%s = %s' % (a,v)
            self.set_status(s)        
            self.push_change_event('Status')
        except:
            self.error_stream(fn.except2str())
    
    def check_attribute_ok(self,a,v,t=0):
        """
        arguments are attribute name and last value from db, plus ref. time
        """
        r = check_attribute_value(a)
        rv = getattr(r,'value',None)
        if isinstance(rv,(type(None),Exception)):
            # Attribute not readable
            self.attr_nok.append(a)
        elif self.is_hpp and not check_attribute_events(a):
            self.attr_nevs.append(a)
        else:
            if v is None or fn.isSequence(v) and not len(v):
                # Attribute has no values in DB
                self.attr_lost.append(a)
            else:
                # Time is compared against last update, current or read time
                t = min((t or fn.now(),fn.ctime2time(r.time)))
                v = self.get_last_value(a,v)
                try: 
                    diff = v[1]!=rv
                except: 
                    diff = 1
                if v[0] < t-3600:
                    if any(diff) if fn.isSequence(diff) else bool(diff):
                        # Last value much older than current data
                        self.attr_lost.append(a)
                    else:
                        self.attr_stall.append(a)
                        self.attr_ok.append(a)
                elif v[1] is None:
                    # Value is readable but not from DB
                    self.attr_err.append(a)
                else:
                    self.attr_ok.append(a)
        return
    
    # PROTECTED REGION END #    //  SchemaManager.class_variable

    # -----------------
    # Device Properties
    # -----------------

    Schemas = device_property(
        dtype=('str',),
    )

    Threaded = device_property(
        dtype='bool', default_value=True
    )

    ValuesFile = device_property(
        dtype='str',
    )

    CacheTime = device_property(
        dtype='int', default_value=600
    )

    # ----------
    # Attributes
    # ----------

    DatabaseSize = attribute(
        dtype='int',
    )

    AttributeList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributeOkList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributeNokList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributeOnList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributeOffList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    ArchiverList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    ArchiverOnList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    ArchiverOffList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributeValues = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributeWrongList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributeLostList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributeNoevList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributeStalledList = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    TableSizesInBytes = attribute(
        dtype=('int',),
        max_dim_x=65536,
    )

    TableNames = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    # ---------------
    # General methods
    # ---------------

    def init_device(self):
        Device.init_device(self)
        self.set_change_event("DatabaseSize", True, False)
        self.set_change_event("AttributeOkList", True, False)
        self.set_change_event("AttributeNokList", True, False)
        self.set_change_event("AttributeOnList", True, False)
        self.set_change_event("AttributeOffList", True, False)
        self.set_change_event("ArchiverList", True, False)
        self.set_change_event("ArchiverOnList", True, False)
        self.set_change_event("ArchiverOffList", True, False)
        self.set_change_event("AttributeValues", True, False)
        self.set_change_event("AttributeWrongList", True, False)
        self.set_change_event("AttributeLostList", True, False)
        self.set_change_event("AttributeNoevList", True, False)
        self.set_change_event("AttributeStalledList", True, False)
        self.set_change_event("TableSizesInBytes", True, False)
        # PROTECTED REGION ID(SchemaManager.init_device) ENABLED START #
        check_attribute_value.expire = self.CacheTime
        check_attribute_events.expire = self.CacheTime
        self.schema = self.Schemas[0]
        self.api = pyta.api(self.schema)
        self.klass = ''
        self.is_hpp = None
        self.update_time = 0
        self.attributes = []
        self.values = {}
        self.arch_on = []
        self.arch_off = []
        self.attr_on = []
        self.attr_off = []
        self.attr_ok = []
        self.attr_lost = []
        self.attr_nok = []        
        self.attr_err = []
        self.attr_nevs = []
        self.attr_stall = []
        self.UpdateArchivers()
        self.threadDict = self.initThreadDict() if self.Threaded else None

        # PROTECTED REGION END #    //  SchemaManager.init_device

    def always_executed_hook(self):
        # PROTECTED REGION ID(SchemaManager.always_executed_hook) ENABLED START #

        self.state_machine()
        
        # PROTECTED REGION END #    //  SchemaManager.always_executed_hook

    def delete_device(self):
        # PROTECTED REGION ID(SchemaManager.delete_device) ENABLED START #
        try:
            print('-'*80)
            print("[Device delete_device method] for %s"%self.get_name())
            self.set_state(PyTango.DevState.INIT)
            if self.threadDict and self.threadDict.alive():
                self.threadDict.stop()
                print('waiting ...')
                #Waiting longer times does not avoid segfault (sigh)
                fn.wait(3.) 
        except: 
            traceback.print_exc()
        print('-'*80)
        # PROTECTED REGION END #    //  SchemaManager.delete_device

    # ------------------
    # Attributes methods
    # ------------------

    def read_DatabaseSize(self):
        # PROTECTED REGION ID(SchemaManager.DatabaseSize_read) ENABLED START #
        return self.get_database().getDbSize()
        # PROTECTED REGION END #    //  SchemaManager.DatabaseSize_read

    def read_AttributeList(self):
        # PROTECTED REGION ID(SchemaManager.AttributeList_read) ENABLED START #
        return self.attributes
        # PROTECTED REGION END #    //  SchemaManager.AttributeList_read

    def read_AttributeOkList(self):
        # PROTECTED REGION ID(SchemaManager.AttributeOkList_read) ENABLED START #
        return self.attr_ok
        # PROTECTED REGION END #    //  SchemaManager.AttributeOkList_read

    def read_AttributeNokList(self):
        # PROTECTED REGION ID(SchemaManager.AttributeNokList_read) ENABLED START #
        return self.attr_nok
        # PROTECTED REGION END #    //  SchemaManager.AttributeNokList_read

    def read_AttributeOnList(self):
        # PROTECTED REGION ID(SchemaManager.AttributeOnList_read) ENABLED START #
        return self.attr_on
        # PROTECTED REGION END #    //  SchemaManager.AttributeOnList_read

    def read_AttributeOffList(self):
        # PROTECTED REGION ID(SchemaManager.AttributeOffList_read) ENABLED START #
        return self.attr_off
        # PROTECTED REGION END #    //  SchemaManager.AttributeOffList_read

    def read_ArchiverList(self):
        # PROTECTED REGION ID(SchemaManager.ArchiverList_read) ENABLED START #
        return self.archivers
        # PROTECTED REGION END #    //  SchemaManager.ArchiverList_read

    def read_ArchiverOnList(self):
        # PROTECTED REGION ID(SchemaManager.ArchiverOnList_read) ENABLED START #
        return self.arch_on
        # PROTECTED REGION END #    //  SchemaManager.ArchiverOnList_read

    def read_ArchiverOffList(self):
        # PROTECTED REGION ID(SchemaManager.ArchiverOffList_read) ENABLED START #
        return self.arch_off
        # PROTECTED REGION END #    //  SchemaManager.ArchiverOffList_read

    def read_AttributeValues(self):
        # PROTECTED REGION ID(SchemaManager.AttributeValues_read) ENABLED START #
        return sorted('%s=%s' % (k,v) for k,v in self.values.items())
        # PROTECTED REGION END #    //  SchemaManager.AttributeValues_read

    def read_AttributeWrongList(self):
        # PROTECTED REGION ID(SchemaManager.AttributeWrongList_read) ENABLED START #
        return self.attr_err
        # PROTECTED REGION END #    //  SchemaManager.AttributeWrongList_read

    def read_AttributeLostList(self):
        # PROTECTED REGION ID(SchemaManager.AttributeLostList_read) ENABLED START #
        return self.attr_lost
        # PROTECTED REGION END #    //  SchemaManager.AttributeLostList_read

    def read_AttributeNoevList(self):
        # PROTECTED REGION ID(SchemaManager.AttributeNoevList_read) ENABLED START #
        return self.attr_nevs
        # PROTECTED REGION END #    //  SchemaManager.AttributeNoevList_read

    def read_AttributeStalledList(self):
        # PROTECTED REGION ID(SchemaManager.AttributeStalledList_read) ENABLED START #
        return self.attr_stall
        # PROTECTED REGION END #    //  SchemaManager.AttributeStalledList_read

    def read_TableSizesInBytes(self):
        # PROTECTED REGION ID(SchemaManager.TableSizesInBytes_read) ENABLED START #
        db = self.get_database()
        tables = sorted(db.getTables())
        return [db.getTableSize(t) for t in tables]
        # PROTECTED REGION END #    //  SchemaManager.TableSizesInBytes_read

    def read_TableNames(self):
        # PROTECTED REGION ID(SchemaManager.TableNames_read) ENABLED START #
        return sorted(self.get_database().getTables())
        # PROTECTED REGION END #    //  SchemaManager.TableNames_read


    # --------
    # Commands
    # --------

    @command(
    polling_period=60000,
    )
    @DebugIt()
    def UpdateAttributes(self):
        # PROTECTED REGION ID(SchemaManager.UpdateAttributes) ENABLED START #
        try:
            self.info_stream('UpdateAttributes()')
            self.attributes = sorted(self.api.keys())
            self.attr_on = sorted(self.api.get_archived_attributes())
            self.attr_off = [a for a in self.attributes 
                             if a not in self.attr_on]
            self.info_stream('pushing_events')
            for a in ['AttributeList','AttributeOnList','AttributeOffList']:
                self.push_change_event(a,getattr(self,'read_%s'%a)())

            if self.Threaded:
                [self.threadDict.append(a,value=[]) for a in self.attr_on]
                
            self.state_machine()
            self.info_stream(self.get_status())
        except:
            self.error_stream(fn.except2str())
        # PROTECTED REGION END #    //  SchemaManager.UpdateAttributes

    @command(
    )
    @DebugIt()
    def UpdateValues(self):
        # PROTECTED REGION ID(SchemaManager.UpdateValues) ENABLED START #
        try:

            t0 = t1 = fn.now()
            self.info_stream('UpdateValues()')
            
            if (self.ValuesFile or '').strip():
                self.info_stream('Load values from: %s ...' % self.ValuesFile)
                if self.ValuesFile.endswith('json'):
                    self.values = fn.json2dict(self.ValuesFile)
                else:
                    with open(self.ValuesFile) as f:
                        self.values = pickle.load(f)
                        
                self.values = dict((a,self.get_last_value(a,v)) 
                                   for a,v in self.values.items())
                t1 = max(v[0] for v in self.values.values() if v)
                t1 = min((t1,fn.now()))
                self.info_stream('reference time is %s' % fn.time2str(t1))

            elif self.Threaded:
                self.info_stream('Loading values from thread cache ...')
                self.values = dict((a,v) for a,v in self.threadDict.items()
                    if self.threadDict._updates.get(a,0))
            else:
                self.info_stream('Loading values from db ...')
                self.values = self.api.load_last_values(self.attr_on)

            self.info_stream('Updating %d values: %s' % (
                len(self.values),str(len(self.values) 
                                     and self.values.items()[0])))
            self.attr_ok = []
            self.attr_nok = []
            self.attr_lost = []
            self.attr_err = []
            for a,v in sorted(self.values.items()):
                try:
                    a = fn.tango.get_full_name(a)
                    if self.Threaded:
                        t1 = self.threadDict._updates.get(a,0)
                    self.check_attribute_ok(a,v,t=t1)
                except Exception as e:
                    self.attr_err.append(a)
                    traceback.print_exc()
                    m = str("%s: %s: %s" % (a, str(v), str(e)))
                    #self.error_stream(m)
                    print('*'*80)
                    print(fn.time2str()+' '+self.get_name()+'.ERROR!:'+m)
                    fn.wait(1e-6)
                    
            for a in ['AttributeValues','AttributeOkList','AttributeNokList',
                    'AttributeWrongList','AttributeLostList',
                    'AttributeNoevList','AttributeStalledList']:
                self.push_change_event(a,getattr(self,'read_%s'%a)())
                
            self.update_time = fn.now()
            self.state_machine()
            self.info_stream(self.get_status())
            self.info_stream('UpdateValues() took %f seconds' % (fn.now()-t0))
            
        except Exception as e:
            traceback.print_exc()
            self.error_stream(fn.except2str())
            raise e            
        # PROTECTED REGION END #    //  SchemaManager.UpdateValues

    @command(
    polling_period=60000,
    )
    @DebugIt()
    def UpdateArchivers(self):
        # PROTECTED REGION ID(SchemaManager.UpdateArchivers) ENABLED START #
        self.info_stream('UpdateArchivers()')
        self.klass,self.is_hpp = ('HdbArchiver',0) if self.schema == 'hdb' \
            else ('TdbArchiver',0) if self.schema == 'tdb' \
                else ('HdbEventSubscriber',1)
            
        self.arch_on, self.arch_off = [],[]
        self.archivers = map(fn.tango.get_full_name,self.api.get_archivers())
        for d in sorted(self.archivers):
            if fn.check_device(d) not in (None,PyTango.DevState.FAULT):
                self.arch_on.append(d)
            else:
                self.arch_off.append(d)
        self.state_machine()
        # PROTECTED REGION END #    //  SchemaManager.UpdateArchivers

    @command(
    dtype_in='str', 
    dtype_out='str', 
    )
    @DebugIt()
    def Test(self, argin):
        # PROTECTED REGION ID(SchemaManager.Test) ENABLED START #
        try:
            v = str(eval(argin,{'self':self}))
        except:
            v = fn.except2str()
        return 
        # PROTECTED REGION END #    //  SchemaManager.Test

# ----------
# Run server
# ----------


def main(args=None, **kwargs):
    # PROTECTED REGION ID(SchemaManager.main) ENABLED START #
    return run((SchemaManager,), args=args, **kwargs)
    # PROTECTED REGION END #    //  SchemaManager.main

if __name__ == '__main__':
    main()
