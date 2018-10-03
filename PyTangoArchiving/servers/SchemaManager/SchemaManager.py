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

From AttributesOn:

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
import pickle, traceback, re, os
import fandango as fn
import PyTangoArchiving as pyta
# PROTECTED REGION END #    //  SchemaManager.additionnal_import

__all__ = ["SchemaManager", "main"]


class SchemaManager(Device):
    """
    SchemaManager
    
    On = Currently Archived
    Off = Archived in the past
    
    From AttributesOn:
    
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
        def read_method(attr,comm=self.get_last_value),
                        log=self.debug_stream):
            try:
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
            
        self.threadDict = fandango.ThreadDict(
            read_method = read_method, timewait=0.1) #trace=True)
        
        [self.threadDict.append(a,value=[]) for a in self.attr_on]
        
        self.threadDict.start()
        self.info_stream('out of initThreadDict()')

                        
    def get_last_value(self,attribute,value=-1):
        if value == -1:
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
                
            s = '%s status is %s' % (self.schema, ns)
            for a in ('ArchiversOn','ArchiversOff','Attributes',
                    'AttributesOn','AttributesOff','AttributesOk',
                    'AttributesNok','AttributesLost','AttributesWrong',):
                try:
                    v = str(len(getattr(self,'read_%s'%a)()))
                except Exception,e:
                    v = str(e)
                s+='\n%s: %s' % (a,v)
            self.set_status(s)        
            self.push_change_event('Status')
        except:
            self.error_stream(fn.except2str())
    
    def check_attribute_value(self,a,v):
        """
        arguments are attribute name and last value from db
        """
        r = fn.check_attribute(a)
        t,rv = getattr(r,'time',None),getattr(r,'value',None)
        if isinstance(rv,(type(None),Exception)):
            # Attribute not readable
            self.attr_nok.append(a)
        else:
            if v is None or fn.isSequence(v) and not len(v):
                # Attribute has no values in DB
                self.attr_lost.append(a)
            else:
                t = fn.ctime2time(r.time)
                v = self.get_last_value(a,v)
                if v[0] < t-3600:
                    # Last value much older than current data
                    self.attr_lost.append(a)
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
        mandatory=True
    )

    Threaded = device_property(
        dtype='bool', default_value=False
    )

    ValuesFile = device_property(
        dtype='str',
    )

    # ----------
    # Attributes
    # ----------

    Attributes = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributesOk = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributesNok = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributesOn = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributesOff = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    Archivers = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    ArchiversOn = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    ArchiversOff = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributesValues = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributesWrong = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    AttributesLost = attribute(
        dtype=('str',),
        max_dim_x=65536,
    )

    # ---------------
    # General methods
    # ---------------

    def init_device(self):
        Device.init_device(self)
        self.set_change_event("AttributesOk", True, False)
        self.set_change_event("AttributesNok", True, False)
        self.set_change_event("AttributesOn", True, False)
        self.set_change_event("AttributesOff", True, False)
        self.set_change_event("Archivers", True, False)
        self.set_change_event("ArchiversOn", True, False)
        self.set_change_event("ArchiversOff", True, False)
        self.set_change_event("AttributesValues", True, False)
        self.set_change_event("AttributesWrong", True, False)
        self.set_change_event("AttributesLost", True, False)
        # PROTECTED REGION ID(SchemaManager.init_device) ENABLED START #
        self.schema = self.Schemas[0]
        self.api = pyta.api(self.schema)
        self.klass = ''
        self.is_hpp = None
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
        self.UpdateArchivers()
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
            self.threadDict.stop()
            print('waiting ...')
            t0 = time.time()
            #Waiting longer times does not avoid segfault (sigh)
            threading.Event().wait(3.) 
            print(time.time()-t0)
            print(self.threadDict.alive())
        except: 
            traceback.print_exc()
        print('-'*80)
        # PROTECTED REGION END #    //  SchemaManager.delete_device

    # ------------------
    # Attributes methods
    # ------------------

    def read_Attributes(self):
        # PROTECTED REGION ID(SchemaManager.Attributes_read) ENABLED START #
        return self.attributes
        # PROTECTED REGION END #    //  SchemaManager.Attributes_read

    def read_AttributesOk(self):
        # PROTECTED REGION ID(SchemaManager.AttributesOk_read) ENABLED START #
        return self.attr_ok
        # PROTECTED REGION END #    //  SchemaManager.AttributesOk_read

    def read_AttributesNok(self):
        # PROTECTED REGION ID(SchemaManager.AttributesNok_read) ENABLED START #
        return self.attr_nok
        # PROTECTED REGION END #    //  SchemaManager.AttributesNok_read

    def read_AttributesOn(self):
        # PROTECTED REGION ID(SchemaManager.AttributesOn_read) ENABLED START #
        return self.attr_on
        # PROTECTED REGION END #    //  SchemaManager.AttributesOn_read

    def read_AttributesOff(self):
        # PROTECTED REGION ID(SchemaManager.AttributesOff_read) ENABLED START #
        return self.attr_off
        # PROTECTED REGION END #    //  SchemaManager.AttributesOff_read

    def read_Archivers(self):
        # PROTECTED REGION ID(SchemaManager.Archivers_read) ENABLED START #
        return self.archivers
        # PROTECTED REGION END #    //  SchemaManager.Archivers_read

    def read_ArchiversOn(self):
        # PROTECTED REGION ID(SchemaManager.ArchiversOn_read) ENABLED START #
        return self.arch_on
        # PROTECTED REGION END #    //  SchemaManager.ArchiversOn_read

    def read_ArchiversOff(self):
        # PROTECTED REGION ID(SchemaManager.ArchiversOff_read) ENABLED START #
        return self.arch_off
        # PROTECTED REGION END #    //  SchemaManager.ArchiversOff_read

    def read_AttributesValues(self):
        # PROTECTED REGION ID(SchemaManager.AttributesValues_read) ENABLED START #
        return sorted('%s:%s' % (k,v) for k,v in self.values.items())
        # PROTECTED REGION END #    //  SchemaManager.AttributesValues_read

    def read_AttributesWrong(self):
        # PROTECTED REGION ID(SchemaManager.AttributesWrong_read) ENABLED START #
        return self.attr_err
        # PROTECTED REGION END #    //  SchemaManager.AttributesWrong_read

    def read_AttributesLost(self):
        # PROTECTED REGION ID(SchemaManager.AttributesLost_read) ENABLED START #
        return self.attr_lost
        # PROTECTED REGION END #    //  SchemaManager.AttributesLost_read


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
            for a in ['Attributes','AttributesOn','AttributesOff']:
                self.push_change_event(a,getattr(self,'read_%s'%a)())
            self.state_machine()
            self.info_stream(self.get_status())
        except:
            self.error_stream(fn.except2str())
        # PROTECTED REGION END #    //  SchemaManager.UpdateAttributes

    @command(
    polling_period=14400000,
    )
    @DebugIt()
    def UpdateValues(self):
        # PROTECTED REGION ID(SchemaManager.UpdateValues) ENABLED START #
        try:

            t0 = fn.now()
            self.info_stream('UpdateValues()')
            if (self.ValuesFile or '').strip():
                self.info_stream('Load values from: %s ...' % self.ValuesFile)
                if self.ValuesFile.endswith('json'):
                    self.values = fn.json2dict(self.ValuesFile)
                else:
                    with open(self.ValuesFile) as f:
                        self.values = pickle.load(f)

            elif self.Threaded:
                pass
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
                    self.check_attribute_value(a,v)
                except Exception as e:
                    self.attr_err.append(a)
                    traceback.print_exc()
                    m = str("%s: %s: %s" % (a, str(v), str(e)))
                    self.error_stream(m)
                    fn.wait(1e-6)
                    
            for a in ['AttributesValues','AttributesOk','AttributesNok',
                    'AttributesWrong','AttributesLost']:
                self.push_change_event(a,getattr(self,'read_%s'%a)())       
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
        self.archivers = self.api.get_archivers()
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
