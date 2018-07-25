#    "$Name:  $";
#    "$Header:  $";
#=============================================================================
#
# file :        PyExtractor.py
#
# description : Python source for the PyExtractor and its commands. 
#                The class is derived from Device. It represents the
#                CORBA servant object which will be accessed from the
#                network. All commands which can be executed on the
#                PyExtractor are implemented in this file.
#
# project :     TANGO Device Server
#
# $Author:  $
#
# $Revision:  $
#
# $Log:  $
#
# copyleft :    European Synchrotron Radiation Facility
#               BP 220, Grenoble 38043
#               FRANCE
#
#=============================================================================
#          This file is generated by POGO
#    (Program Obviously used to Generate tango Object)
#
#         (c) - Software Engineering Group - ESRF
#=============================================================================
#


import PyTango
import sys,time
import fandango
import fandango.functional as fn
import fandango.tango as ft
import PyTangoArchiving
import traceback
from fandango.objects import Cached

def decimate_values(values,N=1024,method=None):
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
      for v in values[:-1]:
        if v[0]>=(interval+float(result[-1][0])):
          #if not len(result)%100: print(interval,result[-1],v)
          result.append(v)
      result.append(values[-1])
    else:
      for v in values:
        if v[0]>=(interval+float(result[-1][0])):
          result.append(method(buff))
          buff = [result[-1]]
        buff.append(v)
    return result

class PyExtractor(PyTango.Device_4Impl):

#--------- Add you global variables here --------------------------

    @staticmethod
    def dates2times(argin):
        """
        Parsing dates like 'Y-M-D h:m' or '+/-X(shmdw)'
        """
        return [fn.time2str(fn.str2time(a)) for a in argin]
      
    @staticmethod
    def bool2float(argin):
        return float(not fn.isFalse(argin))
    
    @staticmethod
    def tag2attr(argin):
        if any(argin.endswith(s) for s in ('_r','_t','_w','_d','_l','_ld')): 
            argin = argin.rsplit('_',1)[0]
        if '/' not in argin: argin = argin.replace('__','/')
        return argin
    
    @staticmethod
    def attr2tag(argin):
        if '/' in argin: argin = argin.replace('/','__')
        return argin

    def read_dyn_attr(self,attr):
        
        try:
            #attr.set_value(1.0)
            aname,values = attr.get_name(),[]
            attribute = self.tag2attr(aname)
            print time.ctime()+'In read_dyn_attr(%s)'%aname
            print(self.counter)

            try:
                req,atformat,attype,data = self.AttrData[attribute]
            except Exception,e:
                print('Unable to read %s: key = %s ; cache = %s' % (attr,attribute,self.AttrData.keys()))
                traceback.print_exc()
                raise e

            conv = self.bool2float if attype is PyTango.DevBoolean \
            else (float if attype is PyTango.DevDouble
                else str)
            
            if aname.endswith('_r'):
                if atformat is PyTango.SpectrumAttr:
                    values = [conv(v[1] or 0.) for v in data]
                else:
                    values = [map(conv,v[1]) for v in data]
                if values: print time.ctime()+'In read_dyn_attr(%s): %s[%d]:%s...%s'%(aname,type(values[0]),len(values),values[0],values[-1])
                else: print '\tno values'
                attr.set_value(values,len(values))
                
            elif aname.endswith('_l'):
                print('%s: %s' % (aname,data))
                if data[-1:]:
                    value = conv(data[-1][1])
                    date =  float(data[-1][0] or 0.)
                    q = ft.AttrQuality.ATTR_VALID
                else:
                    value = None
                    date = fn.now()
                    q = ft.AttrQuality.ATTR_INVALID

                print( time.ctime()+'In read_dyn_attr(%s): (%s,%s,%s)' 
                    % ( aname, value, date, q ) )
                attr.set_value_date_quality((value or 0.),date,q)
                
            elif aname.endswith('_w'): 
                if atformat is PyTango.SpectrumAttr:
                    values = [conv(v[2] or 0.) for v in data]
                else:
                    values = [map(conv,v[2]) for v in data]
                if values: print time.ctime()+'In read_dyn_attr(%s): %s[%d]:%s...%s'%(aname,type(values[0]),len(values),values[0],values[-1])
                else: print '\tno values'
                attr.set_value(values,len(values))
                
            elif aname.endswith('_t'): 
                values = [float(v[0] or 0.) for v in data]
                if values: print time.ctime()+'In read_dyn_attr(%s): %s[%d]:%s...%s'%(aname,type(values[0]),len(values),values[0],values[-1])
                else: print '\tno values'
                attr.set_value(values,len(values))
                
            elif aname.endswith('_d'): 
                values = [fn.time2str(float(v[0] or 0.)) for v in data]
                if values: print time.ctime()+'In read_dyn_attr(%s): %s[%d]:%s...%s'%(aname,type(values[0]),len(values),values[0],values[-1])
                else: print '\tno values'
                attr.set_value(values,len(values))            
                
            elif aname.endswith('_ld'): 
                lv = [fn.time2str(float(v[0] or 0.)) for v in data[-1:]]
                if lv: 
                    print(time.ctime()+'In read_dyn_attr(%s): %s[%d]:%s...%s'
                            %(aname,type(lv[0]),len(lv),lv[0],lv[-1]))
                else: print '\tno values'
                attr.set_value(lv[-1])
                
            else:
                if atformat == PyTango.SpectrumAttr:
                    if attype == PyTango.DevString:
                        values = [(fn.time2str(d[0]),str(d[1])) for d in data]
                    else:
                        values = [(d[0],conv(d[1])) for d in data]
                else:
                    if attype is PyTango.DevString:
                        values = [[fn.time2str(d[0])]+map(str,d[1]) for d in data]
                    else:
                        values = [[d[0]]+map(conv,d[1]) for d in data]
                
                if values: 
                    print time.ctime()+'In read_dyn_attr(%s): %s[%d]:%s...%s'%(aname,type(values[0]),len(values),values[0],values[-1])
                else: 
                    print '\tno values'
                attr.set_value(values,len(values))
                
            print '\treturned %d values'%len(values)
            
        except Exception as e:
            traceback.print_exc()
            raise e
        
    def is_dyn_attr_allowed(self,attr,req_type=None):
        return True #self.IsDataReady(attr.name)
    
    def reader_hook(self,attribute,values):
        """This method will be executed by the ReaderProcess to process the queried data.""" 
        try:
            print('>'*80)
            print(time.ctime()+' In reader_hook(%s,[%d])'
                  %(attribute,len(values)))
            self.counter-=1
            print(self.counter)
            
            MAXDIM = 1024*1024*1024
            #First create the attributes
            epoch,data,aname = [],[],attribute.replace('/','__')
            values = decimate_values(values)
            [(epoch.append(v[0]),data.append(v[1])) for v in values]
            writable = PyTango.AttrWriteType.READ

            #Adding time attribute
            m,atformat,dims = None,PyTango.SpectrumAttr,[MAXDIM]
            for d in data:
              if d is not None:
                if fn.isSequence(d):
                  atformat,dims = PyTango.ImageAttr,[MAXDIM,MAXDIM]
                  m = d[0]
                else:
                  m = d
                break

            attype = PyTango.DevDouble if (fn.isNumber(m) or fn.isBool(m)) else PyTango.DevString
            self.add_attribute(
                PyTango.ImageAttr(aname,attype,writable,MAXDIM,MAXDIM),
                self.read_dyn_attr,None,self.is_dyn_attr_allowed)
            
            self.add_attribute(
                PyTango.SpectrumAttr(aname+'_t',PyTango.DevDouble, writable,MAXDIM),
                self.read_dyn_attr,None,self.is_dyn_attr_allowed)

            self.add_attribute(
                PyTango.SpectrumAttr(aname+'_d',PyTango.DevString, writable,MAXDIM),
                self.read_dyn_attr,None,self.is_dyn_attr_allowed)
            
            #ARRAY
            self.add_attribute(atformat(aname+'_r',attype, writable,*dims),
                               self.read_dyn_attr,None,self.is_dyn_attr_allowed)
            
            #LAST VALUE
            self.add_attribute(PyTango.Attr(aname+'_l',attype,PyTango.AttrWriteType.READ),
                               self.read_dyn_attr,None,self.is_dyn_attr_allowed)   
            
            #LAST DATE
            self.add_attribute(
                PyTango.Attr(aname+'_ld',PyTango.DevString,PyTango.AttrWriteType.READ),
                               self.read_dyn_attr,None,self.is_dyn_attr_allowed)              
            
            #Then add the data to Cache values, so IsDataReady will return True
            t = fn.now()
            self.RemoveCachedAttribute(attribute)
            self.AttrData[attribute] = (t,atformat,attype,values)
            print('Done: %s,%s,%s,%s,%d'%(attribute,t,atformat,attype,len(values)))
        except:
            print(traceback.format_exc())
    
#------------------------------------------------------------------
#    Device constructor
#------------------------------------------------------------------
    def __init__(self,cl, name):
        PyTango.Device_4Impl.__init__(self,cl,name)
        self.AttrData,self.reader = fandango.CaselessDict(),None #Created here to be init() proof
        PyExtractor.init_device(self)

#------------------------------------------------------------------
#    Device destructor
#------------------------------------------------------------------
    def delete_device(self):
        print time.ctime()+"[Device delete_device method] for device",self.get_name()
        self.reader.stop()
        #del self.reader
        print 'Waiting 10 seconds'
        time.sleep(10.)
        print 'Finished'

#------------------------------------------------------------------
#    Device initialization
#------------------------------------------------------------------
    def init_device(self):
        print time.ctime()+"In ", self.get_name(), "::init_device()"
        self.counter = 0
        self.set_state(PyTango.DevState.ON)
        self.get_device_properties(self.get_device_class())
        if not self.reader: 
            self.reader = PyTangoArchiving.reader.ReaderProcess(self.DbSchema)
        if self.AttrData: self.RemoveCachedAttributes()

#------------------------------------------------------------------
#    Always excuted hook method
#------------------------------------------------------------------
    def always_executed_hook(self):
        msg = 'Attributes in cache:\n'
        for k,v in self.AttrData.items():
            msg+='\t%s: %s\n'%(k,fn.time2str(v[0]))
            
        print(time.ctime()+"In "+ self.get_name()+ "::always_executed_hook()"+'\n'+msg)
        status = 'The device is in %s state\n\n'%self.get_state()
        status += msg
        self.set_status(status)
        self.GetCurrentQueries()


#==================================================================
#
#    PyExtractor read/write attribute methods
#
#==================================================================
#------------------------------------------------------------------
#    Read Attribute Hardware
#------------------------------------------------------------------
    def read_attr_hardware(self,data):
        #print time.ctime()+"In ", self.get_name(), "::read_attr_hardware()"
        pass




#==================================================================
#
#    PyExtractor command methods
#
#==================================================================

    @Cached(depth=30,expire=15.)
    def GetAttDataBetweenDates(self, argin):
        """
        Arguments to be AttrName, StartDate, StopDate, Synchronous
        
        If Synchronous is missing or False, data is buffered into attributes, which names are returned
        If True or Yes, all the data is returned when ready
        
        Data returned will be (rows,[t0,v0,t1,v1,t2,v2,...])
        """
        print time.ctime()+"In ", self.get_name(), "::GetAttDataBetweenDates(%s)"%argin
        #    Add your own code here
        size = 0
        aname = argin[0]
        tag = self.attr2tag(aname)
        dates = self.dates2times(argin[1:3])
        RW = False
        synch = fn.searchCl('yes|true',str(argin[3:4]))
        attrs = [tag,tag+'_r',tag+'_w',tag+'_t'] if RW else [tag,tag+'_r',tag+'_w',tag+'_t']
        
        self.reader.get_attribute_values(aname,
            (lambda v: self.reader_hook(aname,v)),dates[0],dates[1],
            decimate=True, cache=self.UseApiCache)
        self.counter+=1
        print(self.counter)
        
        argout = [fn.shape(attrs),[a for a in attrs]]
        
        if not synch:
          print '\t%s'%argout
          return argout
      
        else:
          while not self.IsDataReady(aname):
            fandango.wait(0.1)
          data = self.AttrData[aname][-1]
          for t,v in data:
            argout.append(t)
            argout.extend(fn.toSequence(v))
          return [fn.shape(data),argout]
        
    def GetCachedAttribute(self,argin):
        n,a = self.get_name(),self.attr2tag(argin)
        return [n+'/'+a+s for s in ('','_r','_t')] 

    def RemoveCachedAttribute(self, argin):
        print time.ctime()+"In ", self.get_name(), "::RemoveCachedAttribute(%s)"%argin
        #    Add your own code here
        argin = self.tag2attr(argin)
        if argin in self.AttrData:
            data = self.AttrData.pop(argin)
            del data
        else:
            print('\tAttribute %s not in AttrData!!!!'%argin)
        if False:
            #All this part disabled as it doesn't work well in PyTango 7.2.2
            try:
                attrlist = self.get_device_attr().get_attribute_list()
                attrlist = [a.get_name().lower() for a in attrlist]
                print 'Attributelist: %s'%[str(a) for a in attrlist]
            except:
                print traceback.format_exc()
            aname = argin.replace('/','__').lower()
            for s in ('','_r','_t',''):#,'_w'):
                try:
                    if aname in attrlist:
                        self.remove_attribute(aname+s)
                    else:
                        print('%s attribute does not exist!'%aname)
                except Exception,e: 
                    print('\tremove_attribute(%s): %s'%(aname+s,e))
        return

    def RemoveCachedAttributes(self):
        print "In ", self.get_name(), "::RemoveCachedAttributes()"
        #    Add your own code here
        remove = [a for a,v in self.AttrData.items() if v[0]<fn.now()-self.ExpireTime]
        for a in self.AttrData.keys()[:]:
            self.RemoveCachedAttribute(a)

    def IsArchived(self, argin):
        print "In ", self.get_name(), "::IsArchived()"
        #    Add your own code here
        return self.reader.is_attribute_archived(argin)

    def IsDataReady(self, argin):
        print "In ", self.get_name(), "::IsDataReady(%s)"%argin
        #    Add your own code here
        aname = self.tag2attr(argin)
        argout = aname in self.AttrData
        print '\tIsDataReady(%s == %s): %s'%(argin,aname,argout)
        return argout

    def GetCurrentArchivedAtt(self):
        print "In ", self.get_name(), "::GetCurrentArchivedAtt()"
        #    Add your own code here
        return self.reader.get_attributes(active=True)
      
    @Cached(depth=30,expire=10.)      
    def GetCurrentQueries(self):
        print("In "+self.get_name()+"::GetCurrentQueries()")
        
        #self.get_device_properties()
        #if not self.is_command_polled('state'):
        #self.poll_command('state',3000)
        try:
          pending = []
          for s in self.PeriodicQueries:
            s = s.split(',')
            a,t = s[0],max((float(s[-1]),self.ExpireTime))
            if a not in self.AttrData or self.AttrData[a][0]<(fn.now()-t):
              if a in self.AttrData: 
                print('%s data is %s seconds old'%(a,fn.now()-self.AttrData[a][0]))
              pending.append(s[:3])
              
          if pending: 
            self.set_state(PyTango.DevState.RUNNING)
            print('Executing %d scheduled queries:\n%s'%(len(pending),'\n'.join(map(str,pending))))
            for p in pending:
              self.GetAttDataBetweenDates(p)
          else: 
            self.set_state(PyTango.DevState.ON)
            
        except:
          self.set_state(PyTango.DevState.FAULT)
          self.set_status(traceback.format_exc())
          print(self.get_status())
          
        return self.PeriodicQueries
      
    def AddPeriodicQuery(self,argin):
        attribute = argin[0]
        start = argin[1]
        stop = argin[2] if len(argin)==4 else '-1'
        period = argin[3]
        self.get_device_properties()
        queries = dict(p.split(',',1) for p in self.PeriodicQueries)
        queries[attribute]='%s,%s,%s,%s'%(attribute,start,stop,period)
        fandango.tango.put_device_property(self.get_name(),'PeriodicQueries',sorted(queries.values()))
        self.get_device_properties()
        return self.get_name()+'/'+self.attr2tag(attribute)


#==================================================================
#
#    PyExtractorClass class definition
#
#==================================================================
class PyExtractorClass(PyTango.DeviceClass):

    #    Class Properties
    class_property_list = {
        'AliasFile':
            [PyTango.DevString,
            "",
            [] ],
        'DbConfig':
            [PyTango.DevString,
            "",
            [] ],
        'DbHost':
            [PyTango.DevString,
            "",
            [] ],
        }


    #    Device Properties
    device_property_list = {
        'DbSchema':
            [PyTango.DevString,
            "Database to use (hdb/tdb)",
            ["hdb"] ],
        'UseApiCache':
            [PyTango.DevBoolean,
            "Enable/Disable Reader Cache",
            [ True ] ],            
        'ExpireTime':
            [PyTango.DevLong,
            "Seconds to cache each request",
            [ 180 ] ],
        'PeriodicQueries':
            [PyTango.DevVarStringArray,
            "Queries to be executed periodically: Attr,Start,Stop,Period(s)",
            [ ] ],
        }


    #    Command definitions
    cmd_list = {
        'GetAttDataBetweenDates':
            [[PyTango.DevVarStringArray, ""],
            [PyTango.DevVarLongStringArray, ""]],
        'GetCachedAttribute':
            [[PyTango.DevString, ""],
            [PyTango.DevVarStringArray, ""]],            
        'RemoveCachedAttribute':
            [[PyTango.DevString, ""],
            [PyTango.DevVoid, ""]],
        'RemoveCachedAttributes':
            [[PyTango.DevVoid, ""],
            [PyTango.DevVoid, ""]],
        'IsArchived':
            [[PyTango.DevString, ""],
            [PyTango.DevBoolean, ""]],
        'IsDataReady':
            [[PyTango.DevString, "Requested attribute"],
            [PyTango.DevBoolean, ""]],
        'GetCurrentArchivedAtt':
            [[PyTango.DevVoid, ""],
            [PyTango.DevVarStringArray, ""]],
        'GetCurrentQueries':
            [[PyTango.DevVoid, ""],
            [PyTango.DevVarStringArray, ""],
            {
                'Polling period': "15000",
            } ],
        'AddPeriodicQuery':
            [[PyTango.DevVarStringArray, ""],
            [PyTango.DevString, ""]],            
        }


    #    Attribute definitions
    attr_list = {
        }


#------------------------------------------------------------------
#    PyExtractorClass Constructor
#------------------------------------------------------------------
    def __init__(self, name):
        PyTango.DeviceClass.__init__(self, name)
        self.set_type(name);
        print "In PyExtractorClass  constructor"

#==================================================================
#
#    PyExtractor class main method
#
#==================================================================
if __name__ == '__main__':
    try:
        py = PyTango.Util(sys.argv)
        py.add_TgClass(PyExtractorClass,PyExtractor,'PyExtractor')

        U = PyTango.Util.instance()
        U.server_init()
        U.server_run()

    except PyTango.DevFailed,e:
        print '-------> Received a DevFailed exception:',e
    except Exception,e:
        print '-------> An unforeseen exception occured....',e
