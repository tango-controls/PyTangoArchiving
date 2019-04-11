import fandango as fn
from fandango.objects import SingletonMap, Cached
from fandango.tango import *
from .config import HDBppDB

class HDBppPeriodic(HDBppDB):
    
    def add_periodic_archiver(self,server,device,properties={}):
        klass = 'PyHdbppPeriodicArchiver'
        if '/' not in server: 
            server = klass+'/'+server
        fn.tango.add_new_device(server,klass,fn.tango.get_normal_name(device))
        device = fn.tango.get_full_name(device,fqdn=True)
        properties.update({'ConfigurationManagerDS':self.manager})
        fn.tango.put_device_property(device,properties)
        archivers = self.get_periodic_archivers()
        archivers.append(device)
        fn.tango.put_device_property(self.manager,
            'PeriodicArchivers',archivers)
      
    def get_periodic_archivers(self):
        #archs = fn.tango.get_class_devices('PyHdbppPeriodicArchiver')
        archivers = fn.tango.get_device_property(
            self.manager,'PeriodicArchivers')
        if not fn.isSequence(archivers):
            archivers = fn.toList(archivers)
        try:
            return sorted(archivers)
        except:
            return []
            
    @Cached(expire=60.)
    def get_periodic_archivers_attributes(self,regexp='*'):
        #archs = fn.tango.get_class_devices('PyHdbppPeriodicArchiver')
        archivers = dict.fromkeys([a for a in self.get_periodic_archivers() 
                     if fn.clmatch(regexp,a)])
        for a in archivers:
            prop = fn.tango.get_device_property(a,'AttributeList')
            archivers[a] = [p.split(';')[0] for p in prop]
        return archivers
    
    @Cached(expire=10.)
    def get_periodic_attributes(self):
        loads = self.get_periodic_archivers_attributes()
        return sorted(filter(bool,fn.join(loads.values())))
    
    def add_periodic_attribute(self,attribute,period,archiver=None):
        if archiver is None:
            loads = self.get_periodic_archivers_attributes()
            loads = sorted((len(v),k) for k,v in loads.items())
            archiver = loads[-1][-1]
            
        if not self.is_attribute_archived(attribute):
            self.info('Attribute %s does not exist in %s database, adding it'
                      % (attribute, self.db_name))
            self.add_attribute(attribute,code_event=True)

        self.info('%s.AttributeAdd(%s,%s)' % (archiver,attribute,period))            
        dp = fn.get_device(archiver)
        return dp.AttributeAdd(map(str,[attribute,period]))
            
            
        
        

