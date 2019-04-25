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
            
    @Cached(expire=10.)
    def get_periodic_archivers_attributes(self,regexp='*'):
        #archs = fn.tango.get_class_devices('PyHdbppPeriodicArchiver')
        archivers = dict.fromkeys([a for a in self.get_periodic_archivers() 
                     if fn.clmatch(regexp,a)])
        for a in archivers:
            prop = fn.toList(fn.tango.get_device_property(a,'AttributeList'))
            archivers[a] = dict(p.split(';',1) for p in prop if p.strip())
        return archivers
    
    @Cached(expire=10.)
    def get_periodic_attribute_archiver(self,attribute):
        attribute = fn.tango.get_full_name(attribute,fqdn=True)
        archivers = self.get_periodic_archivers_attributes()
        for a,v in archivers.items():
            if attribute in v:
                return a
        return ''
    
    is_periodic_archived = get_periodic_attribute_archiver
    
    @Cached(expire=10.)
    def get_periodic_attributes(self):
        self.periodic_attributes = {}
        for v in self.get_periodic_archivers_attributes().values():
            for k,p in v.items():
                try:
                    p = [s for s in p.split(';') if 'period' in s][0]
                    self.periodic_attributes[k] = int(p.split('=')[-1])
                except:
                    print(fn.except2str())
        return self.periodic_attributes
    
    def get_next_periodic_archiver(self, attrexp=''):
        """
        attrexp can be used to get archivers already archiving attributes
        """
        loads = self.get_periodic_archivers_attributes()
                
        if attrexp:
            attrs = [a for a in self.get_periodic_attributes()
                        if fn.clmatch(attrexp,a)]
            archs = [self.get_periodic_attribute_archiver(a) for a in attrs]
            if archs:
                loads = dict((k,v) for k,v in loads.items() if k in archs)

        loads = sorted((len(v),k) for k,v in loads.items())
        return loads[0][-1]    
    
    @fn.Catched
    def add_periodic_attribute(self,attribute,period,archiver=None,wait=1.5):
        
        arch = self.get_periodic_attribute_archiver(attribute)
        if arch:
            print('%s is already archived by %s!' % (attribute,arch))
            return False
        
        attribute = parse_tango_model(attribute,fqdn=True).fullname
        archiver = archiver or self.get_next_periodic_archiver(
                            attrexp = fn.tango.get_dev_name(attribute)+'/*')
            
        if not self.is_attribute_archived(attribute):
            self.info('Attribute %s does not exist in %s database, adding it'
                      % (attribute, self.db_name))
            self.add_attribute(attribute,code_event=True)

        self.info('%s.AttributeAdd(%s,%s)' % (archiver,attribute,period))            
        dp = fn.get_device(archiver)
        v = dp.AttributeAdd([attribute,str(int(float(period)))])
        fn.wait(wait)
        return v
    
    def add_periodic_attributes(self,attributes,periods,wait=1.5):
        """
        attributes must be a list, periods a number, list or dict
        """
        attributes = sorted(parse_tango_model(a,fqdn=True).fullname 
                      for a in attributes)
        if fn.isNumber(periods):
            periods = dict((a,periods) for a in attributes)
        elif fn.isSequence(periods):
            periods = dict(zip(attributes,periods))

        devs = fn.defaultdict(list)
        [devs[fn.tango.get_dev_name(a)].append(a) for a in attributes]
        done = []
        
        for dev,attrs in devs.items():
            archiver = self.get_next_periodic_archiver(attrexp = dev+'/*')
            for attribute in attrs:
                try:
                    period = periods[attribute]
                    self.add_periodic_attribute(attribute,period,archiver,wait)
                    done.append((attribute,period,archiver))
                except:
                    self.warning(fn.except2str())
                
        return done

