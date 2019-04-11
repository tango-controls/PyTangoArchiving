import fandango as fn
from fandango.objects import SingletonMap, Cached
from fandango.tango import *
from .config import HDBppDB

class HDBppPeriodic(HDBppDB):
    
    def add_periodic_archiver(self,server,device):
        pass
      
    def get_periodic_archivers(self):
        #archs = fn.tango.get_class_devices('PyHdbppPeriodicArchiver')
        
    def get_periodic_archivers_attributes(self,regexp='*'):
        #archs = fn.tango.get_class_devices('PyHdbppPeriodicArchiver')
        
    def add_periodic_archiver(self):
        #pass
