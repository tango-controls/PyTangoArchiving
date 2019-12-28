import fandango as fn
from .query import HDBppReader, MIN_FILE_SIZE
from .periodic import HDBppPeriodic

class HDBpp(HDBppReader, HDBppPeriodic):
    """
    Wrapper on top of HDBpp API's to provide a dict-like implementation
    
    See PyTangoArchiving documentation for API specification and usage
    """
    
    def keys(self):
        if not self.attributes:
            self.get_attributes()
        return self.attributes.keys()
    
    def has_key(self,k):
        self.keys();
        k = fn.tango.get_full_name(k).lower()
        return k in self.attributes
    
    def __contains__(self,k):
        return self.has_key(k)
    
    def __len__(self):
        self.keys();
        return len(self.attributes)
    
    def values(self):
        self.keys();       
        return self.attributes.values()
    
    def items(self):
        self.keys();       
        return self.attributes.items()
    
    def __getitem__(self,key):
        self.keys();
        key = fn.tango.get_full_name(key).lower()
        return self.attributes[key]
    
    def __iter__(self):
        self.keys();
        return self.attributes.__iter__()

