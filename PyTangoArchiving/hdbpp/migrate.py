
import fandango as fn
import fandango.db as fdb
import fandango.tango as ft
from fandango.functional import *

import PyTangoArchiving as pta
##############################################################################    

def get_archivers_filters(archiver='archiving/es/*'):
    filters = fn.SortedDict(sorted((k,v['AttributeFilters']) for k,v in 
                    fn.tango.get_matching_device_properties(
                    archiver,'AttributeFilters').items()))
    return filters

def get_current_attributes(schema='*'):
    rd = pta.Reader(schema)
    alls = rd.get_attributes(active=True)
    return alls

def match_archivers_with_attributes(alls=[]):
    """
    This method returns matching archivers for a list of attributes
    in simplename format (no tango host).
    
    It applies AttributeFilters as defined in Tango DB (sorted)
    """
    alls = alls or get_current_attributes()
    if not fn.isSequence(alls): alls = [alls]
    devattrs = fn.dicts.defaultdict(set)
    [devattrs[a.rsplit('/',1)[0]].add(a) for a in alls];    

    filters = get_archivers_filters()
    r = devattrs.keys()
    archattrs = {}
    
    for i,k in enumerate(filters):
        v = filters[k]
        if 'DEFAULT' in v:
            df = k
        else:
            m = fn.filtersmart(r,v)
            attrs = set(fn.join(*[devattrs[d] for d in m]))
            if len(attrs):
                print(k,len(attrs),sorted(set(i.split('/')[-2] for i in m)))
                archattrs[k] = attrs
                print('\n')
            r = [a for a in r if a not in m]
        if i == len(filters)-1:
            k = df
            m = r
            attrs = fn.join(*[devattrs[d] for d in m])
            if len(attrs):
                print(k,len(attrs),sorted(set(i.split('/')[-2] for i in m)))
                archattrs[k] = attrs
            
    return archattrs
        
############################################################################## 

def migrate_matching_attributes(regexp,simulate=True):
    
    rd = pta.Reader('*')
    allattrs = rd.get_archived_attributes(active=True)
    
    hdb,1
    
    for a in hdb,tdb:
        pass
