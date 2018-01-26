
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

def get_schema_attributes(schema='*'):
    rd = pta.Reader(schema)
    alls = rd.get_attributes(active=True)
    return alls

def get_hdbpp_databases():
    cms = ft.get_class_devices('HdbConfigurationManager')
    dbs = {}
    for c in cms:
        props = ['LibConfiguration','ArchiverList']
        props = ft.get_database().get_device_property(c,props)
        db = dict(t.split('=') for t in props['LibConfiguration'])['dbname']
        dbs[db] = {c:None}
        for a in props['ArchiverList']:
            dbs[db][a] =  ft.get_device_property(a,'AttributeList')
    return dbs
    

def match_attributes_and_archivers(attrs=[],archs='archiving/es/*'):
    """
    This method returns matching archivers for a list of attributes
    in simplename format (no tango host).
    
    It applies AttributeFilters as defined in Tango DB (sorted)
    """
    if isString(attrs):
        attrs = ft.find_attributes(attrs)
    else:
        attrs = attrs or get_schema_attributes('*')
    
    devattrs = fn.dicts.defaultdict(set)
    [devattrs[a.rsplit('/',1)[0]].add(a) for a in attrs];    

    if isSequence(archs): archs = '(%s)'%')|('.join(archs)
    filters = get_archivers_filters(archs)
    r = devattrs.keys()
    archattrs = {}
    
    for i,k in enumerate(filters):
        v = filters[k]
        if 'DEFAULT' in v:
            df = k
        else:
            m = fn.filtersmart(r,v)
            currattrs = set(fn.join(*[devattrs[d] for d in m]))
            if len(currattrs):
                print(k,len(currattrs),sorted(set(i.split('/')[-2] for i in m)))
                archattrs[k] = currattrs
                print('\n')
            r = [a for a in r if a not in m]
        if i == len(filters)-1:
            k = df
            m = r
            currattrs = fn.join(*[devattrs[d] for d in m])
            if len(attrs):
                print(k,len(currattrs),sorted(set(i.split('/')[-2] for i in m)))
                archattrs[k] = currattrs
            
    return archattrs
        
############################################################################## 

def migrate_matching_attributes(regexp,simulate=True):
    
    rd = pta.Reader('*')
    allattrs = rd.get_archived_attributes(active=True)
    
    hdb,1
    
    for a in hdb,tdb:
        pass
