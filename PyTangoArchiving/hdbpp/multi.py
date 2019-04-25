import sys,os,re,traceback

import fandango as fn
import fandango.db as fdb
import fandango.tango as ft
from fandango.functional import *
from fandango import Cached

import PyTangoArchiving
import PyTangoArchiving as pta
##############################################################################    

   
def get_schema_attributes(schema='*'):
    rd = pta.Reader(schema)
    alls = rd.get_attributes(active=True)
    return alls

def is_attribute_code_pushed(device,attribute,\
        event=ft.EventType.ARCHIVE_EVENT):
    """
    Returns True if it is code pushed
    Returns False if it is not pushed but available anyway
    Returns None if attribute has no events
    """
    if isString(device): 
        device = ft.get_device(device)
    cb = lambda *args: None
    r = None
    try:
        e = device.subscribe_event(attribute,event,cb)
        device.unsubscribe_event(e)
        if not device.is_attribute_polled(attribute):
            # Pushed by Code
            return True
        # Pushed by Polling
        return False
    except:
        # No events available
        return None
        

def get_hdbpp_databases(active=True): #archivers=[],dbs={}):
    """
    Method to obtain list of dbs/archivers; it allows to match any 
    archiver list against existing dbs.
    
    This method can be used in cached mode executed like:
    
        dbs = get_hdbpp_databases()
        for a in archivers:
            db = get_hdbpp_databases(a,dbs).keys()[0]
      
    """
    schemas = pta.Schemas.load()
    hdbpp = sorted(k for k in schemas if fn.clsearch('hdbpp',str(schemas[k])))
    if active:
        r = []
        for h in hdbpp:
            try:
                if fn.check_device(pta.api(h).manager):
                    r.append(h)
            except:
                pass
        return r
    else:
        return hdbpp
    
def get_hdbpp_filters():
    dbs = get_hdbpp_databases(active=True)
    sch = pta.Schemas.load()
    return dict((d,sch[d].get('filters','*')) for d in dbs)

def get_hdbpp_for_attributes(attrlist):
    filters = get_hdbpp_filters()
    r = fn.defaultdict(list)
    for a in attrlist:
        for d,f in filters.items():
            if fn.clmatch(f,a,extend=True):
                r[d].append(a)
    return r

def merge_csv_attrs(exported = True, currents = True, check_dups = True):
    """
    OJU! Correctors are not exported but should be archived anyway!
    """
    folder = fn.tango.get_free_property('PyTangoArchiving','CSVFolder')
    csvs = [f for f in fn.listdir(folder) if f.endswith('csv')]
    print('Parsing %d files from %s' % (len(csvs),folder))
    archattrs = fn.defaultdict(dict)

    alldevs = fn.tango.get_all_devices(exported = exported)
    
    sources = dict()
    for f in csvs:
        try:
            sources[f] = pta.ParseCSV(folder+f)
        except Exception,e:
            print('%s failed: %s\n'%(f,e))
    
    wrongs = []
    for f,data in sources.items():
        a,m,p = '','',''
        for a in data:
            try:
                d = fn.tango.parse_tango_model(a).devicename
                if d.lower() in wrongs: 
                    continue
                elif d.lower() not in alldevs:
                    print('%s: %s do not exists' % (f,d))
                    wrongs.append(d.lower())
                    continue
                
                for m in ('HDB','TDB'):
                    if m in data[a]:
                        a,r,prev = a.lower(),data[a],0
                        mode = sorted(v+[k,f] for k,v in r[m].items())[0]
                        p = mode[0]
                        
                        if a in archattrs and m in archattrs[a]:
                            if check_dups and 'file' in archattrs[a]:
                                print('%s duplicated: %s and %s' %(a,archattrs[a]['file'],f))
                            prev = archattrs[a][m][0]
                            
                        if not prev or p < prev:
                            archattrs[a]['file'] = f
                            archattrs[a][m] = mode
                            
            except Exception,e:
                print(f,a,m,e)
                
    if currents:
        hdb,tdb = pta.api('hdb'),pta.api('tdb')
        for api in ('hdb','tdb'):
            api = pta.api(api)
            m = api.schema.upper()
            for a in api.get_archived_attributes():
                mode = sorted(v+[k,api.schema] for k,v in api[a].modes.items())[0]
                if a not in archattrs or m not in archattrs[a] \
                        or mode[0] < archattrs[a][m][0]:
                    archattrs[a][api.schema.upper()] = mode
                    archattrs[a]['file'] = api.schema
                elif a in archattrs and mode[0] < archattrs[a][m][0]:
                    print('%s had slower DB settings!? %s > %s' % (a,mode,archattrs[a]))
            
    return archattrs

#csvattrs = merge_csv_attrs(False,True,False)

def check_attribute_in_all_dbs(attr_regexp,reader = None,
                               start = None, stop = None):
    reader = reader or PyTangoArchiving.Reader()
    attrs = [a for a in reader.get_attributes() if clmatch(attr_regexp,a)]
    schemas = kmap(reader.is_attribute_archived,attrs)
    result = []
    for a,ss in schemas:
        for s in ss:
            try:
                v = reader.configs[s].get_attribute_values(a,start,stop)
            except:
                v = None
            result.append((a,s,v and len(v)))
    return result

## DEPRECATED
#def get_managers_filters(archiver=''):
    ##filters = fn.SortedDict(sorted((k,v['AttributeFilters']) for k,v in 
                    ##fn.tango.get_matching_device_properties(
                    ##archiver,'AttributeFilters').items()))
    #managers = fn.tango.get_class_devices('HdbConfigurationManager')
    #filters = [fn.tango.get_device_property(m,'AttributeFilters')
                   #for m in managers]
    #filters = zip(managers,map(fn.toList,filters))
    #return filters

## DEPRECATED
#@Cached(expire=60.)
#def get_database_for_attributes(attrs):
    #result = dict()
    #filters = get_managers_filters()
    #for a in attrs:
        #for m,f in filters.items():
            #pass
            

def get_archivers_for_attributes(attrs=[],archs='archiving/es/*'):
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
        v = filters[k] # k is the archiver name
        k = fn.tango.parse_tango_model(k, fqdn = True).fullname
        if 'DEFAULT' in v:
            df = k
        else:
            #filtersmart(list,regexp): returns a clsearch on the list
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
            if len(currattrs):
                print(k,len(currattrs),sorted(set(i.split('/')[-2] for i in m)))
                archattrs[k] = currattrs
            
    return archattrs

match_attributes_and_archivers = get_archivers_for_attributes
        
############################################################################## 

def get_current_conf(attr):
    
    rd = pta.Reader()
    curr = rd.is_attribute_archived(attr)
    if not curr: return {}
    result = dict.fromkeys(curr)
    
    abs_event,per_event,rel_event = 0,60000,0
    events = fn.tango.get_attribute_events(attr)
    polling = events.get('polling',3000.)
                
    if events.get('arch_event',None):
        result['arch_abs_event'] = events['arch_event'][0]
        result['arch_rel_event'] = events['arch_event'][1]
        result['arch_per_event'] = events['arch_event'][2]
    else:
        for s in ('hdb','tdb'):
            if s in curr:
                api = pta.api(s)
                modes = api[attr].modes
                if 'MODE_P' in modes:
                    per_event = modes['MODE_P'][0]
                    polling = min((per_event,polling))
                if 'MODE_A' in modes:
                    abs_event = modes['MODE_A'][1]
                    polling = min((modes['MODE_A'][0],polling))
                if 'MODE_R' in modes:
                    rel_event = modes['MODE_R'][1]
                    polling = min((modes['MODE_R'][0],polling))
                
        if abs_event and per_event != polling:
            result['arch_abs_event'] = float(abs_event)
        if rel_event and per_event != polling:
            result['arch_rel_event'] = float(rel_event)
        if per_event:
            result['arch_per_event'] = int(per_event)

    result['polling'] = int(polling)        
    return result

def start_archiving_for_attributes(attrs,*args,**kwargs):
    """
    from start_archiving(self,attribute,archiver,period=0,
                      rel_event=None,per_event=0,abs_event=None,
                      code_event=False, ttl=None, start=False):

    See HDBpp.add_attribute.__doc__ for a full description of arguments
    """    
    #archs = get_archivers_for_attributes(attrs)
    #dbs = get_hdbpp_databases(archs.keys())
    done = []
    
    if not args and not kwargs:
        kwargs['code_event'] = True
    
    dbs = get_hdbpp_for_attributes(attrs)
    
    for db,attrlist in dbs.items():
        print('Launching %d attributes in %s'%(len(attrlist),db))
        api = PyTangoArchiving.Schemas.getApi(db)
        api.add_attributes(attrlist,*args,**kwargs)
        done.extend(map(fn.tango.get_full_name,attrlist))

    if len(done)!=len(attrs):
        print('No hdbpp database match for: %s' % str(
            [a for a in attrs if fn.tango.get_full_name(a) not in done]))

    return done

def get_last_values_for_attributes(attrs,*args,**kwargs):
    """
    def start_archiving(self,attribute,archiver,period=0,
                      rel_event=None,per_event=300000,abs_event=None,
                      code_event=False, ttl=None, start=False):

    See HDBpp.add_attribute.__doc__ for a full description of arguments
    """    
    attrs = [fn.tango.get_full_name(a) for a in attrs]
    archs = get_archivers_for_attributes(attrs)
    dbs = get_hdbpp_databases(archs.keys())
    result = dict((a,None) for a in attrs)
    
    for db,devs in dbs.items():
        api = PyTangoArchiving.Schemas.getApi(db)
        devs = [d for d in devs if d in archs]
        for d in devs:
            for t in archs[d]:
                result.update(api.load_last_values(t))

    return result


def get_class_archiving(target):
    """ 
    target: device or class
    Reads Class.Archiving property and parses it as:
    Attribute,Polling,Abs change,Rel change,Periodic
    """
    if '/' in target:
        target = fn.tango.get_device_info(target).dev_class
    config = fn.tango.get_class_property(target,'Archiving')
    attrs = dict(t.split(',',1) for t in config)
    for a,v in attrs.items():
        try:
            v = map(float,v.split(','))
            attrs[a] = {'polling':int(v[0])}
            attrs[a]['arch_abs_event'] = v[1] or None
            attrs[a]['arch_rel_event'] = v[2] or None
            attrs[a]['arch_per_event'] = v[3] or None
        except:
            pass
    return attrs

def start_attributes_for_archivers(target,attr_regexp='',event_conf={},
            load=False, by_class=False, min_polling = 100, overwrite = False, 
            check = True):
    """
    Target may be an attribute list or a device regular expression
    if by_class = True, config will be loaded from Tango class properties
    """
    import PyTangoArchiving.hdbpp as ptah
    
    if fn.isSequence(target):
        if attr_regexp:
            attrs = [a for a in target if fn.clmatch(attr_regexp,a.rsplit('/')[-1])]
        else:
            attrs = target

    else:
        dev_regexp = target
        attrs = fn.find_attributes(dev_regexp+'/'+(attr_regexp or '*'))

    if by_class:
        classes = fn.defaultdict(dict)
        devs = fn.defaultdict(list)
        [devs[a.rsplit('/',1)[0]].append(a) for a in attrs]
        
        for d,v in devs.items():
            classes[fn.tango.get_device_class(d)][d] = v
            
        attrs = {}
        for c,devs in classes.items():
            cfg = get_class_archiving(devs.keys()[0])
            for d in devs:
                raw = devs[d]
                for a,v in cfg.items():
                    for aa in raw:
                        if fn.clmatch(a,aa.split('/')[-1],terminate=True):
                            if not attr_regexp or fn.clmatch(attr_regexp,aa):
                                attrs[aa] = v

    elif event_conf:
        attrs = dict((a,event_conf) for a in attrs)
        
    else:
        attrs = dict((a,get_current_conf(a)) for a in attrs)
        
    print('Starting %d attributes' % (len(attrs)))

    archs = ptah.multi.match_attributes_and_archivers(attrs.keys())
    rd = PyTangoArchiving.Reader()
    #print(archs)
    alldbs = ptah.multi.get_hdbpp_databases()
    dbs = ptah.multi.get_hdbpp_databases(archs,alldbs)
    #return dbs,archs,attrs

    for db,rcs in dbs.items():
        api = PyTangoArchiving.Schemas.getApi(db)
        dbs[db] = dict.fromkeys(rcs)
        for d in rcs:
            dbs[db][d] = ts = dict.fromkeys(archs[d])
            #return ts
            for a in ts:
                try:
                    m = fn.parse_tango_model(a,fqdn=True)
                    dbs[db][d][a] = mode = attrs[a]
                    if not overwrite and db in rd.is_attribute_archived(a):
                        print('%s already archived in %s' % (a,db))
                        continue
                    events = ft.check_attribute_events(a,ft.EventType.ARCHIVE_EVENT)
                    ep = events.get(ft.EventType.ARCHIVE_EVENT,False)
                    if ep is True:
                        if 'polling' in mode: 
                            mode.pop('polling')
                    elif isinstance(events.get(ep,(int,float))):
                        mode['polling'] = min((ep,mode.get('polling',10000)))
                        mode['polling'] = max((mode['polling'],min_polling))
                        
                    if not events.get(ft.EventType.CHANGE_EVENT,False):
                        if mode.get('archive_abs_change',0):
                            mode['abs_event'] = mode['archive_abs_change']
                        if mode.get('archive_rel_change',0):
                            mode['rel_event'] = mode['archive_rel_change']    
                        if mode.get('arch_per_event',0):
                            mode['per_event'] = mode['archive_per_event']                               
                        
                    print('%s.start_archiving(%s,%s,%s): %s' % (db,d,m.fullname,mode,load))
                    if load:
                        fn.tango.set_attribute_events(a,**mode)
                        r = api.start_archiving(m.fullname,d,code_event=True)
                        assert not check or r
                except:
                    print('%s failed!'%a)
                    traceback.print_exc()

    return dbs

def migrate_matching_attributes(regexp,simulate=True):
    
    rd = pta.Reader('*')
    allattrs = rd.get_archived_attributes(active=True)
    
    hdb,tdb = pta.api('hdb'),pta.api('tdb')
    
    for a in hdb,tdb:
        pass

