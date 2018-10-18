#!/usr/bin/env python

import sys, pickle, os, re, traceback
import PyTangoArchiving as pta
import fandango as fn
from fandango.functional import *

__doc__ = USAGE = """
Usage:
    check_archiving_schema schema [period=] [ti=] [values=] [--export]
    
    or
    
    check_archiving_schema schema [action=start_devices]
    
    or just save to pickle file
    
    check_archiving_schema schema action=save filename=/tmp/schema.pck

"""

def check_archiving_schema(
        schema='hdb',
        attributes=[],values={},
        ti = None,
        period = 7200,
        old_period=24*3600*90,\
        exclude=['*/waveid','*/wavename','*/elotech-*'],
        use_index = True,
        loads = True,
        action=False,
        trace=True,
        export=None):

    ti = fn.now() if ti is None else str2time(ti) if isString(ti) else ti
    
    api = pta.api(schema)
    is_hpp = isinstance(api,pta.HDBpp)
    check = dict()
    old_period = 24*3600*old_period if old_period < 1000 \
        else (24*old_period if old_period<3600 else old_period)
    
    allattrs = api.get_attributes() if hasattr(api,'get_attributes') else api.keys()
    print('%s contains %d attributes' % (schema,len(allattrs)))
    
    if attributes:
        if fn.isString(attributes) and fn.isRegexp(attributes):
            tattrs = [a for a in allattrs if clsearch(attributes,a)]
        else:
            attributes = map(fn.tango.get_normal_name,fn.toList(attributes))
            tattrs = [a for a in allattrs 
                      if fn.tango.get_normal_name(a) in allattrs]
            
    else:
        tattrs = allattrs
    
    excluded = [a for a in tattrs if any(fn.clmatch(e,a) for e in exclude)]
    tattrs = [a for a in tattrs if a not in excluded]
    
    print('%d attributes to check' % len(tattrs))
    if not len(tattrs):
        return 
    
    if excluded:
        print('\t%d attributes excluded' % len(excluded))
    
    archived = {}
    for a in tattrs:
        if hasattr(api,'get_attribute_archiver'):
            arch = api.get_attribute_archiver(a) 
        else:
            arch = api[a].archiver
        if arch: 
            archived[a] = arch
  
    print('\t%d attributes are archived' % len(archived))
    
    #Getting Tango devices currently not running
    alldevs = set(t.rsplit('/',1)[0] for t in tattrs)
    #tdevs = filter(fn.check_device,alldevs)
    #nodevs = [fn.tango.get_normal_name(d) for d in alldevs if d not in tdevs]
    #if nodevs:
        #print('\t%d devices are not running' % len(nodevs))
        
    archs = sorted(set(archived.values()))
    if loads:
        astor = fn.Astor()
        astor.load_from_devs_list(archs)
        loads = fn.defaultdict(list)
        for k,s in astor.items():
            for d in s.get_device_list():
                d = fn.tango.get_normal_name(d)
                for a in archived:
                    if fn.tango.get_normal_name(archived[a]) == d:
                        loads[k].append(a)
        for k,s in sorted(loads.items()):
            print('\t%s archives %d attributes'%(k,len(s)))
    
    noarchs = [fn.tango.get_normal_name(d) for d in archs if not fn.check_device(d)]
    if noarchs:
        print('\t%d archivers are not running: %s' % (len(noarchs),noarchs))
      
    ###########################################################################

    if isString(values) and values.endswith('.pck'):
        print('\nLoading last values from %s file\n' % values)
        import pickle
        values = pickle.load(open(values))
        
    elif isString(values) and values.endswith('.json'):
        print('\nLoading last values from %s file\n' % values)
        values = fn.json2dict(values)        

    else: #if not use_index or is_hpp:
        print('\nGetting last values ...\n')
        for a in tattrs:
            values[a] = api.load_last_values(a)
        
    #else:
        #print('\nGetting updated tables from database ...\n')
        #tups = api.db.get_table_updates()
        ## Some tables do not update MySQL index tables
        #t0 = [a for a in archived if a in tattrs and not tups[api[a].table]]
        #check.update((t,check_attribute(a,readable=True)) for t in t0 if not check.get(t))
        #t0 = [t for t in t0 if check[t]]
        #print('%d/%d archived attributes have indexes not updated ...'%(len(t0),len(archived)))
        #if t0 and len(t0)<100: 
            #vs = api.load_last_values(t0);
            #tups.update((api[t].table,api[t].last_date) for t in t0)

        #for a in tattrs:
            #if a in tups:
                #values[a] = [tups[api[a].table],0]
            
    for k,v in values.items():
        if (len(v) if isSequence(v) else v):
            if isinstance(v,dict): 
                v = v.values()[0]
            if isSequence(v) and len(v)==1:
                v = v[0]
            if v and not isNumber(v[0]):
                v = [date2time(v[0]),v[1]]
            values[k] = v
        else:
            values[k] = [] if isSequence(v) else None
                
    print('%d values obtained' % len(values))
    
    ###########################################################################
    
    now = fn.now()
    result = fn.Struct()
    times = [t[0] for t in values.values() if t]
    futures = [t for t in times if t>now]
    times = [t for t in times if t<now]
    tmiss = []
    tfutures = [k for k,v in values.items() if v and v[0] in futures]
    tmin,tmax = min(times),max(times)
    print('\toldest update was %s' % time2str(tmin))
    print('\tnewest update was %s' % time2str(tmax))
    if futures:
        print('\t%d attributes have values in the future!' % len(futures))

    tnovals = [a for a in archived if not values.get(a,None)]
    if tnovals:
        print('\t%d archived attributes have no values' % len(tnovals))    
    try:
        tmiss = [a for a,v in values.items() if v 
                 and old_period < v[0] < ti-period and a not in archived]
    except:
        print(values.items()[0])
    if tmiss:
        print('\t%d/%d attrs with values are not archived anymore' % (len(tmiss),len(tattrs)))
        
    result.Excluded = excluded
    result.Schema = schema
    result.All = tattrs
    result.Archived = values   
        
    result.NoValues = tnovals
    result.MissingOrRemoved = tmiss        
    
    result.TMin = tmin
    result.TMax = tmax
    result.Futures = tfutures    
        
    tup = sorted(a for a in values if values[a] and values[a][0] > ti-period)
    tok = [a for a in tup if values[a][1] not in (None,[])]
    print('\n%d/%d archived attributes are updated since %s - %s' 
          % (len(tup),len(archived),ti,period))
    print('%d archived attributes are fully ok\n' % (len(tok)))

    tnotup = sorted(a for a in values if values[a] and values[a][0] < ti-period)
    print('\t%d archived attrs are not updated' % len(tnotup))    
    tupnoread = [a for a in tup if not values[a][1]
               and fn.read_attribute(a) is None]
    
    reads = dict((a,fn.read_attribute(a)) for a in tnotup)
    tnotupread = [a for a in tnotup if reads[a] is not None]
    print('\t%d not updated attrs are readable (Lost)' % len(tnotupread))    
    print('\t%d of them are not floats' 
          % len([t for t in tnotupread if not isinstance(reads[t],float)]))
    print('\t%d of them are states' 
          % len([t for t in tnotupread if t.lower().endswith('/state')]))
    print('\t%d of them seem motors' 
          % len([t for t in tnotupread if t.lower().endswith('/position')]))
    
    tnotupevs = [a for a in tnotupread if fn.tango.check_attribute_events(a)]
    print('\t%d not updated attrs are readable and have events (LostEvents)' % len(tnotupevs))    
    
    tnotupnotread = [a for a in tnotup if a not in tnotupread]
    print('\t%d not updated attrs are not readable' % len(tnotupnotread))
    
    result.Lost = tnotupread
    result.LostEvents = tnotupevs    
    
    losts = (tnotupevs if is_hpp else tnotupread)
    
    diffs = dict()
    for a in losts:
        try:
            v,vv = values.get(a,(None,))[1],reads[a]
            if fn.isSequence(v): v = fn.toList(v)
            if fn.isSequence(vv): vv = fn.toList(vv)
            diffs[a] = v!=vv
            if fn.isSequence(diffs[a]):
                diffs[a] = any(diffs[a])
            else:
                diffs[a] = bool(diffs[a])
        except:
            diffs[a] = None
        
    fams = fn.defaultdict(list)
    for a in tnotupread:
        fams['/'.join(a.split('/')[-4:-2])].append(a)
    for f in sorted(fams):
        print('\t%s: %d attrs not updated' % (f,len(fams[f])))
        
    print()
    
    differ = [a for a in losts if diffs[a]] #is True]
    print('\t%d/%d not updated attrs have also wrong values!!!' 
          % (len(differ),len(losts)))

    rd = pta.Reader()
    only = [a for a in tnotupread if len(rd.is_attribute_archived(a))==1]
    print('\t%d/%d not updated attrs are archived only in %s' 
          % (len(only),len(losts),schema))
    result.LostDiff = differ
    print()   
        
    archs = sorted(set(archived.values()))
    astor = fn.Astor()
    astor.load_from_devs_list(archs)
    badloads = fn.defaultdict(list)
    for k,s in astor.items():
        for d in s.get_device_list():
            d = fn.tango.get_normal_name(d)
            for a in losts:
                if fn.tango.get_normal_name(archived[a]) == d:
                    badloads[k].append(a)
    for k,s in badloads.items():
        if len(s):
            print('\t%s archives %d lost attributes'%(k,len(s)))
        
    print('\t%d updated attrs are not readable' % len(tupnoread))    
    
    result.ArchivedAndReadable = tok
    result.Updated = tup
    result.NotUpdated = tnotup
    result.Unreadable = tnotupnotread
    #result.DeviceNotRunning = nodevs
    result.ArchiverNotRunning = noarchs

    result.LostFamilies = fams
    

    # Tnones is for readable attributes not being archived
    tnones = [a for a in archived if (
        a not in values or values[a] and values[a][1] in (None,[]))
        and a not in tupnoread and a not in tnotupread]
    tupnones = [a for a in tnones if a in tup]

    if tupnones:
        print('\t%d archived readable attrs record empty values' % len(tupnones))
        
    result.Nones = tnones
    
    if 0:
        
        get_ratio = lambda a,b:float(len(a))/float(len(b))
        
        #result.ArchRatio = get_ratio([t for t in readarch if t not in tnotup],readarch)
        #result.ReadRatio = get_ratio(result.Readable,tattrs)
        #result.LostRatio = get_ratio([a for a in tread if a in tnotup],tread)
        #result.MissRatio = get_ratio([a for a in tread if a not in tarch],tread)
        #result.OkRatio = 1.0-result.LostRatio-result.MissRatio
        
        #result.Summary = '\n'.join((
            #('Checking archiving of %s attributes'%(len(attributes) if attributes else schema))
            #,('%d attributes in %s, %d are currently active'%(len(api),schema,len(tarch)))
            #,('%d devices with %d archived attributes are not running'%(len(nodevs),len([a for a in api if a.rsplit('/',1) in nodevs])))
            #,('%d archived attributes (%2.1f %%) are unreadable! (check and remove)'%(len(tnoread),1e2*get_ratio(tnoread,tarch)))
            #,('%d readable attributes are not archived'%(len(tmiss)))
            #,('%d attributes (readable or not) are updated (%2.1f %% of all readables)'%(len(tok),1e2*result.OkRatio))
            #,('-'*80)
            #,('%d archived attributes (readable or not) are not updated!'%len(tnotup))
            #,('%d archived and readable attributes are not updated! (check and restart?)'%len(treadnotup))
            #,('-'*80)
            #,('%d readable attributes have been removed in the last %d days!'%(len(removed),old_period/(24*3600)))
            #,('%d readable scalar attributes are not being archived (not needed anymore?)'%len(tmscalar))
            #,('%d readable array attributes are not being archived (Ok)'%len(tmarray))
            #,('%d readable array attributes are archived (Expensive)'%len(tarray))
            #,('')))
        
        #if trace: print(result.Summary)
        #print('%d readable lost,Ok = %2.1f%%, %2.1f %% over all Readables (%2.1f %% of total)'%\
            #(len(treadnotup),1e2*result.ArchRatio,1e2*result.OkRatio,1e2*result.ReadRatio))

    if action:
        if action == 'start_devices':
            print('Executing action %s' % action)
            api.start_devices()
            
        if action == 'restart_all':
            print('Executing action %s' % action)
            devs = api.get_archivers()
            astor = fn.Astor()
            print('Restarting %d devs:' % (len(devs),devs))
            astor.load_from_devs_list(devs)
            astor.stop_servers()
            fn.wait(10.)
            astor.start_servers()
            
        #print('NO ACTIONS ARE GONNA BE EXECUTED, AS THESE ARE ONLY RECOMMENDATIONS')
        #print("""
        #api = PyTangoArchiving.HDBpp(schema)
        #api.start_devices()
        
        #or  
            
        #api = PyTangoArchiving.ArchivingAPI('%s')
        #lostdevs = sorted(set(api[a].archiver for a in result.NotUpdated))
        #print(lostdevs)
        #if lostdevs < a_reasonable_number:
          #astor = fn.Astor()
          #astor.load_from_devs_list(lostdevs)
          #astor.stop_servers()
          #fn.time.sleep(10.)
          #astor.start_servers()
        #"""%schema)
        
    print('\nfinished in %d seconds\n\n'%(fn.now()-ti))
    
    if export is not None:
        if export is True:
            export = 'txt'
        for x in (export.split(',') if isString(export) else export):
            if x in ('json','pck','pickle','txt'):
                x = '/tmp/%s.%s' % (schema,x)
            print('Saving %s file with keys:\n%s' % (x,result.keys()))
            if 'json' in x:
                fn.dict2json(result.dict(),x)
            else:
                f = open(x,'w')
                if 'pck' in x or 'pickle' in x:
                    pickle.dump(result.dict(),f)
                else:
                    f.write(fn.dict2str(result.dict()))
                f.close()
        
    return result 

import PyTangoArchiving as pta, fandango as fn

def check_db_schema(schema,tref = None):
    
    r = fn.Struct()
    r.api = api = pta.api(schema)
    r.tref = fn.notNone(tref,fn.now()-3600)
    
    r.attrs = api.keys()
    r.on = api.get_archived_attributes()
    r.off = [a for a in r.attrs if a not in r.on]
    if schema in ('tdb','hdb'):
        ups = api.db.get_table_updates()
        r.vals = dict((k,(ups[api[k].table],None)) for k in r.on)
    else:
        r.vals = dict(fn.kmap(api.load_last_values,r.on))
        r.vals = dict((k,v and v.values()[0]) for k,v in r.vals.items())

    # Get all updated attributes
    r.ok = [a for a,v in r.vals.items() if v and v[0] > r.tref]
    # Try to read not-updated attributes
    r.check = dict((a,fn.check_attribute(a)) for a in r.on if a not in r.ok)
    r.nok, r.stall, r.noev, r.lost, r.evs = [],[],[],[],{}
    # Method to compare numpy values
    fbool = lambda x: all(x) if fn.isSequence(x) else bool(x)
    
    for a,v in r.check.items():
        # Get current value/timestamp
        vv,t = getattr(v,'value',v),getattr(v,'time',0)
        t = t and fn.ctime2time(t)
        
        if isinstance(vv,(type(None),Exception)):
            # attribute is not readable
            r.nok.append(a)
        elif r.vals[a] and 0<t<=r.vals[a][0]:
            # attribute timestamp doesnt change
            r.stall.append(a)
        elif r.vals[a] and fbool(vv==r.vals[a][1]):
            # attribute value doesnt change
            r.stall.append(a)
        else:
            r.evs[a] = fn.tango.check_attribute_events(a)
            if not r.evs[a]:
                # attribute doesnt send events
                r.noev.append(a)
            else:
                # archiving failure (events or polling)
                r.lost.append(a)
                
    # SUMMARY
    print(schema)
    for k in 'attrs on off ok nok noev stall lost'.split():
        print('\t%s:\t:%d' % (k,len(r.get(k))))
                
    return r


def save_schema_values(schema, filename='', folder=''):
    t0 = fn.now()
    print('Saving %s attribute values' % schema)
    filename = filename or '%s_values.pck' % schema
    if folder: filename = '/'.join(folder,filename)
    api = pta.api(schema)
    attrs = api.keys() if hasattr(api,'keys') else api.get_attributes()
    print('%d attributes in %s' % (len(attrs),schema))
    values = dict.fromkeys(filter(api.is_attribute_archived,attrs))
    print('%d attributes archived' % (len(values)))
    values.update((a,api.load_last_values(a)) for a in values.keys())
    pickle.dump(values,open(filename,'w'))
    print('%s written, %d seconds ellapsed' % (filename,fn.now()-t0))
    print(os.system('ls -lah %s' % filename))

if __name__ == '__main__':
    
    args = {}
    try:
        assert sys.argv[2:]
        args = fn.sysargs_to_dict(defaults=('schema','period','ti',
                                            'values','action'))
        print(args)

        if args.get('action') == 'start':
            print('Call Start() for %s devices' % sys.argv[1])
            pta.api(args['schema']).start_devices(force=True)
            print('done')
        if args.get('action') == 'restart':
            print('Restart %s servers' % sys.argv[1])
            pta.api(args['schema']).start_servers(restart=True)
            print('done') 
        if args.get('action') == 'save':
            save_schema_values(args['schema'],
                    filename=args.get('filename',''),
                    folder=args.get('folder',''))
        else:
            try:
                args = dict((k,v) for k,v in args.items() 
                            if k and v not in (False,[]))
                r = check_archiving_schema(**args);
            except:
                print(fn.except2str())

    except:
        print fn.except2str()
        print(USAGE)
