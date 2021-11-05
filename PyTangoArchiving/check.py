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

def main(args=None):
    """
    see PyTangoArchiving.check.USAGE
    """
    try:
        import argparse
        parser = argparse.ArgumentParser() #usage=USAGE)
        parser.add_argument('schema')
        #parser.add_argument('--period',type=int)
        parser.add_argument('--tref',type=str,default='-43200',
            help = 'min epoch considered ok')
        parser.add_argument('--action',type=str,default='check',
            help = 'start|restart|save|check')
        parser.add_argument('--export',help = 'json|pickle',default='json')
        parser.add_argument('--values',type=str,
            help = 'values file, will be loaded by load_schema_values()')
        
        try:
            args = dict(parser.parse_args().__dict__)
        except:
            sys.exit(-1)
        
        #if not args:
            #args = {}
            #assert sys.argv[2:]
            #args = fn.sysargs_to_dict(defaults=('schema','period','ti',
                #'values','action','folder'))
        #print(args)

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
                args.pop('action')
                args = dict((k,v) for k,v in args.items() 
                            if k and v not in (False,[]))
                print(args)
                r = check_db_schema(**args)
            except:
                print(fn.except2str())

    except SystemExit:
        pass
    except:
        print fn.except2str()
        #print(USAGE)

###############################################################################

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
        export=None,
        n = 1):
    
    raise Exception('deprecated!, use check_db_schema')

    ti = fn.now() if ti is None else str2time(ti) if isString(ti) else ti
    api = pta.api(schema)
    is_hpp = isinstance(api,pta.HDBpp)
    attributes = list(attributes)
    values = dict(values)
    
    check = dict()
    old_period = 24*3600*old_period if old_period < 1000 \
        else (24*old_period if old_period<3600 else old_period)
    
    allattrs = api.get_attributes() if hasattr(api,'get_attributes') \
        else api.keys()
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
    
    ###########################################################################

    values = load_schema_values(schema,attributes,values,n)
    
    ###########################################################################    
    
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
            print('\t%s archives %d attrs (last at %s)' 
                % (k,len(s),fn.time2str(max(values[a][0] 
                    for a in loads[k] if values[a]))))
    
    noarchs = [fn.tango.get_normal_name(d) for d in archs 
               if not fn.check_device(d)]
    if noarchs:
        print('\t%d archivers are not running: %s' % (len(noarchs),noarchs))
    
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
        print('\t%d/%d attrs with values are not archived anymore' % 
              (len(tmiss),len(tattrs)))
        
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
    tupnoread = [a for a in tup if values[a][1] is None 
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
    print('\t%d not updated attrs are readable and have events (LostEvents)' 
          % len(tnotupevs))    
    
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
            
    differ = [a for a in losts if diffs[a]] #is True]
    print('\t%d/%d not updated attrs have also wrong values!!!' 
          % (len(differ),len(losts)))     
    result.LostDiff = differ    
        
    print('\n')
    fams = fn.defaultdict(list)
    for a in tnotupread:
        fams['/'.join(a.split('/')[-4:-2])].append(a)
    for f in sorted(fams):
        print('\t%s: %d attrs not updated' % (f,len(fams[f])))
        
    print('-'*80)
    rd = pta.Reader()
    only = [a for a in tnotupread if len(rd.is_attribute_archived(a))==1]
    print('\t%d/%d not updated attrs are archived only in %s' 
          % (len(only),len(losts),schema))
    print()   
    print('-'*80)        
    archs = sorted(set(archived.values()))
    astor = fn.Astor()
    astor.load_from_devs_list(archs)
    badloads = fn.defaultdict(list)
    for k,s in astor.items():
        for d in s.get_device_list():
            d = fn.tango.get_normal_name(d)
            for a in losts:
                try:
                    if fn.tango.get_normal_name(archived[a]) == d:
                        badloads[k].append(a)
                except:
                    traceback.print_exc()
                    
    for k,s in sorted(badloads.items()):
        if len(s):
            t = loads and len(loads[k]) or len(s)
            print('\t%s archives %d/%d lost attributes (%f)'%
                  (k,len(s),t,float(len(s))/t))
        
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
            
        #print('NO ACTIONS ARE GONNA BE EXECUTED, AS THESE ARE ONLY 
        # RECOMMENDATIONS')
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

def load_schema_values(schema, attributes=None, values=None, n=1, tref=None):
    
    api = schema if not isString(schema) else pta.api(schema)
    
    if isString(values) and values.endswith('.pck'):
        print('\nLoading last values from %s file\n' % values)
        import pickle
        values = pickle.load(open(values))
        
    elif isString(values) and values.endswith('.json'):
        print('\nLoading last values from %s file\n' % values)
        values = fn.json2dict(values)        

    elif values is None: #if not use_index or is_hpp:

        print('\nGetting last values ...\n')
        if n==1 and not isinstance(api,pta.HDBpp):
            ups = api.db.get_table_updates()
            values = dict((k,(ups[api[k].table],None)) for k in attributes)
        else:
            value = {}
            values = dict((a,api.load_last_values(a,n=n,tref=tref))
                          for a in attributes)
            
    values = values.get('values',values)
            
    for k,v in values.items():
        # each value can be either a tuple (t,v), a list of t,v values or None
        if isinstance(v,dict): 
            v = v.values()[0]
        if not (len(v) if isSequence(v) else v):
            values[k] = [] if n>1 else None
        else:
            if n > 1:
                v = v[0]
            if v and not isNumber(v[0]):
                try:
                    v = [date2time(v[0]),v[1]]
                except:
                    print('unable to parse %s' % str(v))
            values[k] = v
                
    print('%d values obtained' % len(values))
    
    return values
    

CheckState = fn.Struct(
    ON = 0, # archived
    OFF = 1, # not archived
    OK = 2, # device up and running, values updated
    NO_READ = 3, # device not running
    STALL = 4, # value not changing
    NO_EVENTS = 5, # not sending events
    LOST = 6, # value changed, but not updated in db
    UNK = 7, # value cannot be evaluated
    )

def check_attribute_exists(model):
    model = fn.tango.parse_tango_model(model)
    alldevs = fn.tango.get_all_devices()
    device = fn.tango.get_normal_name(model.device)
    if device not in alldevs:
        return False
    #alldevs = fn.tango.get_all_devices(exported=True)
    #if not device in alldevs:
        #return True
    if not fn.tango.check_device(device):
        return True
    return bool(fn.find_attributes(model.normalname))

def check_db_schema(schema, attributes = None, values = None,
                    tref = -12*3600, n = 1, filters = '*', export = 'json',
                    restart = False, subscribe = False):
    """
    tref is the time that is considered updated (e.g. now()-86400)
    n is used to consider multiple values
    
    attrs: all attributes in db
    on: archived
    off: in db but not currently archived
    ok: updated   
    
    known error causes (attrs not lost but not updated):
    
    nok: attributes are not currently readable
    noevs: attributes not sending events
    novals: attributes never recorded a value
    stall: not updated, but current value matches archiving
    lost: not updated, and values doesn't match with current
    """
    
    t0 = fn.now()
    if hasattr(schema,'schema'):
        api,schema = schema,api.schema
    else:
        api = pta.api(schema)

    r = fn.Struct(api=api,schema=schema)    
    if isString(tref): 
        tref = fn.str2time(tref)
    r.tref = fn.now()+tref if tref < 0 else tref
    r.attrs = [a for a in (attributes or api.get_attributes())
                if fn.clmatch(filters,a)]
    print('check_db_schema(%s,attrs[%s],tref="%s",export as %s)' 
          % (schema,len(r.attrs),fn.time2str(r.tref),export))
    
    if restart and schema!='hdbpc':
        archs = [a for a in api.get_archivers() if not fn.check_device(a)]
        if archs:
            try:
                print('Restarting archivers: %s' % str(archs))
                astor = fn.Astor(archs)
                astor.stop_servers()
                astor.start_servers()
            except:
                traceback.print_exc()
        
        stopped = api.get_stopped_attributes()
        print('Restarting %d stopped attributes' % len(stopped))
        api.restart_attributes(stopped)
    
    r.on = [a for a in api.get_archived_attributes() if a in r.attrs]
    r.off = [a for a in r.attrs if a not in r.on]
    
    r.archs = fn.defaultdict(list)
    r.pers = fn.defaultdict(list)
    r.values = load_schema_values(api,r.on,values,n,tref=tref)
    
    if schema in ('tdb','hdb'):
        [r.archs[api[k].archiver].append(k) for k in r.on]
    else:
        r.rvals = r.values
        r.freq, r.values = {}, {}
        for k,v in r.rvals.items():
            try:
                if n > 1:
                    v = v[0] if isSequence(v) and len(v) else v
                    r.values[k] = v[0] if isSequence(v) and len(v) else v
                    r.freq[k] = v and float(len(v))/abs(v[0][0]-v[-1][0])
                else:
                    r.values[k] = v
            except Exception as e:
                print(k,v)
                print(fn.except2str())
                
        for k in api.get_archivers():
            r.archs[k] = api.get_archiver_attributes(k)
        for k in api.get_periodic_archivers():
            r.pers[k] = api.get_periodic_archivers_attributes(k)

    # Get all updated attributes
    r.ok = [a for a,v in r.values.items() if v and v[0] > r.tref]
    # Try to read not-updated attributes
    r.check = dict((a,fn.check_attribute(a)
                    ) for a in r.on if a not in r.ok)
    #r.novals = [a for a,v in r.values.items() if not v]
    r.nok, r.stall, r.noevs, r.lost, r.novals, r.evs, r.rem = [],[],[],[],[],{},[]
    # Method to compare numpy values
    
    for a,v in r.check.items():
        state = check_archived_attribute(a, v, default=CheckState.LOST, 
            cache=r, tref=r.tref, 
            check_events = subscribe and not api.is_periodic_archived(a))
        {
            #CheckState.ON : r.on,
            #CheckState.OFF : r.off,
            CheckState.OK : r.ok, #Shouldn't be any ok in check list               
            CheckState.NO_READ : r.nok,
            CheckState.STALL : r.stall,
            CheckState.NO_EVENTS : r.noevs,
            CheckState.LOST : r.lost,
            CheckState.UNK : r.novals,
         }[state].append(a)
                
    # SUMMARY
    r.summary = schema +'\n'
    r.summary += ','.join(
        """on: archived
        off: not archived
        ok: updated   
        nok: not readable
        noevs: no events
        novals: no values
        stall: not changing
        lost: not updated
        """.split('\n'))+'\n'
    
    getline = lambda k,v,l: '\t%s:\t:%d\t(%s)' % (k,len(v),l)
    
    r.summary += '\n\t%s:\t:%d\tok+stall: %2.1f %%' % (
        'attrs',len(r.attrs),
        (100.*(len(r.ok)+len(r.stall))/(len(r.on) or 1e12)))
    r.summary += '\n\t%s/%s:\t:%d/%d' % (
        'on','off',len(r.on),len(r.off))
    #if r.off > 20: r.summary+=' !!!'
    r.summary += '\n\t%s/%s:\t:%d/%d' % (
        'ok','nok',len(r.ok),len(r.nok))
    if len(r.nok) > 10: 
        r.summary+=' !!!'
    r.summary += '\n\t%s/%s:\t:%d/%d' % (
        'noevs','novals',len(r.noevs),len(r.novals))
    if len(r.novals) > 1: 
        r.summary+=' !!!'
    r.summary += '\n\t%s/%s:\t:%d/%d' % (
        'lost','stall',len(r.lost),len(r.stall))
    if len(r.lost) > 1: 
        r.summary+=' !!!'
    r.summary += '\n'
        
    r.archivers = dict.fromkeys(api.get_archivers())
    for d in sorted(r.archivers):
        r.archivers[d] = api.get_archiver_attributes(d)
        novals = [a for a in r.archivers[d] if a in r.novals]   
        lost = [a for a in r.archivers[d] if a in r.lost]
        if (len(novals)+len(lost)) > 2:
            r.summary += ('\n%s (all/novals/lost): %s/%s/%s' 
                % (d,len(r.archivers[d]),len(novals),len(lost)))
            
    if hasattr(api,'get_periodic_archivers'):
        r.periodics = dict.fromkeys(api.get_periodic_archivers())
        for d in sorted(r.periodics):
            r.periodics[d] = api.get_periodic_archiver_attributes(d)
            novals = [a for a in r.periodics[d] if a in r.novals]
            lost = [a for a in r.periodics[d] if a in r.lost]
            if len(novals)+len(lost) > 2:
                r.summary += ('\n%s (all/novals/lost): %s/%s/%s' % 
                    (d,len(r.periodics[d]),len(novals),len(lost)))
        
        r.perattrs = [a for a in r.on if a in api.get_periodic_attributes()]
        r.notper = [a for a in r.on if a not in r.perattrs]
        
        
    r.summary += '\nfinished in %d seconds\n\n'%(fn.now()-t0)
    print(r.summary)
    
    if restart:
        try:
            retries = r.lost+r.novals+r.nok
            print('restarting %d attributes' % len(retries))
            api.restart_attributes(retries)
        except:
            traceback.print_exc()
    
    if export is not None:
        if export is True:
            export = 'txt'
        for x in (export.split(',') if isString(export) else export):
            if x in ('json','pck','pickle','txt'):
                x = '/tmp/%s.%s' % (schema,x)
            print('Saving %s file with keys:\n%s' % (x,r.keys()))
            if 'json' in x:
                fn.dict2json(r.dict(),x)
            else:
                f = open(x,'w')
                if 'pck' in x or 'pickle' in x:
                    pickle.dump(r.dict(),f)
                else:
                    f.write(fn.dict2str(r.dict()))
                f.close()     
                
    for k,v in r.items():
        if fn.isSequence(v):
            r[k] = sorted(v)
                
    return r

def check_archived_attribute(attribute, value = False, state = CheckState.OK, 
        default = CheckState.LOST, cache = None, tref = None, 
        check_events = True):
    """
    generic method to check the state of an attribute (readability/events)
    
    value = AttrValue object returned by check_attribute
    cache = result from check_db_schema containing archived values
    
    this method will not query the database; database values should be 
    given using the chache dictionary argument
    """
    # readable and/or no reason known for archiving failure
    state = default # do not remove this line
    
    # Get current value/timestamp
    if cache:
        stored = cache.values[attribute]
        #evs = cache.evs[attribute]
        if stored is None or (fn.isSequence(stored) and not len(stored)):
            return CheckState.UNK
        else:
            t,v = stored[0],stored[1]
            if t>=tref and not isinstance(v,(type(None),Exception)):
                print('%s should not be in check list! (%s,%s)' % (attribute,t,v))
                return CheckState.OK
        
    if value is False:
        value = fn.check_attribute(attribute, brief=False)
        
    vv,t = getattr(value,'value',value),getattr(value,'time',0)
    t = t and fn.ctime2time(t)
    
    if isinstance(vv,(type(None),Exception)):
        # attribute is not readable
        state = CheckState.NO_READ
    elif cache and stored and 0 < t <= stored[0]:
        # attribute timestamp doesnt change
        state = CheckState.STALL
    elif cache and stored and fbool(vv == stored[1]):
        # attribute value doesnt change
        state = CheckState.STALL
    elif check_events:
        # READABLE NOT STORED WILL ARRIVE HERE
        evs = fn.tango.check_attribute_events(attribute)
        if cache:
            cache.evs[attribute] = evs
        if not evs:
            # attribute doesnt send events
            state = CheckState.NO_EVENTS

    return state

def check_table_data(db, att_id, table, start, stop, gap, period):
    """
    db must be a fandango.FriendlyDB object
    
    NOTE: count(*) seems to be a very unefficient method to do this!!
    
    this method will check different intervals within the table to 
    see whether there is available data or not for the attribute
    
    start/stop must be epoch times
    """
    cols = db.getTableCols(table)
    if 'att_conf_id' in cols:
        query = ("select count(*) from %s where att_conf_id = %s and "
            "data_time between " % (table, att_id))
    else:
        query = ("select count(*) from %s where "
            "time between " % (table))
    
    tend = start + period
    while tend < stop:
        tq = '"%s" and "%s"' % (fn.time2str(start),fn.time2str(tend))
        try:
            r = db.Query(query + ' ' + tq)
            print('%s:%s' % (tq, r[0][0]))
        except:
            traceback.print_exc()
            break
            print('%s: failed' % tq)
        start, tend = start+gap, tend+gap
        
    return


def save_schema_values(schema, filename='', folder=''):
    """
    This method saves all last values from a given schema into a file
    it can be called from crontab to generate daily reports
    """
    t0 = fn.now()
    print('Saving %s attribute values' % schema)
    date = fn.time2str().split()[0].replace('-','')
    filename = filename or '%s_%s_values.pck' % (schema,date)
    if folder: 
        filename = '/'.join((folder,filename))

    api = pta.api(schema)
    attrs = api.keys() if hasattr(api,'keys') else api.get_attributes()
    print('%d attributes in %s' % (len(attrs),schema))
    values = dict.fromkeys(filter(api.is_attribute_archived,attrs))
    print('%d attributes archived' % (len(values)))
    values.update((a,api.load_last_values(a)) for a in values.keys())
    pickle.dump(values,open(filename,'w'))

    print('%s written, %d seconds ellapsed' % (filename,fn.now()-t0))
    print(os.system('ls -lah %s' % filename))

##############################################################################

if __name__ == '__main__':
    
    main()
    
