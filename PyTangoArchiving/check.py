def check_archiving_performance(schema='hdb',attributes=[],period=24*3600*90,\
    exclude=['*/waveid','*/wavename','*/elotech-*'],action=False,trace=True):
    import PyTangoArchiving as pta
    import fandango as fn

    ti = fn.now()
    api = pta.api(schema)
    check = dict()
    period = 24*3600*period if period < 1000 else (24*period if period<3600 else period)
    attributes = fn.get_matching_attributes(attributes) if fn.isString(attributes) else map(str.lower,attributes)
    tattrs = [a for a in api if not attributes or a in attributes]
    excluded = [a for a in tattrs if any(fn.clmatch(e,a) for e in exclude)]
    tattrs = [a for a in tattrs if a not in excluded]

    #Getting Tango devices currently not running
    alldevs = set(t.rsplit('/',1)[0] for t in tattrs if api[t].archiver)
    tdevs = filter(fn.check_device,alldevs)
    nodevs = [d for d in alldevs if d not in tdevs]

    #Updating data from archiving config tables
    if not attributes:
      tattrs = sorted(a for a in api if a.rsplit('/',1)[0] in tdevs)
      tattrs = [a for a in tattrs if not any(fn.clmatch(e,a) for e in exclude)]
    print('%d attributes will not be checked (excluded or device not running)'%(len(api)-len(tattrs)))
    
    tarch = sorted(a for a in api if api[a].archiver)
    tnoread = sorted(t for t in tarch if t not in tattrs)
    check.update((t,None) for t in tnoread)

    #Getting attributes archived in the past and not currently active
    tmiss = [t for t in tattrs if not api[t].archiver]
    check.update((t,fn.check_attribute(t,readable=True)) for t in tmiss)
    tmiss = [t for t in tmiss if check[t]]
    tmarray = [t for t in tmiss if fn.isString(check[t].value) or fn.isSequence(check[t].value)]
    tmscalar = [t for t in tmiss if t not in tmarray]
    
    #Getting updated tables from database
    tups = pta.utils.get_table_updates(schema)
    # Some tables do not update MySQL index tables
    t0 = [a for a in tarch if a in tattrs and not tups[api[a].table]]
    check.update((t,check_attribute(a,readable=True)) for t in t0 if not check.get(t))
    t0 = [t for t in t0 if check[t]]
    print('%d/%d archived attributes have indexes not updated ...'%(len(t0),len(tarch)))
    if t0 and len(t0)<100: 
      vs = api.load_last_values(t0);
      tups.update((api[t].table,api[t].last_date) for t in t0)
    tnotup = [a for a in tarch if tups[api[a].table]<fn.now()-1800]
    check.update((t,1) for t in tarch if t not in tnotup)
    
    #Updating readable attributes (all updated are considered as readable)
    tread = sorted(t for t in tattrs if t not in tnoread)
    for t in tattrs:
      if t not in check:
        check[t] = fn.check_attribute(t,readable=True)
    tread = sorted(t for t in tattrs if check[t])
    tnoread.extend(t for t in tread if not check[t])
    tnoread = sorted(set(tnoread))
          
    #tread contains all readable attributes from devices with some attribute archived
    #tnoread contains all unreadable attributes from already archived

    #Calcullating all final stats
    #tok will be all archivable attributes that are archived
    #tnotup = [a for a in tnotup if check[a]]
    #tok = [t for t in tread if t in tarch and t not in tnotup]
    tok = [t for t in tarch if t not in tnotup]
    readarch = [a for a in tread if a in tarch]
    treadnotup = [t for t in readarch if t in tnotup] #tnotup contains only data from tarch
    tokread = [t for t in readarch if t not in tnotup] #Useless, all archived are considered readable
    tarray = [t for t in tarch if check[t] and get_attribute_pytype(t) in (str,list)]
    removed = [a for a in tattrs if not api[a].archiver and tups[api[a].table]>fn.now()-period]
    
    result = fn.Struct()
    result.Excluded = excluded
    result.Schema = schema
    result.All = api.keys()
    result.Archived = tarch
    result.Readable = tread
    result.ArchivedAndReadable = readarch
    result.Updated = tok #tokread
    result.Lost = treadnotup
    result.Removed = removed
    result.TableUpdates = tups
    result.NotUpdated = tnotup
    result.Missing = tmiss
    result.MissingScalars = tmscalar
    result.MissingArrays = tmarray
    result.ArchivedArray = tarray
    result.Unreadable = tnoread
    result.DeviceNotRunning = nodevs
    
    get_ratio = lambda a,b:float(len(a))/float(len(b))
    
    result.ArchRatio = get_ratio([t for t in readarch if t not in tnotup],readarch)
    result.ReadRatio = get_ratio(result.Readable,tattrs)
    result.LostRatio = get_ratio([a for a in tread if a in tnotup],tread)
    result.MissRatio = get_ratio([a for a in tread if a not in tarch],tread)
    result.OkRatio = 1.0-result.LostRatio-result.MissRatio
    
    result.Summary = '\n'.join((
      ('Checking archiving of %s attributes'%(len(attributes) if attributes else schema))
      ,('%d attributes in %s, %d are currently active'%(len(api),schema,len(tarch)))
      ,('%d devices with %d archived attributes are not running'%(len(nodevs),len([a for a in api if a.rsplit('/',1) in nodevs])))
      ,('%d archived attributes (%2.1f %%) are unreadable! (check and remove)'%(len(tnoread),1e2*get_ratio(tnoread,tarch)))
      ,('%d readable attributes are not archived'%(len(tmiss)))
      ,('%d attributes (readable or not) are updated (%2.1f %% of all readables)'%(len(tok),1e2*result.OkRatio))
      ,('-'*80)
      ,('%d archived attributes (readable or not) are not updated!'%len(tnotup))
      ,('%d archived and readable attributes are not updated! (check and restart?)'%len(treadnotup))
      ,('-'*80)
      ,('%d readable attributes have been removed in the last %d days!'%(len(removed),period/(24*3600)))
      ,('%d readable scalar attributes are not being archived (not needed anymore?)'%len(tmscalar))
      ,('%d readable array attributes are not being archived (Ok)'%len(tmarray))
      ,('%d readable array attributes are archived (Expensive)'%len(tarray))
      ,('')))
    
    if trace: print(result.Summary)
    print('%d readable lost,Ok = %2.1f%%, %2.1f %% over all Readables (%2.1f %% of total)'%\
        (len(treadnotup),1e2*result.ArchRatio,1e2*result.OkRatio,1e2*result.ReadRatio))

    if action:
        print('NO ACTIONS ARE GONNA BE EXECUTED, AS THESE ARE ONLY RECOMMENDATIONS')
        print("""
        api = PyTangoArchiving.ArchivingAPI('%s')
        lostdevs = sorted(set(api[a].archiver for a in result.NotUpdated))
        print(lostdevs)
        if lostdevs < a_reasonable_number:
          astor = fn.Astor()
          astor.load_from_devs_list(lostdevs)
          astor.stop_servers()
          fn.time.sleep(10.)
          astor.start_servers()
        """%schema)
        
    if trace: print('finished in %d seconds'%(fn.now()-ti))
        
    return result 
