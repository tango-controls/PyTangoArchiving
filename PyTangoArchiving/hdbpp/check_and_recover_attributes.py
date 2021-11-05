import PyTangoArchiving as pta, fandango as fn, PyTangoArchiving.hdbpp.maintenance as ptam
import traceback

#dbs = ['hdbacc','hdbct','hdbdi','hdbpc','hdbrf','hdbvc']
dbs = pta.get_hdbpp_databases()

checks = dict((d,pta.check.check_db_schema(d,subscribe=False)) for d in dbs)

for db in dbs:
    check,api = checks[db],checks[db].api
    print('>'*80)
    print('\nrecovering %d lost attributes from %s\n' % (len(check.lost),db))
    
    perlost = [a for a in check.lost if api.is_periodic_archived(a)]
    evlost = [a for a in check.lost if not api.is_periodic_archived(a)]

    errors = [a for a in evlost if api.get_attribute_errors(a)]
    recover = [a for a in errors if fn.tango.check_attribute_events(a)]

    failed = []
    for a in evlost:
        print('recovering %s' % a)
        if a in errors and a not in recover:
            print('%s not recoverable' % a)
            continue
        try:
            d = api.get_attribute_subscriber(a)
            dp = fn.get_device(d)
            dp.AttributeStop(a)
            fn.wait(0.5)
            dp.AttributeStart(a)
        except:
            failed.append(a)
            print(a,d,traceback.format_exc())

    periods = dict((a,api.get_periodic_attribute_period(a)) for a in perlost)
            
    for per in api.get_periodic_archivers():
        perattrs = api.get_periodic_archiver_attributes(per)
        if len([a for a in perattrs if a in perlost]) > 0.3*len(perattrs):
            fn.Astor(per).stop_servers()
            fn.wait(5.)
            fn.Astor(per).start_servers()
        else:
            for attr in [p for p in perattrs if p in perlost]:
                period = periods[attr]
                print('recovering %s' % attr)
                try:
                    d = api.get_periodic_attribute_archiver(attr)
                    dp = fn.get_device(d)
                    dp.AttributeRemove(attr)
                    fn.wait(.5)
                    dp.AttributeAdd([attr,str(int(period))])
                    fn.wait(.5)
                    print('%s done' % attr)
                except:
                    failed.append(attr)
                    print(attr,d,traceback.format_exc())

    print('attributes not recoverable: %s' % str([a for a in errors if a not in recover]))
    print('attributes failed: %s' % str(failed))
    
