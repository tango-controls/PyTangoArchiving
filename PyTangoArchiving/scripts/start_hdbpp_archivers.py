#!/usr/bin/env python

import fandango as fn
import PyTangoArchiving as pta

import sys
dbs = sys.argv[1:] or pta.get_hdbpp_databases()

# empty output to be used from cron

for db in dbs:
    api = pta.api(db)
    devs = api.get_subscribers() 
    off = [d for d in devs if not fn.check_device(d)]
    if len(off):
        print('restart %s subscribers stopped in %s db' % (len(off),db))
        api.start_devices(dev_list=off,do_restart=True,force=False)
    
    devs = api.get_periodic_archivers() 
    poff = [d for d in devs if not fn.check_device(d)]
    if len(poff):
        print('restart %s pollers stopped in %s db' % (len(poff),db))
        api.start_devices(dev_list=poff,do_restart=True,force=False)
    
    stopped = api.get_stopped_attributes()
    if len(stopped):
        print('restart %s attributes stopped in %s db' % (len(stopped),db))
        for a in stopped:
            try:
                api.restart_attribute(a)
            except Exception as e:
                print(e)

