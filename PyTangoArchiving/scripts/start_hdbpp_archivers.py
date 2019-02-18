#!/usr/bin/env python

import fandango as fn

devs = fn.tango.get_class_devices('HdbEventSubscriber')

for d in devs:
    try:
        if not fn.check_device(d):
            fn.Astor(d).stop_servers()
            fn.Astor(d).start_servers()
        else:
            # Wait to next iteration before setting polling
            dp = fn.get_device(d)
            dp.poll_command('start',1200000)
            print(d,'done')
    except:
        print(fn.getLastException())
