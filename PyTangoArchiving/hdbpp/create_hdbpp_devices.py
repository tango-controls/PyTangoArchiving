import sys, traceback
import fandango as fn
import PyTangoArchiving as pta

__doc__="""
Usage:

python create_hdbpp_devices.py db_name host user passwd <number of archivers>

"""

try:
    db_name = sys.argv[1]
    host = sys.argv[2]    
    user = sys.argv[3]
    passwd = sys.argv[4]
    n_arch = int(sys.argv[5])
    if sys.argv[6:]:
        lib_location = sys.argv[6]
    else:
        lib_location = '/usr/lib/x86_64-linux-gnu/libhdb++mysql.so.6'    

    schemas = fn.tango.get_free_property('PyTangoArchiving','DbSchemas')
    schemas  = [] if not len(schemas) else fn.toList(schemas)
    if db_name not in schemas:
        fn.tango.put_free_property('PyTangoArchiving','DbSchemas',
                                   schemas+[db_name])

    config = fn.tango.get_free_property('PyTangoArchiving',db_name)
    if not config:
        print('Creating PyTangoArchiving.hdbpp property')
        fn.tango.put_free_property('PyTangoArchiving',db_name,[
            "reader=PyTangoArchiving.hdbpp.HDBpp('%s','%s','%s','%s')" % (
                db_name,host,user,passwd),
            "api=PyTangoArchiving.hdbpp.HDBpp",
            "check=True",
            "method=get_attribute_values",
            'db_name=%s'%db_name,
            'user=%s'%user,
            'passwd=%s'%passwd,
            'host=%s'%host,
            'libname=%s'%lib_location,
            ])
            
    print(pta.Schemas.getSchema(db_name))
    devs = map(fn.tango.get_normal_name,fn.find_devices('archiving/%s/*' % db_name))

    api = pta.api(db_name) 
    cm = 'archiving/%s/cm-01'%db_name    
    if cm not in devs:
        print('Creating %s archiving manager' % cm)
        api.add_archiving_manager('%s'%db_name,cm) 
        
    astor = fn.Astor('archiving/%s/cm-01'%db_name) 
    [astor.set_server_level(s,host,6) for s in astor.keys()] 
    astor.start_servers() 
    fn.wait(3.)
    print(api.get_manager())
    
    fn.tango.put_class_property('HdbEventSubscriber','DefaultContext','RUN')
    fn.tango.put_class_property('HdbEventSubscriber','StartArchivingAtStartup','True')
    fn.tango.put_class_property('HdbEventSubscriber','SubscribeChangeAsFallback','True')
    
    print('Creating event subscribers')
    for i in range(1,n_arch+1):
        es = 'archiving/%s/es-%02d'%(db_name,i)
        if es not in devs:
            api.add_event_subscriber(db_name+'-%02d'%i,es)

    print('Creating periodic archivers')
    for i in range(1,n_arch+1):
        per = 'archiving/%s/per-%02d'%(db_name,i)
        if per not in devs:
            api.add_periodic_archiver(db_name+'-%02d'%i,per)
        
    print('starting devices')
    astor = fn.Astor('archiving/%s/*' % db_name) 
    [astor.set_server_level(s,host,3) for s in astor.keys() if 'event' in s.lower()] 
    [astor.set_server_level(s,host,4) for s in astor.keys() if 'period' in s.lower()] 
    astor.start_servers(host=host)
    for t in astor.states().items():
        print(t)

except:
    traceback.print_exc()
    print(__doc__)
