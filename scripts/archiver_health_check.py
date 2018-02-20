#!/usr/bin/python

import time,os,sys,traceback
sys.path.append('/homelocal/sicilia/lib/python/site-packages/')
import fandango as fun
from collections import defaultdict
import PyTangoArchiving as pta

V = fun.Struct({'torestart':[]})
WAIT_TIME = 150
MAX_ERROR = 7200
STOP_WAIT = 30.
last_restart = 0

TANGO_HOST = fun.tango.get_tango_host()
BL_FOLDER = '/beamlines/%s/controls/archiving'%(TANGO_HOST[1:5])
MACH_FOLDER = '/data/Archiving/Config'
LOG_FOLDER = '/homelocal/sicilia/var/' 

#'/intranet01mounts/controls/intranet/archiving'

def trace(msg):
  print('%s: %s' % (time.ctime(),msg))

def restart_server(ss,api='hdb',wait=WAIT_TIME):
  if fun.isString(api): api = pta.api('hdb')
  trace('restart_server(%s)'%ss)
  api.servers.stop_servers(ss)
  time.sleep(STOP_WAIT)
  api.servers.start_servers(ss,wait=0.1)
  last_restart = time.time()
  if wait: 
    print '\tWaiting %s seconds' %wait
    time.sleep(wait)
  return ss
    
def get_attributes_servers(attr_list,api='hdb',dedicated=False):
  if fun.isString(api): 
      api = pta.api(api)
  if dedicated:
    api.load_dedicated_archivers(check=False);
  
  devices = defaultdict(set)
  servers = defaultdict(set)
  
  [devices[x].add(l) for l in attr_list 
        for x in (api[l].archiver,api[l].dedicated) if x]
  #assigned = set(x for l in attr_list 
  #     for x in (api[l].archiver,api[l].dedicated) if x)
  [servers[api.servers.get_device_server(d).lower()].update(devices[d]) 
        for d in devices]
  #servers = sorted(set(api.servers.get_device_server(a) 
  #      for a in set(x for l in attr_list 
  #             for x in (api[l].archiver,api[l].dedicated) if x)))
  return servers

from PyTangoArchiving.utils import get_table_updates

def get_assigned_attributes(api='hdb',dedicated=False):
  if fun.isString(api): api = pta.api(api)
  if dedicated: api.load_dedicated_archivers(check=False);
  return sorted(set(a for a in api if api[a].archiver or dedicated and api[a].dedicated))

def get_deactivated_attributes(api='hdb',updates=None,period=6*30*24*3600):
  # Returns the list of attributes that are not archived despite readable and having data from the last months
  if fun.isString(api): api = pta.api(api)                                                                    
  if updates is None: updates = get_table_updates(api)                                                        
  now = fun.time.time()                                                                                       
  return sorted(a for a,t in updates.items() if (now-period)<t<(now-24*3600) and fun.check_attribute(a))  
  
def get_idle_servers(api='hdb'):
  idle = dict()
  if fun.isString(api): api = pta.api(api)
  for s,t in api.servers.items():
    if 'archiver' not in s: continue
    for d in t.get_device_list():
      if not fun.check_device(d):
        idle[s] = [d]
        break
  trace('\t%d servers have idle devices'%len(idle))  
  return idle
 
def get_csv_folder(tango_host=None):
  tango_host = tango_host or TANGO_HOST
  if 'bl' in tango_host:
      csvfolder = BL_FOLDER
  else:
      csvfolder = MACH_FOLDER
  return csvfolder
  
def get_all_configs(csvfolder=""):
    tango_host = fun.tango.get_tango_host()
    if tango_host.startswith('alba02'):
      return []
    csvfolder = csvfolder or get_csv_folder()
    return pta.GetConfigFiles(csvfolder,'.*.csv') 
    
def get_all_config_attrs(schema,csvfolder=""):
  configs  = get_all_configs(csvfolder)
  csv_ats = dict()
  for f in configs:
    try:
      print('loading %s.%s'%(f,schema))
      for k,v in pta.ParseCSV(f,schema).items():
        csv_atts[k] = v[schema]
    except:
      traceback.print_exc()
  return csv_ats
    
def check_config_files(schema,restart=False,save='',email='',csvfolder=""):
    api = pta.api(schema)
    csv_ats = get_all_config_attrs(schema,csvfolder)

    active = api.get_archived_attributes()
    missing = [a for a in csv_ats if a not in active]
    missread = [a for a in missing if fun.check_attribute(a)]
    msg = '%d CSV(%s) attributes missing in %s (%d readable)'%(len(missing),schema,fun.tango.get_tango_host(),len(missread))
    print msg
    txt = '\n'.join(sorted(missread))
    print txt
    try:
        if save:
            trace('Saving results in %s'%save)
            import pickle
            pickle.dump(missing,open(save,'w'))
        if email and missing:
            fun.linos.sendmail(msg,txt,email) 
    except:
        print traceback.format_exc()
    return missread
        
def minimal_check(schema,interval=3*3600,exclude=".*(wavename|waveid)$",csvs=""):
    import re,time
    from PyTangoArchiving import Reader
    from PyTangoArchiving.utils import check_attribute
    r = fun.Struct()
    rd = Reader(schema)
    db = rd.get_database()
    r.ids = db.get_attributes_IDs()
    r.active = db.get_attribute_names(active=True)
    r.shouldbe = get_all_config_attrs(schema,csvs) if csvs else r.active
    r.shouldbe = [a for a in r.shouldbe if not re.match(exclude,a.lower())]
    print('%d attributes are active'%len(r.active))
    print('%d attributes should be active'%len(r.shouldbe))
    r.missing = [a for a in r.shouldbe if a not in r.ids]
    r.polizon = [a for a in r.active if a not in r.shouldbe]
    print('%d attributes are archived but not configured'%len(r.polizon))
    r.updates = db.get_table_updates()
    r.notupdated = [a for a in r.active if  r.updates[db.get_table_name(r.ids[a])]<time.time()-interval]
    print('%d active attributes are not updated'%len(r.notupdated))
    print('%d shouldbe attributes are missing'%len(r.missing))    
    r.lost = [a for a in r.shouldbe if a in r.ids and r.updates[db.get_table_name(r.ids[a])]<time.time()-interval]
    r.lost = filter(check_attribute,r.lost)
    print('%d shouldbe attributes are active but lost'%len(r.lost))
    return r
    
def check_schema_information(schema,restart=False,save='',email=''):
  trace('In check_schema_information(%s,restart=%s,save=%s)'%(schema,restart,save)) 
  
  api = pta.api(schema,load=True)
  active = api.get_archived_attributes()
  idle = get_idle_servers(api)
  updates = get_table_updates(api)
  get_time_limit = lambda attr:time.time()-max((MAX_ERROR,api[attr].modes.get('MODE_P',[60000])[0]/1000. if 'sqlserver' not in attr else 86400))
  exclude = [] if 'ctbl' in api.host else ['sys','setpoint','wavename','waveid','bpm-acq','elotech','bake','temp','errorcode'] 
  tread,nread = 0,0 
  
  #Get all attributes updated in information_schema
  updated = [a for a in api if updates[api[a].table]>get_time_limit(a)]
  
  #Get values for attributes with no info
  attrs0 = [a for a in active if not updates[api[a].table]]
  if attrs0:
    trace('%s tables have no update_time'%len(attrs0))
    for s in (ss for ss in api.servers if 'archiver' in ss.lower() and ss not in idle):
      devs = api.servers[s].get_device_list()
      for a in (aa for aa in attrs0 if api[aa].archiver in devs):
        t0 = time.time()
        api.load_last_values(a);
        tread+=(time.time()-t0)
        nread+=1
        if api[a].last_date>get_time_limit(a):
          updated.append(a)
        elif not any(e in a for e in exclude):
          #Server already marked, so we won't need to continue querying
          break   
        
  if not nread:
      import random
      for i in range(100):
          t0 = time.time()
          api.load_last_values(active[random.randint(0,len(active)-1)])
          tread+=(time.time()-t0)
      nread = 100
      
  t1read = float(tread)/nread
  print 't1read: %f'%t1read
    
  #BECAUSE ALL BPMS DEPRECATED ARE STILL DEDICATED
  excluded = [a for a in active if a not in updated and any(e in a for e in exclude)]
  shouldbe = [a for a in get_assigned_attributes(api,dedicated=False) if a not in excluded]
  
  if 'ctbl' in api.host: shouldbe = sorted(set(shouldbe + get_deactivated_attributes(api,updates)))
  
  lost = [a for a in shouldbe if a not in updated and fun.check_attribute(a)]
  depr = [a for a in lost if not api[a].archiver]
  msg = '%s: %d/%d/%d attributes updated (%s ignored, %s lost)'%(schema.upper(),len(updated),len(shouldbe),len(api),len(excluded),len(lost))
  trace(msg)
  marked = get_attributes_servers(lost,api=api)
  marked.update(idle)
  
  if excluded: 
    print 'ignored : %s'%','.join(excluded)
  print ''
  
  txt = ''
  if depr:
    txt += ( '%d attributes should be reloaded ...'%len(depr))
    txt += '\n'+','.join(depr)
    
  if marked:
    txt += '%d servers should be restarted ...'%len(marked)
    txt += '\n'.join('\t%s:%s'%(s,' '.join(marked[s])) for s in sorted(marked))
  trace(txt)
  
  print ''
  result = {'updates':updates,'active':active,'shouldbe':shouldbe,'lost':lost,'marked':marked,'excluded':excluded,'tread':tread,'nread':nread,'t1read':t1read}
  print 'nread: %s, tread: %s, t1read: %s'%(nread,tread,t1read)
  
  try:
    if save:
      trace('Saving results in %s'%save)
      import pickle
      pickle.dump(result,open(save,'w'))
    if email and lost>5:
      fun.linos.sendmail(msg,txt,email) 
  except:
    print traceback.format_exc()
    
  if restart:
    for s in sorted(depr):
      api.start_archiving(depr)
    for s in sorted(marked):
      restart_server(s,api=api)
    trace('Archiving check finished, %d servers restarted'%len(marked))
  return marked

def check_schema_with_queries(schema):
  api = pta.api(schema)
  pending = []
  done = []

  trace('check_schema(%s)'%schema)
  #Check IDLE devices  
  for s,t in api.servers.items():
    if 'archiver' not in s: continue
    for d in t.get_device_list():
      if not fun.check_device(d):
        pending.append(s)
        break
  trace('\t%d servers have idle devices'%len(pending))

  for s,t in sorted(api.servers.items()):
    if s in pending or s in done: continue
    #Check current server attributes
    now = time.time()
    devs = map(str.lower,t.get_device_list())
    attrs = [a for a in api if api[a].archiver.lower() in devs]
    for a in attrs:
      if 'errorcode' in a: continue
      api.load_last_values(a)
      if api[a].last_date<now-(MAX_ERROR+api[a].modes['MODE_P'][0]/1000.):
        pending.append(s)
        trace('\t%s marked to restart (%s not updated since %s)'%(s,a,fun.time2str(api[a].last_date)))
        break

    #Then check servers pending to restart
    now = time.time()
    if pending and now > last_restart+WAIT_TIME:
      done.append(restart_server(api,pending.pop(0)))

  trace('\tAttribute check finished, %d/%d servers pending to restart'%(
        len(pending),len(pending)+len(done)))
      
  #Emptying the queue
  while len(pending):
    if pending and now > last_restart+WAIT_TIME:
      done.append(restart_server(api,pending.pop(0)))
    else:
      time.sleep(1.)
  trace('%s check finished, %d/%d servers have idle devices'%(schema,len(pending),len(servers)))
  
  #Now checking for attributes in .csvs that are not archived!
  #or attributes with dedicated != '' but not archived
  ##@todo...
  
  return done    

__doc__  = """
Usage:

  archiver_health_check.py [--options] [schemas] [--email=dadada@cells.es]
  
Options:

    --email=...
    --folder=...
    --configs[=...]
    --restart
  
"""

def main():
  print(__doc__)
  
  args = map(str.lower,sys.argv[1:])
  schemas = pta.Schemas.load()
  schemas = [a for a in args if a in schemas]
  
  options = dict((k.strip('-'),v) for k,v in 
                 ((a.split('=',1) if '=' in a else (a,""))
                    for a in args if a not in schemas))
                    #for a in args if a.startswith('--')))
   
  import platform
  host = platform.node()

  folder = options.get('folder',LOG_FOLDER)
  configs = options.get('config',None)
  restart = str(options.get('restart',False)).lower() not in ('false','no')
  email = options.get('email',False)
  if configs is not None: 
      configs = configs or get_csv_folder()
  
  for schema in schemas:
    date = fun.time2str(time.time(),'%Y%m%d_%H%M%S')
    if 'bl' not in host and configs:
      try:
        done = check_config_files(schema,restart=False,
                save='%s/missing_%s_%s_%s.pck'%(folder,host,schema,''),
                    email=email,csvfolder=configs)
      except:
        traceback.print_exc()
        
    done = check_schema_information(schema,restart=restart,email=email,
      save='%s/lost_%s_%s_%s.pck'%(folder,host,schema,''))

if __name__ == '__main__':
  main()
  
