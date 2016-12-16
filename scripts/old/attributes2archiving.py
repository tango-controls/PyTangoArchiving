import time
gstart=time.time()

from PyTango import *
import traceback
import inspect
import sys
import operator
import re
import time

from xml.dom import minidom
from os.path import abspath

from fandango.arrays import CSVArray
from PyTangoArchiving import ArchivingAPI,ArchivedAttribute,SnapAPI

#def addTangoDev(server,_class,device):
        #di = DbDevInfo()
        #di.name,di._class,di.server = device,_class,server
        #db.add_device(di)
  
MAX_INSTANCE_LOAD=20
log_counters={}

def loadCSVfile(filename,dedicated={},deletelist=[],context={}):
  ''' returns attrslist, dedicated, deletelist and a context_info object
  filename is the file from where the Array is loaded
  dedicated is a dictionary with key=attribute and value=archiver 
  deletelist is a list, contains the attributes to be removed from archiving
  context is a dictionary, contains the information about the authority of the configuration
  '''
  print 'Loading CSV/XML file ...',filename
  config = CSVArray()
  config.load(filename,comment='#')
  headers=['Device','Host','Attribute','Type','ArchivingMode','Periode','MinRange','MaxRange']
  print 'Searching headers ...'
  head=config.get(0)
  
  def checkHeaders():
    if not all(h in ''.join(head) for h in headers):
      print 'WRONG FILE HEADERS!'
      exit()
      
  [config.fill(head=h) for h in head]
  config.setOffset(1)
  
  print 'Getting attributes from the file ...'
  #it returns the list of device names and the lines that matches for each
  hosts=config.getAsTree(lastbranch='ArchivingMode')#config.get(head='Device',distinct=True)
  
  ##Parsing the params to create a Context
  #-------------------------------------------------
  defaultparams = {'@LABEL':'User_Code-0X','@AUTHOR':'Who?','@DATE':'When?','@DESCRIPTION':'What?','@REASON':'Why?'}
  transparams = {'@LABEL':'name','@AUTHOR':'author','@DATE':'time','@DESCRIPTION':'description','@REASON':'reason'}
  for p,v in defaultparams.items():
    if p not in hosts.keys() or not hosts[p] or hosts[p].keys()[0]==v:
      raise Exception('PARAMS_ERROR','All these defaultparams are MANDATORY!: %s'%str(defaultparams.keys()))
    defaultparams[p]=hosts.pop(p).keys()[0]
    context[p]=defaultparams[p]
    if p=='@DATE':
      t,time_fmts = None,['%Y-%m-%d','%Y-%m-%d %H:%M','%y-%m-%d','%y-%m-%d %H:%M','%d-%m-%Y','%d-%m-%Y %H:%M','%m-%d-%Y','%m-%d-%Y %H:%M',
        '%Y/%m/%d','%Y/%m/%d %H:%M','%y/%m/%d','%y/%m/%d %H:%M','%d/%m/%Y','%d/%m/%Y %H:%M','%m/%d/%Y','%m/%d/%Y %H:%M  ',]
      for tf in time_fmts:
        try:
          #print 'trying format %s'%str(tf)
          t = time.strftime('%Y-%m-%d',time.strptime(context[p],tf))
          break
        except: pass
      if t is not None: context[transparams[p]]=t
      else: raise Exception('PARAMS_ERROR','@DATE format should be YYYY-MM-DD!: %s'%str(context[p]))

  ##Reading the archiving modes
  #-------------------------------------------------
  attrslist = {}
  archmodes={'PERIODIC':'MODE_P','ABSOLUTE':'MODE_A','RELATIVE':'MODE_R','THRESHOLD':'MODE_T','CALC':'MODE_C','EXTERNAL':'MODE_E'}
  for host,devs in sorted(hosts.items()):
    for dev,attributes in sorted(devs.items()):
      print 'reading device %s:%s'%(dev,str(attributes))
      template=[a for a in attributes if '@COPY' in a]
      if template:
        if ':' in template[0]:
            dev2 = template[0].split(':')[1]
        elif template[0] in attributes:
            dev2 = attributes[template[0]].keys()[0]
        #else: dev2 = ''
        [attributes.__setitem__(k,v) for k,v in devs[dev2].items() if k and k not in attributes]
        [attributes.pop(t) for t in template]
      
      #If a @DELETE or @STOP is found as single attribute all dev. attributes are stopped
      elif '@DELETE' in attributes.keys() or '@STOP' in attributes.keys():
        deletelist.append(dev)
      
      defaults = [(a,v) for a,v in attributes.items() if '@DEFAULT' in a and v]
      DEFAULT_MODE = defaults and {} or {'MODE_P':[300000]}
      for a,tipus in defaults:
        mode,params = tipus.values()[0].items()[0]
        mode =  archmodes.get(mode.upper(),mode)
        DEFAULT_MODE[mode] = params
        print dev,'.DEFAULT_MODE=',DEFAULT_MODE
        attributes.pop(a)
      
      for attribute,modes in sorted(attributes.items()):
        config = dict(DEFAULT_MODE.items())
        if not modes:
          print '\treading attribute %s: using default modes %s'%(attribute,str(DEFAULT_MODE))
        else:
          print '\treading attribute %s:%s'%(attribute,str(modes))
          tipus=modes.keys()[0]
          #And modes is overriden by its own member
          modes=modes.values()[0]
          firstmode,firstparam=modes.keys()[0],modes.values()[0][0]
          if any(a.startswith('@') for a in [attribute,firstmode,tipus]):
            if attribute=='@DEDICATED':
              dedicated[dev]=tipus
              #print dev,'.DEDICATED=',tipus
            elif any(c in ['@STOP','@DELETE'] for c in [firstmode,tipus]):
              deletelist.append(dev+'/'+attribute)
              #print dev,'.STOP ARCHIVING'
          else:
            #Adding always a default value to the list of modes.
            if 'MODE_P' not in config: config.update(DEFAULT_MODE.items())
            for mode,params in modes.items():
              if not mode: continue
              print '\treading mode %s:%s'%(mode,str(params))
              mode=mode.upper()
              if mode in archmodes.keys():
                mode=archmodes[mode]
              elif mode not in archmodes.values():
                print 'Unknown mode!: ',mode
                continue
              params = [float(p) for p in params if p]
              config[mode]=params
            attrslist[dev+'/'+attribute]={'host':host,'type':tipus,'modes':config}
            #print 'attrslist[%s/%s]=%s'%(dev,attribute,attrslist[dev+'/'+attribute])

  print 'Specified %d attributes from %d hosts'%(len(attrslist),len(hosts))
  if dedicated: print '%d devices are dedicated'%(len(dedicated))
  if deletelist: print '%d attributes to delete'%(len(deletelist))
  
  return attrslist
#END OF loadCSVFile
#############################################################################################################


def checkDev(dev,attr='Pressure'):
    dp = PyTango.DeviceProxy(dev)
    try:
        dp.ping()
    except Exception,e:
        print '%s is not working: %s'%(dev,str(e))
    try:
        a_value = dp.read_attribute(attr)
        return a_value.value
    except Exception,e:
        print '%s.%s failed: %s'%(dev,attr,str(e))
    return False

def restartAttributes(attrslist,deletelist,arch_type):
  ''' 
  restart/stop the archiving of attributes using an ArchivingManager DeviceProxy
  attrslist = {attribute:{modes:{MODE_:,},host:,...}}
  deletelist = attributes to delete (to stop and not restart)
  api = An existing ArchivingAPI
  '''

  api=getSingletonAPI()
  dp = api.API_getProxy(api.managers[arch_type][0])
  dp.set_timeout_millis(3600000)
  
  try:
    attrs=attrslist.keys()
    #Attributes to delete
    if deletelist:
      print 'Stopping archiving for all selected attributes ... ',deletelist
      for a in attrs:
        if a not in deletelist and a.rsplit('/',1)[0] in deletelist:
          deletelist.append(a)
        #else:
        if a in deletelist:
          attrs.remove(a)
      for d in deletelist:
        if d.count('/')==2: deletelist.remove(d)
      print 'deletelist enhanced to ',deletelist
      #THIS PART OF THE CODE IS NOT REALLY WORKING!
      api.attr_StopArchiving(deletelist,arch_type)
  
    #NOTE: THE ATTRIBUTES SHOULD BE PREVIOUSLY SPLITTED IN DIFFERENT GROUPS BY MODE!!!
    #Start all attributes that have the same mode (if a restart is needed it is done here too)
    modeslist={}
    for a in attrs:
      modestring=ArchivedAttribute().extractModeString(attrslist[a]['modes'])
      if modestring in modeslist.keys(): modeslist[modestring].append(a)
      else: modeslist[modestring]=[a]
    for modes,alist in modeslist.items():
      print 'Starting archiving for all attributes with mode %s'%modes
      api.attr_StartArchiving(alist,arch_type,ArchivedAttribute().extractModeString(modes))
    
  except PyTango.DevFailed,e:
    PyTango.Except.print_exception(e)
  except Exception,e:
    exstring = traceback.format_exc()
    print 'Exception occurred and catched: ', exstring
    print "Exception '",str(e),"' in ",inspect.currentframe().f_code.co_name  
    print 'Last exception was: \n'+str(e)+'\n'

  pass 

#------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------#

def splitAttributesInGroups(attrslist,MAX_SIZE=100):
  '''
  This method will return a dict of lists of attributes; each of them being a list of strings
  attrslist = {attrname:{host:,modes:{MODE_:,},}}
  '''
  result = {}
  if len(attrslist)>MAX_SIZE:
    print 'splitAttributesInGroups: More than %d attributes in a single list!!!, splitting by domains ...'%MAX_SIZE
    # If the list of attributes is >100 it is split into domains
    domains = {}
    for a in attrslist:
      domain=a.split('/')[0]
      if domain not in domains: domains[domain]={a:attrslist[a]}
      else: domains[domain][a]=attrslist[a]
        
    for domain,attributes in domains.items():
      if len(attributes)<=MAX_SIZE:
        result[domain]=attributes.keys()
      else:
        # If the list of attributes is >100 it is split into families
        families={}
        for a in attributes:
          family=a.split('/')[1]
          if family not in families: families[family]={a:attrslist[a]}
          else: families[family][a]=attrslist[a]
        for family,atts in families.items():
          for i in range(1+int(len(atts)/100)):
            # If the list of attributes is >100 it is split
            gid = domain+'-'+family+ ('-%02d'%i if len(atts)>100 else '')
            result[gid]=atts.keys()[100*i:100*(i+1)]
  else: result['ALL']=attrslist.keys()
  return result
  
  
def extend_list(*args):
  ''' This method allows to concatenate together a set of lists or generator results '''
  result = []
  if len(args)==1 and (type(args[0]) is list or type(args[0]).__name__=='generator'): args=list(args[0])
  for a in args:
    if type(a) is list: result.extend(a)
    else: result.append(a)
  return result


def configureArchivers(api,arch_type,attrslist,dedicated,delete_list):
  ''' 
  args are archiving api, attrslist, dedicated, delete_list
    attrslist = {attribute:{type:,host:,modes:,dedicated:,delete:,}}
  returns modservers: {host:{server:{archiver:{new:True}}}}
  '''
  archivers=api.getArchiversLoad(arch_type)
  if archivers:
    print 'Loads of existing archivers are: '
    for n,l in archivers.items():
      if l: print '%s:\t%d'%(n,l)

  #Using the result of api.DB_loadAttrStatus() instead   #creating a dictionary {attribute:archiver}
  previous_archiver={}
  for a in api.attributes.values():
    if a.archiver: previous_archiver[a.name.lower()]=a.archiver
  
  #Getting the previous dedicated variables introduced in the archiving system
  archiver_properties={}
  for archiver in archivers:
    archiver_properties[archiver]=api.db.get_device_property(archiver,['isDedicated','reservedAttributes'])

  mod_hosts = {} #To this dict will be added the modified servers
  mod_archivers = []
  
  # Checking for each attribute which archivers are available
  # It will depend of the previous load of each archiver and whether it is dedicated or not
  atts = [a for a in attrslist.keys() if a not in delete_list]
  for a in atts:
    archiver,host,label,serial='','','',1
    dev=a.rsplit('/',1)[0]
    host=attrslist[a]['host']
    
    #Create regular expression for finding a suitable archiver
    if dev in dedicated.keys():
      print 'The device %s has a dedicated archiver.'%dev
      label = dedicated[dev]
      reg = ('(.*?%s.*?%s-)([0-9]{2,2})'%(host,label)).lower()
      server='%sArchiver/%s_%s'%('Hdb' if arch_type=='hdb' else 'Tdb',host,label)
      #print 'The attribute %s is dedicated for %s/%s'%(a,host,label)
    else:
      #print 'The attributes is not dedicated, uses generic archivers'
      reg = ('(.*-)([0-9]{1,2})')
      server='%sArchiver/%s'%('Hdb' if arch_type=='hdb' else 'Tdb',host)
    
    if a.lower() in previous_archiver.keys() and re.match(reg,previous_archiver[a.lower()].lower()):
      #Keeping the same archiver that was being used previously
      archiver=previous_archiver[a.lower()]
      print 'Attribute %s will use the same archiver %s'%(a,archiver)
      
      #DEPRECATED BECAUSE THE API DOESN'T RESPECT THE EXECUTABLE NAME CASE
      #try:
        #server=api.API_getProxy(archiver).info().server_id
      #except Exception,e:
        #print e
      
      #Adding to mod_hosts the server with not_running archivers
      if not api.server_Ping(archiver):
        if host not in mod_hosts: mod_hosts[host]={}
        if server not in mod_hosts[host]: mod_hosts[host][server]=[]
        print 'Archiver %s exists but is not running, it must be started!'%(archiver)
        mod_hosts[host][server].append(archiver)
    else:  
      #Creating a new link archiver-attribute
      #print 'Creating a new Archiver-Attribute link (%s) for %s'%(reg,a)
      match = [arch for arch in archivers if re.match(reg,arch)]
      #To an existing archiving server ... (could need a new archiver instance)
      if match:
        match.sort()#Getting the last of the matching archiver names
        suitable = [m for m in match if archivers[m]<MAX_INSTANCE_LOAD]
        lastmatch = re.match(reg,match[-1])
        #With Full Load
        if not suitable:
          archiver='%s%02d'%(lastmatch.groups()[0],(int(lastmatch.groups()[-1])+1))
          #print 'Archiver %s overloaded (%d), creating %s'%(lastmatch.group(),archivers[lastmatch.group()],archiver)
        #Reusable
        else: 
          archiver=suitable[0]
          #print 'Suitable archiver %s found for attribute %s,' % (str(archiver),a)
          archivers[archiver]+=1
        #DEPRECATED BECAUSE THE API DOESN'T RESPECT THE EXECUTABLE NAME CASE
        #try:
          #dp=PyTango.DeviceProxy(archiver if suitable else lastmatch.group())
          #server=dp.info().server_id
          #print 'Server name got from TangoDB : %s'%server
        #except Exception,e:
          ##Using the name previously generated
          #pass
      #To a New Dedicated Archiver ...
      elif dev in dedicated.keys():
        archiver='%sArchiver/%s/%s-01'%('Hdb' if arch_type=='hdb' else 'Tdb',host,label)
        server='%sArchiver/%s_%s'%('Hdb' if arch_type=='hdb' else 'Tdb',host,label)
      else:
        print 'No matching archiver has been found for %s'%a

      #Adding to mod_hosts the server with new,not_running or dedicated archivers
      if archiver not in archivers.keys() or not api.server_Ping(archiver) or dev in dedicated.keys():
        if host not in mod_hosts: mod_hosts[host]={}
        if server not in mod_hosts[host]: mod_hosts[host][server]=[]
        if not (dev in dedicated.keys()): #The archiver is new or not running
          if archiver not in archivers.keys(): #Creating new archivers if necessary
            print 'Creating new archiver %s on %s'%(archiver,server)
            di = DbDevInfo()
            di.name,di._class,di.server = archiver,'%sArchiver'%('Hdb' if arch_type=='hdb' else 'Tdb',),server
            api.db.add_device(di)
            archivers[archiver.lower()]=1
            archiver_properties[archiver.lower()]={}
          else: #It means that server_Ping has failed! 
            print 'Archiver %s exists but is not running, it must be started!'%(archiver)
          mod_hosts[host][server].append(archiver)
        else:
            pass
        pass
        
      if dev in dedicated.keys():
        #Update isDedicated and reservedAttributes properties if necessary
        #print 'archiver_properties length is %d vs %d archivers'%(len(archiver_properties),len(archivers))
        for arch,d in archiver_properties.items():
          if arch==archiver.lower():
            if 'reservedAttributes' not in d: 
              #print 'Adding default Dedicated properties to archiver %s'%(arch)
              d['reservedAttributes']=[]
            if 'isDedicated' not in d or not d['isDedicated']: d['isDedicated']=['TRUE']
            if a not in d['reservedAttributes']:
              print 'Adding attribute %s to archiver %s'%(a,arch)
              d['reservedAttributes'].append(a)
              if archiver not in mod_hosts[host][server]: mod_hosts[host][server].append(archiver)
              if archiver not in mod_archivers: mod_archivers.append(archiver)
            else:
              print 'The attribute %s was already assigned to archiver %s; but it doesnt appear to be the actual archiver!!!'%(a,arch)
              if archiver not in mod_hosts[host][server]: mod_hosts[host][server].append(archiver)
              if archiver not in mod_archivers: mod_archivers.append(archiver)
          elif 'reservedAttributes' in d and a in d['reservedAttributes']:
            print 'Removing attribute %s from archiver %s'%(a,arch)
            d['reservedAttributes'].remove(a)
            #@TODO: When host information for existing servers become readable
            # this lines must be changed.
            mod_hosts[host][server].append(arch)
            if arch not in mod_archivers: mod_archivers.append(arch)
          elif ('reservedAttributes' not in d or not len(d['reservedAttributes'])) and 'isDedicated' in d and 'TRUE' in d['isDedicated']:
            print 'Removing isDedicated property from archiver %s'%(arch)
            d['reservedAttributes']=[]
            d['isDedicated']=['FALSE']
            mod_hosts[host][server].append(arch)
            if arch not in mod_archivers: mod_archivers.append(arch)
          #Updating the modified properties
          archiver_properties[arch]=d
          
      pass #End of adding archivers
            
    #Checking TDB Properties
    if arch_type.lower()=='tdb':
      if archiver in archiver_properties.keys():
        val = archiver_properties[archiver]
      else: archiver_properties[archiver],val = {},{}
      pathprops=['DbPath','DiaryPath','DsPath']
      tdbpath='/tmp/archiving/tdb'
      if any(p not in val or val[p][0]!=tdbpath for p in pathprops):
        print 'Updating TdbPath properties of %s'%archiver
        [archiver_properties[archiver].__setitem__(p,[tdbpath]) for p in pathprops]
        if host not in mod_hosts: mod_hosts[host]={}
        if server not in mod_hosts[host]: mod_hosts[host][server]=[]
        if archiver not in mod_hosts[host][server]: mod_hosts[host][server].append(archiver)
        if archiver not in mod_archivers: mod_archivers.append(archiver)
    pass #End of checking each attribute archiver
    
  print 'Updating properties of %d archivers ...'%(len(mod_archivers))
  for archiver in mod_archivers:
    #print 'Updating properties of %s: %s'%(archiver,str(archiver_properties[archiver]))
    api.db.put_device_property(archiver,archiver_properties[archiver])
  return mod_hosts
  
#------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------#
    
def attributes2archiving(filename,arch_type):
  tstart=time.time()
  #Context value by default
  defaultparams={'@LABEL':'User_Code-0X','@AUTHOR':'Who?','@DATE':'When?','@DESCRIPTION':'What?','@REASON':'Why?'}
  
  #dedicated={}
  #deletelist=[]
  #context={}
  attrslist=loadCSVfile(filename)#,dedicated,deletelist,context)
  print '%d attributes read from %s file'%(len(attrslist),filename)
  #Now all the previous lists are initialized
   
  #Get attributes in both DB and file
  api = ArchivingAPI('hdb')
  
  #attrslist[dev+'/'+attribute]={'host':host,'type':tipus,'modes':modes_}  
  for a,v in sorted(attrslist.items()):
    attrslist[a]['modes']=api.check_modes(v['modes'])
  
  #############################################################################################
  ##configureArchiving(attrslist)
  ## returns attrslist, dedicated, deletelist
  #############################################################################################
  ##If there's only names in the file go directly to the export part
  #if not all(not a['modes'] for a in attrslist.values()):
    ##Use @STOP to delete attributes from the archiving system.
    #newlist=[]
    #for d in deletelist:
      #if d.count('/')==2: #is a device name
        #devattrs = [a.name for a in dbattrs.values() if a.device==d]
        #newlist+=devattrs
      #else: newlist+=d
    #deletelist=newlist
    #if deletelist: print 'Devices and/or attributes to Stop Archiving: ', deletelist
    ##Deletion is implemented later
    
    #api.setLogLevel('info')
    #api.compare2db(attrslist)
    
    ##Ask for confirmation before adding to archivers
    #answer = raw_input('Are you sure that you want to introduce this changes in the archiving?')
    #if not answer.lower() in ['y','yes']:
      #return
    
    #modhosts = configureArchivers(api,arch_type,attrslist,dedicated,deletelist)
    ##modhosts = {host:{server:[archivers]}}
    
    #servers2stop = {}
    #servers2start = {}
    
    #if modhosts:
      #answer = raw_input('The new archivers will be now started. PRESS ENTER TO CONTINUE')
      
      #for host,servers in modhosts.items():
        #for server,archivers in servers.items(): #If servers is empty nothing happens
          #if not archivers: continue
          ##print 'Checking modified host/server: %s/%s'%(host,server)
          #if host not in servers2stop: servers2stop[host]=set()
          #servers2stop[host].add(server)
          ##for arch in archivers:
          ##Why to check archivers individually? If they are, they are modified; and if they are not they should be started!!!!
      
      ##The servers must be started separately for each Host!!!
      #api.setLogLevel('DEBUG')
      #if servers2stop:
        #for host,servers in servers2stop.items():
          #print 'Stopping servers in host %s'%host
          #api.server_Stop(servers)
        #print 'Waiting for the devices to stop ...'
        #time.sleep(10.)
    
      #for host,servers in modhosts.items():
        #print 'Starting servers in host %s'%host
        #api.server_Start(servers.keys(),host)
      #api.setLogLevel('INFO')
      #print 'Waiting for the devices to start ...'
      #time.sleep(10.)
    
      #api.info("Restart ArchivingManager because of new archivers (40s.) ...")
      ##Seems that .Init() is not enough with Dedicated archivers!
      #api.server_Stop('ArchivingManager/1')
      #time.sleep(10.)
      #api.server_Start('ArchivingManager/1',wait=600)
      #time.sleep(30.)
      ##manager.command_inout('init')
    
    #time.sleep(1.)
    #att_groups=splitAttributesInGroups(attrslist,MAX_SIZE=100)
    #time.sleep(1.)
    
    #for gid,group in att_groups.items():
      #snap_api = SnapAPI(api,'manager','manager')
      #label = arch_type.upper()+'-' + (context['@LABEL'] if len(att_groups)==1 else context['@LABEL']+'-'+gid)
      #print 'Creating Context and Configuration for group %s.'%label
      ##print 'type of author is %s, type of group is %s'%(str(type(context['@AUTHOR'])),str(type(group)))
      #context_id=snap_api.createNewContext(
        #context['@AUTHOR'],
        #label,
        #context['@DATE'],
        #context['@REASON'],
        #context['@DESCRIPTION'],
        #group
        #)
      #a_group = {}
      #[a_group.__setitem__(k,v) for k,v in attrslist.items() if k in group]
      #restartAttributes(a_group,deletelist,arch_type)
      ##launchSnapshot(context_id)
  #else:
    #pass
  
  #print 'Ellapsed %f seconds'%(time.time()-tstart)
  #pass
  
#THIS WERE THE ARGUMENTS THAT WORKED ON TG_DEVTEST: 0,1,sim/pysignalsimulator/01-01/A1,MODE_P,10000.

if __name__ == "__main__":
  print 'argv are "%s"'%(sys.argv)
  attributes2archiving(filename=sys.argv[1],arch_type=sys.argv[2] if len(sys.argv)>2 else 'hdb')