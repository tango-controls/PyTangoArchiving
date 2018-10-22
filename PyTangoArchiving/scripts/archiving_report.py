###############################################################################
# New archiving report, 2014
###############################################################################

"""
This file replaces the previous archiving_report, that was too exhaustive and intrusive to the system
"""

__doc__ = """
Arguments of the script are:
    --output=archiving_report.html  file to be generated
    [--input=*.csv]                 files to be read
    [--domains=.*]                  domains to be checked
    [--cycles=X]                    number of iterations
    [--filters=field:reg]           list of regular expressions to be matched
    [--exclude=field:reg]           list of regular rexpressions to exclude
    [--restart]                     whether if lost attributes have to be restarted or not
"""

import sys,time,traceback,re,pickle
import fandango
import fandango.functional as fun
import fandango.web as web
from collections import defaultdict
import PyTangoArchiving
import PyTangoArchiving.utils as utils
import PyTangoArchiving.files as files


#Overridable by fandango.web?
camel = lambda s: ''.join(r[0].upper()+(r[1:] or '').lower() for r in s.split())
color = lambda s,color: '<font color="%s">%s</font>'%(camel(color),s)

def summarize(seq,seq2=[],NMAX=5):
    #Max number of attrs to display in a single line
    #seq = [seq for seq in seq if seq]
    #return str(seq) if len(seq)<NMAX else '[%d]'%len(seq)
    if not fun.isSequence(seq): 
        if type(seq) is float:
            if seq>1e-2 or not seq: 
                if 0<=seq<=.9:
                    return color(web.bold('%1.2f'%seq),'red')
                else:
                    return '%1.2f'%seq
            else: 
                return '%1.2e'%seq
        else: return seq
    res = fandango.device.reduce_distinct(seq,seq2) if seq2 else (seq,1.0)
    if seq and len(res[0])<NMAX and res[1]>.5:
        return '%d, like:<br>%s' % (len(seq),'<br>'.join(res[0]))
    else:
        return '%d'%len(seq)
    
def attribute_name_check(attribute):
    dev,attr = a.rsplit('/',1)
    all_devs = fandango.get_all_devices()
    if dev not in all_devs: 
        return False #Device does not exist
    elif not fandango.check_device(dev): 
        return True #If we can't check the attribute we assume that exists.
    elif attr.lower() in map(str.lower,PyTango.DeviceProxy(dev).get_attribute_list()):
        return True
    else:
        return False
    
from PyTangoArchiving import GetConfigFiles,ParseCSV
from fandango import str2time, get_device_info
from fandango.dicts import defaultdict

def archiving_check(schema,csvpath=''):
    api = PyTangoArchiving.ArchivingAPI(schema)
    
    states = api.servers.states()
    
    values = api.load_last_values()#time consuming on HDB
    shouldbe = sorted(a for a in api if values[a] and fandango.date2time(values[a][0][0]) > time.time()-2*30*3600*24)
    active = api.get_archived_attributes()
    updated = sorted(a for a in active if values[a] and fandango.date2time(values[a][0][0]) > time.time()-3*3600)
    
    missing = sorted(a for a in shouldbe if a not in active)
    lost = sorted(a for a in active if a not in updated)
    
    loadarchivers = defaultdict(list)
    loadservers = defaultdict(list)
    lostarchivers = defaultdict(list)
    lostservers = defaultdict(list)
    for a in active:
        arch = api[a].archiver.lower()
        server = api.servers.get_device_server(arch).lower()
        loadarchivers[arch].append(a)
        loadservers[server].append(a)
        if a in lost:
            lostarchivers[arch].append(a)
            lostservers[server].append(a)
            
    [loadservers[api.servers.get_device_server(api[a].archiver.lower()).lower()].append(a) for a in active]

    emptyarchivers = [a for a,v in loadarchivers.items() if not len(v)]
    lostrate = dict((a,len(v) and len([a for a in v if a in lost])/float(len(v))) for a,v in loadarchivers)
    lostserversrate = dict((a,len(v) and len([a for a in v if a in lost])/float(len(v))) for a,v in loadservers.items())
    
    dedi = api.load_dedicated_archivers()
    dediattrs = defaultdict(list)
    [dediattrs[a.lower()].append(d) for d,v in dedi.items() for a in v];
    dmult = [a for a,v in dediattrs.items() if len(v)>1]
    wrongnames = [a for a in dediattrs if not attribute_name_check(a)]
    wrongarchivers = set(k.lower() for k,v in dedi.items() if any(a.lower() in map(str.lower,v) for a in wrongnames))
    wrongattrs = [a for a,v in dediattrs if a in api and api[a].archiver.lower()!=v[0].lower()]
    deleteattrs = [a for a in dediattrs if a not in shouldbe]
    
    fnames = GetConfigFiles(csvpath) if csvpath else GetConfigFiles()
    csvs = dict((f,pta.ParseCSV(f,schema)) for f in fnames)
    csvattrs = defaultdict(list)
    [csvattrs[a.lower().strip()].append(f) for f,v in csvs.items() for a in v]
    
    stats = sorted([(len(v),len(v) and len([a for a in v if a in lost])/float(len(v))) for v in loadservers.values()])
    stats = [(x,fandango.avg(t[1] for t in stats if t[0]==x)) for x in sorted(set(v[0] for v in stats))]
    # pylab.plot([t[0] for t in stats], [t[1] for t in stats]); pylab.show()
    exported = dict((d,fandango.str2time(fandango.get_device_info(d).started,'%dst %B %Y at %H:%M:%S')) for d in api.get_archivers())
    first = min(exported.values())
    #SLOWER SPEEDS ALWAYS HAVE MORE LOST ATTRIBUTES
    
    #Let's try a different approach to restart, much less agressive than fandango.start_servers()!
    #It seems that there's a lock when so many devices are restarted at once!
    torestart = list(reversed(sorted((len(v),k) for k,v in lostservers.items())))
    
    for k in torestart.values():
        print('Restarting %s')
        fandango.Astor(k).stop_servers()
        time.sleep(20.)
        fandango.Astor(k).start_servers(wait=240.)
    
    allattrs = sorted(set([a for a in csvattrs if a in api]+shouldbe+active))
    
    
def get_access_times(schema,attrs=None):
    import time
    import PyTangoArchiving as pta
    api = pta.api(schema)
    values,times = {},{}
    t0 = time.time()
    attrs = attrs or api.keys()
    for a in attrs:
        times[a] = time.time()
        values[a] = api.load_last_values(a)
        times[a] = time.time()-times[a]
        total = time.time()-t0
    return times
    
"""
    loads = defaultdict(list)
[loads[tdb[a].archiver].append(a) for a in active]
[loads[tdb.servers.get_device_server(tdb[a].archiver)].append(a) for a in active]
In [57]: wrongname = sorted(a for a,v in dediattrs.items() if a not in tdb)

In [58]: wrongarchiver = sorted(a for a,v in dediattrs.items() if a in tdb and tdb[a].archiver and tdb[a].archiver.lower() not in v)

In [59]: len(wrongname)
Out[59]: 145

In [60]: len(wrongarchiver)
Out[60]: 21
shouldbe = sorted(a for a in active if fandango.date2time(last_values[a][0][0]) > fandango.str2time('2014-01-17 00:00'))

                            In [319]:len([a for a in active if a not in csvactive]) 
active = oldactive + [a for a in active if a not in oldactive]                          Out[319]:966    
                                
csvactive = sorted(set(a for c in [pta.ParseCSV(f,'hdb') for f in files] for a in c))                           In [320]:len([a for a in csvactive if a not in active]) 
                            Out[320]:833    
allactive = list(set(active+csvactive))                             
                                
"""
    
######################################################################################################3    

def main_report():
    
    args = fandango.linos.sysargs_to_dict(['output','input','domains'])
    assert args, __doc__
    print 'archiving_report.py arguments are:\n\t%s'%args
        
    filename = args.get('output','/tmp/archiving_report.html')
    inputs = args.get('input','')
    domreg = fun.toRegexp(args.get('domains',''))
    restart = args.get('restart','')
    cycles, cycle_time = 1,6*3600
    if restart is True: restart = '.*'
    
    if 'filters' in args:
        if ':' not in args['filters']:
            argfilters = {'name':args['filters']}
        else:
            argfilters = dict((f.split(':') for f in args['filters'].split(',') if ':' in f))
    else: argfilters = {}
    
    if 'exclude' in args:
        if ':' not in args['exclude']:
            argexclude = {'name':args['exclude']}
        else:
            argexclude = dict((f.split(':') for f in args['exclude'].split(',') if ':' in f))
    else: argexclude = {}
    
    if 'type' in argexclude: 
        argexclude['type'] = '(stop|%s)'%argexclude['type']
    else: 
        argexclude['type'] = 'stop'
    
    if 'cycles' in args:
        cycles = int(args['cycles'])
        
    ######################################################################################################3
    results = {}
    summary = {'hdb':{},'tdb':{}}
    failed = {}
    polizons = {}
    
    print 'The archiving check will be performed %s.' % (cycles<0 and 'forever' or '%s times'%cycles)
    
    while cycles!=0:
        tstart = time.time()
        
        # Getting the CSV input files
        if domreg:
            configs = []
        elif inputs:
            if '/' in inputs: 
                configs = sorted(files.GetConfigFiles(*inputs.rsplit('/',1)))
            else:
                configs = sorted(files.GetConfigFiles(mask=inputs))
        else:
            configs = sorted(files.GetConfigFiles())
        
        print '#'*80
        print '%s, In archiving_report.py: reading %s, generating %s' % (time.ctime(),configs or domreg,filename)
        
        for schema in ('hdb','tdb'):
            print 'Checking %s configurations' % schema.upper()
        
            api = PyTangoArchiving.ArchivingAPI(schema,load=True)    
            api.clean_attribute_modes() #Cleanup of amt table in database
            
            active = [a for a in api if api[a].archiver]
            if not active: #No attributes being archived in the database
                continue
            dedicated = list(set([a for a in active if api[a].dedicated]))
            print 'There are %d dedicated attributes' % (len(dedicated))
            
            #Initializing variables
            results[schema] = {}
            summary[schema] = {'active':len(active),'dedicated':len(dedicated)}
            polizons[schema] = {}
            up,down,idle,archivers = [],[],[],api.check_archivers()
            for k,v in archivers.items():
                {True:up,False:down,None:idle}[v].append(k)
            summary[schema].update({'up':up,'down':down,'idle':idle,'archivers':archivers.keys()})
            
            ############################################################################
            #Checking Attribute Configuration CSV files
            ############################################################################
            if configs:
                polizons[schema] = active[:]
                for c in configs:
                    filters = dict(argfilters.items())
                    exclude = dict(argexclude.items())
                    try:
                        print '%s: Checking file %s' % (schema.upper(),c.split('/')[-1])
                        print 'filters: %s ; exclude: %s' % (filters,exclude)
                        
                        check = files.CheckArchivingConfiguration(
                            c,
                            api=api,schema=schema,
                            restart=bool(restart and fun.matchCl(restart,c)), #Restart has to be done here as it is where the modes configuration is available
                            filters=filters,exclude=exclude)
                            
                        if not any(check.values()): #No values returned?
                            continue    
                        
                        #Creating a summary of check results:
                        results[schema][c] = check
                        #check['rate'] = (float(len(check['ok']))/len(check['all'])) if (check['ok'] and check['all']) else 0.                    
                        print ('\n'.join('%s: %s'%(k.upper(),summarize(v)) for k,v in results[schema][c].items() if v))
                        [polizons[schema].remove(a) for a in check['all'] if a in polizons[schema]] #Checking how many attributes are 'alien' to specs
                        flie = open('/tmp/%s.%s.pck'%(c.split('/')[-1].rsplit('.',1)[0],schema),'w')
                        pickle.dump((c,check),flie)
                        flie.close()
                    except Exception,e:
                        if c in results[schema]: 
                            #results[schema].pop(c)
                            results[schema][c] = dict((k,0) for k in 'all  rate  ok  diff  late  hung  lost  retried  unavailable  missing  triable  dedicated  polizon'.split())
                        failed['%s:%s'%(schema,c)] = traceback.format_exc()
                        print failed['%s:%s'%(schema,c)]
                                        
            ############################################################################
            #Doing a generic check of the archiving, ignoring configurations
            ############################################################################
            elif domreg:
                domains = defaultdict(list)
                [domains[a.split('/')[0].lower()].append(a) for a in api if fun.matchCl(domreg,a.split('/')[0]) and api[a].archiver]
                
                print ('Checking %s attributes by domain(%s): %s'%(schema,domreg,[(k,len(v)) for k,v in domains.items()]))
                for d,attributes in domains.items():
                    print '%d attributes in domain %s' % (len(attributes),d)
                    ok,lost,goods,bads,retried = [],[],[],[],[]
                    try:
                        [(goods if utils.check_attribute(a,readable=True) else bads).append(a) for a in attributes]
                        if not goods:
                            continue
                        if goods: 
                            [(lost if v else ok).append(a) for k,v in api.check_attributes_errors(goods,hours=1,lazy=True).items()]
                        print '%d attributes on time'%len(ok)
                        if restart and lost:
                            for att in lost:
                                modes = api[att].modes
                                if not modes: continue
                                print 'Restarting archiving for %s' % att
                                if api.start_archiving([att],modes,load=False):
                                    retried.append(att)
                        check = {'all':attributes,'ok':ok,'lost':lost,'retried':retried,'unavailable':bads}
                        results[schema][d] = check
                        results[schema][d]['rate'] = (float(len(ok))/len(attributes)) if goods else 0
                        print ('\n'.join('%s: %s'%(k.upper(),summarize(v)) for k,v in results[schema][d].items() if v))
                    except Exception,e:
                        if d in results[schema]: results[schema].pop(d)
                        failed['%s:%s'%(schema,d)] = traceback.format_exc()
                        print failed['%s:%s'%(schema,d)]
                        
        ###################################################################################################
        
        CHECK_KEYS = ['all','rate','ok','diff','late','hung','lost','retried','unavailable','missing','triable','dedicated','polizon']
        
        report = open(filename,'w')
        def add2lines(text,trace=True):
            #print text
            lines.append(text)
            #report.write(text)
        #add2lines('<html><body>')
        
        index = web.title('Index',4) + '\n<ul>'
        lines = []
        totals = {}
        
            
        add2lines('<br>Script called like: archiving_report.py %s<br>' % (' '.join(sys.argv)))
        add2lines(web.separator)
        add2lines('<br>'+'Archiving Status Report at %s'%time.ctime(tstart)+', generated in %d seconds <br>'%(int(time.time()-tstart)))
        add2lines(web.dict2dict2table(summary,keys=['','active','dedicated','archivers','down','idle','up'],formatter=fun.partial(summarize,NMAX=0)))
        add2lines(web.em('active,dedicated refers to attributes status'))
        add2lines(web.link(web.em('up,down,idle refers to archivers status'),web.iurl('Archivers Status')))
        add2lines(web.separator)
        
        def conf2file(chkfile,vals):
            print 'Writing check values to %s ...' % chkfile
            try:
                f = open(chkfile,'w')
                f.write('\t'.join(CHECK_KEYS)+'\n')
                f.write('\t'.join([str(vals[k]) for k in CHECK_KEYS]) + '\n')
                f.close()
            except:
                print 'Unable to write %s: %s' % (chkfile,traceback.format_exc())
                
        #Adding Summaries for each Schema
        for schema,vals in sorted(results.items()):
            add2lines(web.title('Checking %s configurations' % schema.upper(),2))
            add2lines(web.paragraph('Filters are:'+web.ulist(web.item('include %s'%argfilters)+web.item('exclude %s'%argexclude))))
            
            add2lines(web.dict2dict2table(dict((web.link(k,web.iurl(schema+':'+k)),v) for k,v in vals.items()),keys=['']+CHECK_KEYS,formatter=fun.partial(summarize,seq2=(vals['ok'] if 'ok' in vals else[]),NMAX=0)))
            
            totals.update([(k,sum([(len(v[k]) if fun.isSequence(v[k]) else v[k]) for v in vals.values() if k in v])) for k in CHECK_KEYS])
            
            conf2file(filename.rsplit('.',1)[0]+'.%s'%schema,totals)
            if polizons.get(schema): add2lines(web.paragraph(web.link('%d attributes in archiving does not appear in listed files'%len(polizons[schema]),web.iurl(schema+':polizons'))))    
            add2lines(web.separator)
            
        try:
            add2lines(web.title('Tables legend:',3))
            add2lines(web.ulist('<br>\n'.join([web.item(s) for s in ['%s: \t%s'%(k,v) for k,v in files.REPORT_LEGEND.items()]])))
        except:
            pass
            
        if failed:
            try:
                add2lines(web.title('Failed config files',2))
                for conf,error in failed.items():
                    add2lines(web.title(conf,3))
                    add2lines(error.replace('\n','<br>\n'))
                add2lines(web.separator)
            except:
                add2lines(traceback.format_exc())
                
        #index += '</ul>'+web.paragraph(web.separator)+'\n'
        #lines.insert(1,index) #adding the index after title
        try: add2lines('<p>check finished after %d minutes</p>'%((time.time()-tstart)/60))
        except: pass
        
        add2lines(web.separator)
        
        add2lines(web.separator)
        
        add2lines(web.title('Archivers Status',2))
        for k in ['down','idle']:
            add2lines(web.title(k,4))
            for schema in ('hdb','tdb'):
                if schema not in summary: continue
                values = summary[schema].get(k,[])
                add2lines(', '.join(values),trace=False)
        
        add2lines(web.separator)
        
        #Adding Summaries for each File
        for schema,vals in sorted(results.items()):
            add2lines(web.title('Checking %s configurations' % schema.upper(),2))
            add2lines(web.paragraph('Filters are:'+web.ulist(web.item('include %s'%argfilters)+web.item('exclude %s'%argexclude))))
            #add2lines(web.dict2dict2table(dict((web.link(k,web.iurl(schema+':'+k)),v) for k,v in vals.items()),keys=['']+CHECK_KEYS,formatter=fun.partial(summarize,seq2=(vals['ok'] if 'ok' in vals else[]))))
            for conf,keys in vals.items():
                add2lines(web.title('%s:%s' % (schema,conf),3))
                for key in CHECK_KEYS:
                    if key not in keys: continue
                    if key.lower() in ['all','ok','dedicated','rate']: continue
                    add2lines(web.title(key,4))
                    add2lines(', '.join(sorted(keys[key])),trace=False)
            
            poli = polizons.get(schema,[])
            if poli:# and len(poli)<totals['all']:
                add2lines(web.title(schema+':polizons'))
                add2lines(web.paragraph('%d attributes in archiving does not appear in listed files: <br>%s'%(len(polizons[schema]),polizons[schema])))
            add2lines(web.separator)
        
        add2lines(web.separator)
        report.write(web.page(web.body('\n<br>'.join(lines))))
        #add2lines('</body></html>')
        report.close()
        
        #Writing rates to a CSV file
        csv_file = open(filename.replace('.html','.csv'),'w')
        rates = sorted(('%s_%s'%(schema.upper(),c.rsplit('/')[-1].rsplit('.',1)[0]),stats['rate']) for schema,vals in sorted(results.items()) for c,stats in vals.items())
        csv_file.write('\t'.join(['Date']+[v[0] for v in rates])+'\n'+'\t'.join(str(x) for x in [time.time()]+[v[1] for v in rates]))
        csv_file.close()
        
        if cycles>0: cycles-=1
        if cycles>0: time.sleep(cycle_time)
        
    ######################################################################################################
    print 'archiving_report(%s) finished' % (sys.argv[1:])
    ######################################################################################################3
        
    

if __name__ == '__main__':
    main_report()


