#!/usr/bin/env python

import PyTangoArchiving as pta, sys, time, re, fandango as fn
import PyTangoArchiving.utils as ptau

args = sys.argv[1:]
tt0 = time.time()

__doc__ = """
Usage:

This script should allow to decimate several HDB++ dbs/tables in parallel
A db_repair call may be needed at the end of the process.

    PyTangoArchiving/hdbpp/decimate.py action db_name args 

actions/args are:

    check db_name : 
        list bigger files in /var/lib/mysql/db_name (may need permissions)
        
    check db_name tstart tend table
        print number of rows per attribute in the given table

    decimate db_name tstart tend [data_types / attributes] :
        will decimate data on the given table/attributes in the specified interval
"""

def check_db(db_name):        
    data = fn.shell_command("du -sh /var/lib/mysql/%s/*"%db_name)
    lines = [l.strip().split() for l in data.split('\n')]
    print(lines[0])
    gs = sorted((eval(l[0].replace('G','')),'G',l[1]) for l in lines if l and 'G' in l[0])
    for g in gs:
        print(g)        

def check_table(db_name,table,tstart,tend):
    print('check_table(%s,%s,%s,%s)'% (db_name,table,tstart,tend))
    api = pta.api(db_name)
    tstart,tend = fn.time2str(tstart),fn.time2str(tend)
    rows = dict()
    for a in api:
        api.get_attr_id_type_table(a)
        if api[a].table == table:
            r = api.Query("select count(*) from %s where att_conf_id = %s and "
                "data_time between '%s' and '%s'" % (table,api[a].id,tstart,tend))
            #if r[0][0] > 1e3:
            #print(a,r)
            rows[a] = r[0][0]
            
    print('%d attributes found'  % len(rows))
    for n,k in sorted((s,a) for a,s in rows.items()):
        print('%s id=%s rows=%s' % (k, api[k].id, n))
            
def decimate(db_name,keys,tstart,tend,period=10,dry=False):
    """
    time arguments are strings
    """
    api = pta.api(db_name)
    
    if '/' in keys[0]:
        print('Decimating by attribute names')
        tables = fn.defaultdict(list)
        for a in keys:
            api.get_attr_id_type_table(a)
            tables[api[a].table].append(a)
            
        print('tables: %s' % (tables.keys()))
        for table,attrs in tables.items():
            for a in attrs:
                pta.dbs.decimate_db_table_by_time(api,
                    table,api[a].id,tstart,tend,period,
                    optimize=(a==attrs[-1]))
    
    if not '/' in keys[0]:
        print('Decimating by data_type')

        data_types = keys
        if not data_types:
            data_types = [r[0] for r in api.Query('select data_type from att_conf_data_type')]
        else:
            data_types = [d.replace('att_','') for d in data_types]
            
        print('Decimating %s types between %s and %s: %s'%(db_name,tstart,tend,data_types))

        for data_type in data_types:

            attrs = api.Query('select att_conf_id from att_conf,att_conf_data_type '
                    'where att_conf.att_conf_data_type_id = att_conf_data_type.att_conf_data_type_id '
                    'and data_type = "%s"'%data_type)
            attrs = [r[0]  for r in attrs]

            q = ("select partition_name,table_name"
                    " from information_schema.partitions where"
                    " partition_name is not NULL"
                    " and table_schema = '%s'"%db_name +
                    " and table_name like '%"+data_type+"'" )
            print(q)
            partitions = api.Query(q)
            if partitions:
                table = partitions[0][1]
            else:
                table = 'att_'+data_type
            print('%s has %d attributes in %d partitions'%(table,len(attrs),len(partitions)))
            c0 = api.Query('select count(*) from %s '%table)

            import re
            intervals = []

            for p in partitions:
                p = p[0]
                r = '(?P<year>[0-9][0-9][0-9][0-9])(?P<month>[0-9][0-9])'
                md = re.search(r,p).groupdict()
                t0 = '%s-%s-01 00:00:00'%(md['year'],md['month'])
                m,y = int(md['month']),int(md['year'])
                
                if m == 12:
                    m,y = 1, y+1
                else:
                    m+=1
                    
                t1 = '%04d-%02d-01 00:00:00'%(y,m)
                if fn.str2time(t0)<fn.str2time(tend) and \
                fn.str2time(t1)>fn.str2time(tstart):
                    intervals.append((t0,t1,p))

            if not partitions:
                ts,te = fn.str2time(tstart),fn.str2time(tend)
                tinc = (te-ts)/10.
                for i in range(1,11):
                    intervals.append((fn.time2str(ts+(i-1)*tinc),
                                    fn.time2str(ts+i*tinc),None))
                
            print('%d intervals in %s'%(len(intervals),table))
                
            for t0,t1,p in intervals:
                
                print((t0,t1))
                if dry: continue
                for a in attrs:
                    c0 = api.getTableSize(table)
                    pta.dbs.decimate_db_table(db=api,table=table,
                        start=fn.str2time(t0),end=fn.str2time(t1),
                        period=600 if 'string' in table else 300,
                        condition=' att_conf_id = %s '%a,
                        iteration=2000,cols=['data_time','value_r'],
                        us=True, repeated=True)
                    
                if p: api.Query('alter table %s optimize partition %s'%(table,p))

            if not dry:
                q = 'repair table %s;'%table
                print('\n'+q)
                api.Query(q)
                c1 = api.getTableSize(table)
                print('\n\n%s size reduced from %s to %s'%(table,c0,c1))
            
        print('ellapsed %d seconds'%(time.time()-tt0))
            
if __name__ == '__main__':            
    

    if not args or args[0] not in "help check decimate":
        print(__doc__)
        sys.exit(-1)
        
    action,args = args[0],args[1:]
    
    if action == 'help':
        print(__doc__)

    elif action == 'check':
        db_name = args[0]
        if len(args) == 1:
            check_db(db_name)
        else:
            tstart = fn.str2time(args[1])
            tend = fn.str2time(args[2])
            table = args[3]
            check_table(db_name,table,tstart,tend)
        sys.exit(0)
      
    elif action == 'decimate':
        db_name = args[0] #'hdbmiras'
        tstart = fn.time2str(fn.str2time(args[1]))
        tend = fn.time2str(fn.str2time(args[2]))
        period = int(args[3])
        keys = args[4:] #['scalar_devdouble_ro']
        decimate(db_name,keys,tstart,tend, period)

    
    
