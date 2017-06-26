#!/usr/bin/env python

import PyTangoArchiving, sys, time, re, fandango as fd
import PyTangoArchiving.hdbpp.config as ptahc
import PyTangoArchiving.utils as ptau

args = sys.argv[1:]
tt0 = time.time()

try:
    db_name = args[0] #'hdbmiras'
    tstart = fd.time2str(fd.str2time(args[1]))
    tend = fd.time2str(fd.str2time(args[2]))
    data_types = args[3:] #['scalar_devdouble_ro']
except:
    print('Usage: PyTangoArchiving/hdbpp/decimate.py db_name tstart tend [data_types]')

api = ptahc.HDBpp(db_name=db_name,user='manager',passwd='manager')

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
        if fd.str2time(t0)<fd.str2time(tend) and \
           fd.str2time(t1)>fd.str2time(tstart):
            intervals.append((t0,t1,p))

    if not partitions:
        ts,te = fd.str2time(tstart),fd.str2time(tend)
        tinc = (te-ts)/10.
        for i in range(1,11):
            intervals.append((fd.time2str(ts+(i-1)*tinc),
                              fd.time2str(ts+i*tinc),None))
        
    print('%d intervals in %s'%(len(intervals),table))
        
    for t0,t1,p in intervals:
        
        print((t0,t1))

        for a in attrs:
            c0 = api.Query('select count(*) from %s'%table)
            ptau.decimate_db_table(db=api,table=table,
              start=fd.str2time(t0),end=fd.str2time(t1),
              period=600 if 'string' in table else 300,
              condition=' att_conf_id = %s '%a,
              iteration=2000,cols=['data_time','value_r'],
              us=True, repeated=True)
              
        if p: api.Query('alter table %s optimize partition %s'%(table,p))

    q = 'repair table %s;'%table
    print('\n'+q)
    api.Query(q)
    c1 = api.Query('select count(*) from %s '%table)
    print('\n\n%s size reduced from %s to %s'%(table,c0,c1))
    print('ellapsed %d seconds'%(time.time()-tt0))
    
    
    
