__doc__ = """
Usage:

 decimate_table.py [-d] schema table[.ID] tbegin tend mlimit [tdiff]
 
 -d : dry mode
 tbegin/tend : range of dates to decimate (string)
 mlimit : min time between records to decimate (int seconds)
 tdiff : force decimation on time if by change does not reduce size
 
 ctsadev01:~$ python PyTangoArchiving/scripts/decimate_table.py \
    tdb att_05171 2018-01-01 2018-09-01 60 60
 
To obtain biggest tables in file system:

 from fandango import shell_command, clmatch
 shell_command('sudo chmod a+rX -R /var/lib/mysql/%s' % schema)
 shell_command('du -sh /var/lib/mysql/%s/* > /tmp/table_sizes.txt' % schema)
 lines = open('/tmp/table_sizes.txt').readlines()
 sizes = sorted(
     (float(l.split()[0].replace('G','e3').replace('M','')),
      l.split()[1].split('/')[-1].split('.')[0]) 
      for l in lines if clmatch('[.0-9]+(G|M)',l)
      )
 biggest = sizes[-1]
 
"""

import sys
import fandango as fn
import PyTangoArchiving as pta

tstart = fn.now()

if not sys.argv[4:]:
    print(__doc__)
    sys.exit(0)

opts = [a for a in sys.argv[1:] if a.startswith('-')]
flags = set(c for a in opts for c in a if not a.startswith('--'))
args = [a for a in sys.argv[1:] if not a.startswith('-')]
schema, table, tbegin, tend, mlimit = args[:5]
if '.' in table:
    table, aid = table.split('.')
else:
    aid = None
if table.count('_')>1 and aid is None:
    raise Exception('HDB++ tables require an ID!')

tdiff = int((args[5:] or [0])[-1])

api = pta.api(schema)
db = api if hasattr(api,'Query') else api.db
db.getTables();
cols = db.getTableCols(table)
tcol = next(s for s in ('data_time','time') if s in cols)
vcol = next(s for s in ('read_value','value_r','value') if s in cols)

try:
    print('%s belongs to %s' % (table,
        next(a for a in api if api[a].table == table) ))
except:
    pass

def query(q, att_conf_id=None):
    if att_conf_id is not None:
        q = q.replace('where ','where att_conf_id = %d and ' % att_conf_id)
    print(q)
    return db.Query(q)

q = "select UNIX_TIMESTAMP(%s), %s from %s " % (tcol, vcol, table)
q += ("where (%s between '%s' and  '%s') and %s is not NULL order by time" 
       % (tcol, tbegin, tend, vcol))

vals = query(q,aid)

print('%s has %d rows' % (table, len(vals)))

diffs = [vals[0]]
for i,r in enumerate(vals[1:]):
    if r[1]!=vals[i][1]:     
        if vals[i] != diffs[-1]:
            diffs.append(vals[i])
        diffs.append(r)       

print('At least, %d rows will be kept' % len(diffs))

if float(len(diffs))/len(vals) < 0.7 :
    
    if 'd' in flags: sys.exit(0)
    
    for i,d in enumerate(diffs[1:]):                                                      
        t0 = fn.time2str(diffs[i][0]+1)                                                   
        t1 = fn.time2str(d[0]-1)                                                          
        if fn.str2time(t1)-fn.str2time(t0) >= abs((int(tdiff) or int(mlimit))-2):
            q = ("delete from %s where time between '%s' and '%s'" 
                % (table, t0, t1))     
            query(q,aid)    
else:
    print('standard decimation doesnt pay off')
    
    if tdiff:
        print('decimating t < %s' % tdiff)
        tfirst = vals[0][0] #query(q)[0][0]
        
        trange = 3600*12
        for tt in range(int(tfirst),int(fn.str2time(tend)),int(trange)):
            q = ("select count(*) from %s where (UNIX_TIMESTAMP(%s) between %s and %s) "
                "and UNIX_TIMESTAMP(%s) %% %d <= %d")
            q = q % (table, tcol, tt, tt+trange,
                     tcol, tdiff, 3 if tdiff > 10 else 1)
            c = query(q,aid)[0][0]
   
            if c > 0.7 * trange / tdiff:
                print('%d values will be kept between %s and %s' % (c, fn.time2str(tt),fn.time2str(tt+trange)))
                q = ("delete from %s where (UNIX_TIMESTAMP(%s) between %s and %s) "
                   "and UNIX_TIMESTAMP(%s) %% %d > %d")
                q = q % (table, tcol, tt, tt+trange, tcol, tdiff, 3 if tdiff > 10 else 1)
                if 'd' in flags: 
                   print('dry run: %s' % q)
                else:
                   query(q, aid)
            else:
                   print('unable to decimate %s' % table)
                
    else:
        diffs = []
        sys.exit(0)

if 'd' not in flags:
    query('repair table %s' % table)

print('done in %d seconds' % (fn.now()-tstart))
