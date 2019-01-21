import fandango as fn
import fandango.db as fdb
import PyTangoArchiving as pta
from MySQLdb.cursors import SSCursor


dec = 'hdbrf_d'
org = 'hdbrf_r'

dec = fdb.FriendlyDB(dec, user='manager', passwd='manager')
org = fdb.FriendlyDB(org, user='manager', passwd='manager')

"""
tables = org.getTables()
tables = [t for t in tables if 'data_time' in org.getTableCols(t)]

d = org.Query('select att_conf_id, UNIX_TIMESTAMP(data_time), value_r, quality, att_error_desc_id from att_scalar_devboolean_ro order by data_time, att_conf_id', export = False)
d.fetchone()
dec.Query('insert into att_scalar_devboolean_ro (att_conf_id, data_time, value_r, quality, att_error_desc_id values (5, FROM_UNIXTIME(1525878257.517), 1, 0, NULL')
dec.Query('insert into att_scalar_devboolean_ro (att_conf_id, data_time, value_r, quality, att_error_desc_id values (5, FROM_UNIXTIME(1525878257.517), 1, 0, NULL)')
import fandango as fn
fn.time2str(1525878257.517)
dec.Query("insert into att_scalar_devboolean_ro (att_conf_id, data_time, value_r, quality, att_error_desc_id values (5, '2018-05-09 17:04:17', 1, 0, NULL)")
dec.Query("insert into att_scalar_devboolean_ro (att_conf_id, data_time, value_r, quality, att_error_desc_id) values (5, '2018-05-09 17:04:17', 1, 0, NULL)")
dec.Query('select * from att_scalar_devboolean_ro')
r = d.fetchone()
r
fn.time2str(r[1])
fn.time2str?
fn.time2str(r[1],us=True)
fn.time2str(float(r[1]),us=True)
r
dec.Query("insert into att_scalar_devboolean_ro (att_conf_id, data_time, value_r, quality, att_error_desc_id) values (5, '2018-05-09 17:04:17.519999', 1, 0, NULL)")
dec.Query('select * from att_scalar_devboolean_ro')
org.getTables()
org.getTableCols('att_scalar_devboolean_rw')


In [50]: sorted(org.getTableCols('att_array_devboolean_rw'))
Out[50]: 
['att_conf_id',
 'att_error_desc_id',
 'data_time',
 'dim_x_r',
 'dim_x_w',
 'dim_y_r',
 'dim_y_w',
 'idx',
 'insert_time',
 'quality',
 'recv_time',
 'value_r',
 'value_w']

In [51]: sorted(org.getTableCols('att_array_devboolean_rw'))
Out[51]: 
['att_conf_id',
 'att_error_desc_id',
 'data_time',
 'dim_x_r',
 'dim_x_w',
 'dim_y_r',
 'dim_y_w',
 'idx',
 'insert_time',
 'quality',
 'recv_time',
 'value_r',
 'value_w']

"""

def transfer_table(db, db2, table, bunch = 1024, is_str = False):
    
    t0 = fn.now()       
    cols = db.getTableCols(table)
    
    cols = sorted(c for c in cols if c not in ('recv_time','insert_time'))
    it, iv, ii = (cols.index('data_time'), cols.index('value_r'), 
        cols.index('att_conf_id'))
    ix = cols.index('idx') if 'idx' in cols else None
    #if is_array:
        #print("%s: THIS METHOD IS NO SUITABLE YET FOR ARRAYS!" % table)
        ## dim_x/dim_y dim_x_r/dim_y_r columns should be taken into account
        ## when array should be stored?  only when value changes, or on time/fixed basis?
        #return
    
    
    last = db2.Query('select UNIX_TIMESTAMP(data_time) from %s order by '
        'data_time desc, att_conf_id limit 1' % table)
    last = fn.time2str(last and last[0][0] or 0)
    end = db.Query('select CAST(UNIX_TIMESTAMP(data_time) AS DOUBLE) from %s order by '
        'data_time desc, att_conf_id limit 1' % table)
    end = fn.time2str(end and end[0][0] or 0, us = True)    
    
    lasts = dict()
    
    qcols = (','.join(cols)).replace('data_time',
        'CAST(UNIX_TIMESTAMP(data_time) AS DOUBLE)')
    query = 'select %s from %s' % (qcols, table)
    where = " where data_time >= '%s'"
    limit = ' limit %s' % bunch
    order = ' order by data_time, att_conf_id'
    
    print('inserting data ...')
    
    count,done = 0,0
    while True:
        values = ''
        #.split('.')[0]
        qr = query+(where%last)+order+limit
        print(qr)
        cursor = db.Query(qr, export=False)
        v = cursor.fetchone()
        if v is None:
            break
        last = fn.time2str(v[it],us=True)
        if last == end:
            break
        
        for _ in range(bunch):
            count += 1
            i,t,w = v[ii], v[it], v[iv]
            x = v[ix] if ix is not None else None
            last = fn.time2str(t,us=True)
            if ix is None and (i not in lasts or w != lasts[i][1] 
                    or (t-lasts[i][0]) >= 60):
                lasts[i] = (t,w)
                v = map(str,v)
                v[2] = repr(last)
                if values:
                    values += ','
                values += '(%s)' % ','.join(v)
                v = cursor.fetchone()
                if v is None:
                    break
                done += 1
            elif ix is not None and ((i,x) not in lasts 
                    or (t-lasts[(i,x)][0])>=60):
                lasts[(i,x)] = (t,w)
                v = map(str,v)
                v[2] = repr(last)
                if values:
                    values += ','
                values += '(%s)' % ','.join(v)
                v = cursor.fetchone()
                if v is None:
                    break
                done += 1
            else:
                v = cursor.fetchone()
                if v is None:
                    break
        
        if values:        
            values = values.replace('None','NULL')
            insert = "insert into %s (%s) VALUES %s" % (
                table, ','.join(cols), values)
            print(insert[:80],insert[-80:])
            db2.Query(insert)
        print(done,count)
    
    print('%d/%d values inserted in %d seconds'  % (done,count,fn.now()-t0))
