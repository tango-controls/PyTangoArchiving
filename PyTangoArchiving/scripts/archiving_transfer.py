import fandango as fn
from fandango import str2time, time2str
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

def get_type_tables(db):
    return db.Query('select data_type,att_conf_data_type_id '
        'from att_conf_data_type')

def get_table_attr_ids(db, table):
    t = table.replace('att_','')
    ti = dict(get_type_tables(db))[t]
    return [r[0] for r in db.Query('select att_conf_id from att_conf'
        ' where att_conf_data_type_id = %d' %ti)]

def transfer_table(db, db2, table, bunch = 16*16*1024, is_str = False,
                   per_value = 60, min_tdelta = 0.2, ids = []):
    
    t0 = fn.now()       
    tq = 0
    cols = db.getTableCols(table)
    
    has_int = 'int_time' in cols
    
    cols = sorted(c for c in cols 
                  if c not in ('recv_time','insert_time','int_time'))
    it, iv, ii = (cols.index('data_time'), cols.index('value_r'), 
        cols.index('att_conf_id'))
    ix = cols.index('idx') if 'idx' in cols else None
    
    is_float = 'double' in table or 'float' in table
    
    #if is_array:
        #print("%s: THIS METHOD IS NO SUITABLE YET FOR ARRAYS!" % table)
        ## dim_x/dim_y dim_x_r/dim_y_r columns should be taken into account
        ## when array should be stored?  only when value changes, or on time/fixed basis?
        #return
    
    lasts = dict()
    
    qcols = (','.join(cols)).replace('data_time',
        'CAST(UNIX_TIMESTAMP(data_time) AS DOUBLE)')
    query = 'select %s from %s' % (qcols, table)
    if has_int:
        where = " where int_time >= %d and int_time < %d "
    else:
        where = " where data_time >= '%s'"
        where += " and data_time < '%s'"
    
    order = ' order by data_time'
    if has_int:
        #order = ' order by int_time' #It may put NULL/error values FIRST!!
        if min_tdelta > 1:
            order = ' group by int_time DIV %d'%int(min_tdelta) + order
    else:
        if min_tdelta > 1:
            order = ' group by data_time DIV %d'%int(min_tdelta) + order
        
    limit = ' limit %s' % bunch
    
    print('inserting data ...')
    
    count,done,changed,periodic = 0,0,0,0
    attr_ids = get_table_attr_ids(db, table)
    for aii,ai in enumerate(attr_ids):

        if ids and ai not in ids:
            continue
        
        print('attr: %s (%s/%s)' % (ai,aii,len(attr_ids)))
        
        print('getting limits ...')
        last = db2.Query('select UNIX_TIMESTAMP(data_time) from %s '
            ' where att_conf_id = %d order by '
            'att_conf_id, data_time desc limit 1' % (table,ai))
        last = last and last[0][0] or 0
        if not last:
            last = db.Query('select CAST(UNIX_TIMESTAMP(data_time) AS DOUBLE) from %s '
                ' where att_conf_id = %d '
                'order by att_conf_id,data_time limit 1' % (table,ai))
            last = last and last[0][0] or 0
        last = fn.time2str(last)
        
        print(last)
        end = db.Query('select CAST(UNIX_TIMESTAMP(data_time) AS DOUBLE) from %s '
            ' where att_conf_id = %d '
            'order by att_conf_id,data_time desc limit 1' % (table,ai))
        end = end and end[0][0] or fn.now()
        if end > fn.now(): end = fn.now()
        end = fn.time2str(end, us = True)         
        print(end)
        
        #return
        while True:        
            print('attr: %s (%s/%s)' % (ai,aii,len(attr_ids)))
            values = ''
            #.split('.')[0]
            prev = last
            print('last: %s' % last)
            nxt = fn.time2str(fn.str2time(last)+4*86400)
            
            if fn.str2time(last) >= fn.now() or fn.str2time(nxt) >= fn.now():
                break            
            if fn.str2time(last)+60 >= fn.str2time(end):
                break            
            if has_int:
                qr = query+(where%(int(str2time(last)),int(str2time(nxt))))
            else:
                qr = query+(where%(last,nxt))
                
            qr += ' and att_conf_id = %s' % ai
            qr += order+limit
            print(qr)
            tq = fn.now()
            cursor = db.Query(qr, export=False)
            print(fn.now()-tq)
            v = cursor.fetchone()
            if v is None:
                last = nxt
            else:
                last = fn.time2str(v[it],us=True)
                
            if fn.str2time(last)+60 >= fn.str2time(end):
                break #It must be checked before and after querying
            if v is None:
                continue
            
            curr = 0
            for _i in range(bunch):
                #print(_i,bunch)
                curr += 1
                count += 1
                i,t,w = v[ii], v[it], v[iv]
                x = v[ix] if ix is not None else None

                last = fn.time2str(t,us=True)
                if i not in lasts:
                    diff = True
                elif t < lasts[i][0]+min_tdelta:
                    diff = False
                else:
                    diff = (w != lasts[i][1])
                    if is_float:
                        if w and None not in (w,lasts[i][1]):
                            diff = diff and abs((w-lasts[i][1])/w)>1e-12
                            
                if ix is None and diff:
                    # changed scalar value
                    lasts[i] = (t,w)
                    v = map(str,v)
                    v[2] = repr(last)
                    if values:
                        values += ','
                    values += '(%s)' % ','.join(v)
                    changed += 1
                    done += 1
                    v = cursor.fetchone()
                    if v is None:
                        break
                    
                elif ix is None and (t-lasts[i][0]) >= per_value:
                    # periodic scalar value
                    lasts[i] = (t,w)
                    v = map(str,v)
                    v[2] = repr(last)
                    if values:
                        values += ','
                    values += '(%s)' % ','.join(v)
                    periodic += 1
                    done += 1                    
                    v = cursor.fetchone()
                    if v is None:
                        break
                    
                elif ix is not None and ((i,x) not in lasts 
                        or (t-lasts[(i,x)][0]) >= per_value):
                    # periodic array value
                    lasts[(i,x)] = (t,w)
                    v = map(str,v)
                    v[2] = repr(last)
                    if values:
                        values += ','
                    values += '(%s)' % ','.join(v)
                    done += 1
                    v = cursor.fetchone()
                    if v is None:
                        break
                    
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
            #else:
                #print('NO VALUES TO INSERT')
                #break
                
            print(curr,changed,periodic,done,count)
            #print(last,nxt,end)
            if last == prev:
                last = nxt
            if fn.str2time(last) >= fn.now():
                break
    
    print('%d/%d values inserted in %d seconds'  % (done,count,fn.now()-t0))
