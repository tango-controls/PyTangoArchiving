import sys, traceback
import fandango as fn
import PyTangoArchiving as pta
from fandango.db import FriendlyDB
from fandango import time2str, str2time

def get_table_description(api,table):
    if fn.isString(api): api = pta.api(api)
    return api.Query('show create table %s'%table)[-1][-1]

"""

In [1]: import PyTangoArchiving as pta

In [2]: api = pyta.api('hdbpp')
---------------------------------------------------------------------------
NameError                                 Traceback (most recent call last)
<ipython-input-2-b81ce98a59f8> in <module>()
----> 1 api = pyta.api('hdbpp')

NameError: name 'pyta' is not defined

In [3]: table = 'att_array_devfloat_rw'

In [4]: hdbpp = pta.api('hdbpp')
HDBpp(): Loading from Schemas

In [5]: partitions = pta.dbs.get_partitions_from_query(hdbpp,'select * from %s'%table)

In [6]: partitions
Out[6]: 'afw20171201,afw20180101,afw20180201,afw20180301,afw20180401,afw20180501,afw20180601,afw20180701,afw20180801,afw20180901,afw20181001,afw20181101,afw20181201,afw20190101,afw20190201,afw20190301'

In [7]: partitions = 'afw20180301,afw20180401,afw20180501,afw20180601,afw20180701,afw20180801,afw20180901,afw20181001,afw20181101,afw20181201,afw20190101,afw20190201,afw20190301'

In [8]: hdbpp.Query('alter table %s optimize partition %s' % (table,partitions))
Out[8]: [('hdbpp.att_array_devfloat_rw', 'optimize', 'status', 'OK')]

"""

def get_all_partitions(api):
    partitions = dict(((t,p),api.getPartitionSize(t,p)) 
                      for t in api.getTables() 
                      for p in api.getTablePartitions(t))
    return partitions

def get_attributes_row_counts(db,attrs='*',start=0, stop=0,
                              partition='',limit=0):
    """
    DUPLICATED BY HDBPP.get_attribute_rows !!!
    
    It will return matching $attrs that recorded more than $limit values in 
    the $start-$stop period::
    
      countsrf = get_attributes_row_counts('hdbrf',start=-3*86400,limit=20000)
      
    """
    db = pta.api(db) if fn.isString(db) else db
    if fn.isString(attrs):
        attrs = [a for a in db.get_attributes() if fn.clmatch(attrs,a)]
        
    r = {}
    for a in attrs:
        i,t,b = db.get_attr_id_type_table(a)
        q = "select count(*) from %s " % b
        if partition:
            q += "partition(%s) " % partition
        q += "where att_conf_id = %d"  % (i)
        if start and stop:
            start = start if fn.isString(start) else fn.time2str(start) 
            stop = stop if fn.isString(stop) else fn.time2str(stop)
            q += " and data_time between '%s' and '%s'" % (start,stop)
        l = db.Query(q)
        c = l[0][0] if len(l) else 0
        if c >= limit:
            r[a] = c
    return r

#@staticmethod
#def decimate_values(values,N=540,method=None):
    #"""
    #values must be a sorted (time,...) array
    #it will be decimated in N equal time intervals 
    #if method is not provided, only the first value of each interval will be kept
    #if method is given, it will be applied to buffer to choose the value to keep
    #first value of buffer will always be the last value kept
    #"""
    #tmin,tmax = sorted((values[0][0],values[-1][0]))
    #result,buff = [values[0]],[values[0]]
    #interval = float(tmax-tmin)/N
    #if not method:
        #for v in values:
        #if v[0]>=(interval+float(result[-1][0])):
            #result.append(v)
    #else:
        #for v in values:
        #if v[0]>=(interval+float(result[-1][0])):
            #result.append(method(buff))
            #buff = [result[-1]]
        #buff.append(v)

    #print(tmin,tmax,N,interval,len(values),len(result),method)
    #return result
    
def decimate_table(db, table, attributes = [], 
                   start = 0, stop = -1, partition = '', 
                   trange = 3, fmargin = 1):
    """
    @TODO
    """
    db = pta.api(db) if fn.isString(db) else db
    l,_ = db.getLogLevel(),db.setLogLevel('DEBUG')
    t0 = fn.now()
    
    int_time = 'int_time' in db.getTableColumns(t)
    
    where = 'partition(%s)'%partition if partition else ''
    where += " where "
    if attributes:
        where += " att_conf_id in (%s) and " % (','.join(attributes))
        
    if start!=0 or stop!=-1 or not partition:
        if int_time:
            s = start if start>0 else fn.now()-start
            e = stop if stop>0 else fn.now()-stop
            dates += " int_time between %s and %s "
        else:
            s, e = fn.time2str(start), fn.time2str(stop)
    
    hours = [t0+i*3600 for i in range(24*30)]
    days = [t0+i*86400 for i in range(30)]
    dvalues = {}
    q = ("select count(*) from %s where att_conf_id = %d "
        "and data_time between '%s' and '%s'")
    for d in days:
        s = fn.time2str(d)
        q = hdbpp.Query(q%(table,att_id,s,fn.time2str(d+86400))
                        +" and (data_time %% 5) < 2;")
        
    sorted(values.items())
    3600/5
    for h in hours:
        s = fn.time2str(h)
        q = hdbpp.Query("select count(*) from att_scalar_devdouble_ro "
            "where att_conf_id = 1 and data_time between '%s' and '%s' "
            "and (data_time %% 5) < 2;"%(s,fn.time2str(h+3600)))
        
    
    ## Get Bigger ID's in table
    # MariaDB [hdbpp_r]> select att_conf_id, count(*) as COUNT 
    # from att_scalar_devdouble_ro partition(sdr20180601) 
    # group by att_conf_id HAVING COUNT > 864000 order by COUNT;
    
    #MariaDB [hdbpp_r]> select att_conf_id, count(*) as COUNT from att_scalar_devdouble_ro partition(sdr20180601) group by att_conf_id HAVING COUNT > 864000 order by COUNT;
    #+-------------+----------+
    #| att_conf_id | COUNT    |
    #+-------------+----------+
    #|          99 |  1756407 |
    #|         967 |  1855757 |
    #|         963 |  1877412 |
    #|         966 |  1917039 |
    #|         961 |  1921648 |
    #|         964 |  1956849 |
    #|         975 |  1989966 |
    #|        1035 |  1989980 |
    #|        1024 |  1990009 |
    #|        1023 |  1990068 |
    #|         962 |  2211943 |
    #|         968 |  2659039 |
    #|         969 |  2659042 |
    #|        1005 |  2754352 |
    #|        1006 |  2754378 |
    #|        1029 |  2755194 |
    #|        1007 |  2755194 |
    #|         985 |  2797790 |
    #|        1019 |  2797801 |
    #|          97 |  3782196 |
    #|        1014 |  4054545 |
    #|         992 |  4054548 |
    #|        1015 |  4077444 |
    #|         997 |  4077526 |
    #|         974 |  4174792 |
    #|        1012 |  4174847 |
    #|        1013 |  4266515 |
    #|         986 |  4266653 |
    #|           1 |  8070710 |
    #|         982 |  8456059 |
    #|         981 |  8456105 |
    #|         996 |  9815756 |
    #|        1018 |  9815768 |
    #|        1037 | 10138245 |
    #|        1032 | 10138463 |
    #|           5 | 12769963 |
    #|           6 | 12881867 |
    #+-------------+----------+
    #37 rows in set (2 min 37.02 sec)

    """
    drop table tmpdata;
    set maxcount : = 864000;
    
    create temporary table tmpdata (attid int(10), rcount int(20));
    
    insert into tmpdata select att_conf_id, count(*) as COUNT 
        from att_scalar_devdouble_ro partition(sdr20180501) 
        group by att_conf_id order by COUNT;
        
    delete from att_scalar_devdouble_ro partition(sdr20180501) 
        where att_conf_id in (select attid from tmpdata where rcount > @maxcount)
        and CAST(UNIX_TIMESTAMP(data_time) AS INT)%3 > 0;
        
    select att_conf_id, count(*) as COUNT 
        from att_scalar_devdouble_ro partition(sdr20180501) 
        group by att_conf_id order by COUNT;
        
    select att_conf_id, count(*) as COUNT 
        from att_scalar_devdouble_ro partition(sdr20180501) 
        group by att_conf_id order by COUNT HAVING COUNT > @maxcount;
        
    set attid := (select attid from tmpdata order by count desc limit 1);
    select * from tmpdata where attid = @attid;
    select att_conf_id, data_time, count(*) as COUNT 
        from att_scalar_devdouble_ro partition(sdr20180501) 
        where att_conf_id = @attid group by att_conf_id, 
        CAST((UNIX_TIMESTAMP(data_time)/(86400)) AS INTEGER) 
        order by att_conf_id,data_time;
    """
    
    ## Get N rows per day
    # select data_time, count(*) as COUNT from att_scalar_devdouble_ro 
    # partition(sdr20180501) where att_conf_id = 1013 group by 
    # CAST((UNIX_TIMESTAMP(data_time)/86400) AS INTEGER) order by data_time;
    
    # MariaDB [hdbpp_r]> delete from att_scalar_devdouble_ro 
    # partition(sdr20180501) where att_conf_id = 6 
    # and CAST(UNIX_TIMESTAMP(data_time) AS INTEGER)%3 > 0; 
        
    q = 'repair table %s' + ('partition(%s)'%partition if partition else '')
    api.Query(q)
    print('decimate(%s, %s) took %f seconds' % (table,partition,fn.now()-t0))
    db.setLogLevel(l)
    return


def decimate_by_value_to_tmp(api, table, start, stop, ntable='', min_period=0, 
                 max_period=21600, max_gap=86400, suffix='_dec',drop=False):
    """
    decimate by distinct value, or by fix period
    accept a min_resolution argument
    do selects in bunches of ids*1000 values or 250000 (the minimum)
    """
    #bigger = sorted((db.getTableSize(t),t) for t in dbr.getTables() if 'scalar' in t)[-1]
    
    if not ntable:
        ntable = table+suffix
    l = api.getLogLevel()
    nattrs = len(api.get_attributes_by_table(table))
    
    if fn.isString(start):
        date0,date1 = start,stop
        start,stop = fn.str2time(start),fn.str2time(stop)
    else:
        date0,date1 = fn.time2str(start),fn.time2str(stop)
    
    where = 'where att_conf_id < 1000000 '
    if 'int_time' in api.getTableCols(table):
        where += ' and int_time between %d and %d' % (start,stop)
    else:
        where += " and data_time between '%s' and '%s'" % (date0,date1)

    tables = api.getTables()
    api.setLogLevel('DEBUG')        
    if drop:
        api.Query('drop table if exists %s' % ntable)
        
    if ntable not in tables:
        code = api.getTableCreator(table)
        qi = code.split('/')[0].replace(table,ntable)
        try:
            api.Query(qi)
        except Exception as e:
            api.warning('Unable to create table %s' % ntable)
            print(e)

    ###########################################################################
    t0 = fn.now()
    api.info('Get %s values between %s and %s' % (table, date0, date1))
    what = "att_conf_id,data_time,value_r,quality,UNIX_TIMESTAMP(data_time)"
    array = 'idx' in api.getTableCols(table)
    if array:
        what+= ',idx,dim_x_r,dim_y_r'
    data = api.Query('select %s from %s ' % (what, table) + where 
                     + ' order by att_conf_id, data_time')
    print(fn.now()-t0,'seconds')
    
    data_ids = fn.defaultdict(list)
    if array:
        [data_ids[i].append((d,v,q,t,j,x,y)) for i,d,v,q,t,j,x,y in data];
    else:
        [data_ids[i].append((d,v,q,t)) for i,d,v,q,t in data];

    data_dec = dict((k,[] if not v else [v.pop(0)]) for k,v in data_ids.items())
    
    ###########################################################################
    t0 = fn.now()
    api.info('Decimating %d values' % len(data))
    if array:
        [data_dec[i].append((d,v,q,j,x,y,t)) for i in data_ids for d,v,q,t,j,x,y in data_ids[i] 
            if not data_dec[i] or (
                (v!=data_dec[i][-1][1] or q!=data_dec[i][-1][2] 
                    or t>data_dec[i][-1][-1]+max_period) 
                and (t-min_period)>data_dec[i][-1][-1])]
            
        data_all = sorted((d,i,v,q,j,x,y) for i in data_dec for d,v,q,j,x,y,t in data_dec[i])
    else:
        [data_dec[i].append((d,v,q,t)) for i in data_ids for d,v,q,t in data_ids[i] 
            if not data_dec[i] or (
                (v!=data_dec[i][-1][1] or q!=data_dec[i][-1][2] 
                    or t>data_dec[i][-1][-1]+max_period) 
                and (t-min_period)>data_dec[i][-1][-1])]
            
        data_all = sorted((d,i,v,q) for i in data_dec for d,v,q,t in data_dec[i])
    print(fn.now()-t0,'seconds')

    ###########################################################################    
    t0 = fn.now()
    api.setLogLevel('INFO')
    if array:
        qi = 'insert into %s (`data_time`,`att_conf_id`,`value_r`,`quality`,`idx`,`dim_x_r`,`dim_y_r`) VALUES %s'
    else:
        qi = 'insert into %s (`data_time`,`att_conf_id`,`value_r`,`quality`) VALUES %s'
        
    api.info('Inserting %d values into %s' % (len(data_all), ntable))
    while len(data_all):
        api.info('Inserting values into %s (%d pending)' % (ntable, len(data_all)))
        for j in range(100):
            if len(data_all):
                vals = [data_all.pop(0) for i in range(1000) if len(data_all)]
                if array:
                    vals = ','.join(("('%s',%s,%s,%s,%s,%s,%s)"%(d,i,v,q,j,x,y)
                                     ).replace('None','NULL') 
                                for d,i,v,q,j,x,y in vals)
                else:
                    vals = ','.join(("('%s',%s,%s,%s)"%(d,i,v,q)).replace('None','NULL') for d,i,v,q in vals)
                api.Query(qi % (ntable,vals))
    api.setLogLevel(l)
    print(fn.now()-t0,'seconds')
    return

def insert_into_table(api, table, source, start, stop, step = 86400):
    t0 = fn.now()
    
    if fn.isString(api):
        api = pta.api(api)
        
    if fn.isString(start):
        date0,date1 = start,stop
        start,stop = fn.str2time(start),fn.str2time(stop)
    else:
        date0,date1 = fn.time2str(start),fn.time2str(stop)        

    int_time = 'int_time' in api.getTableCols(table)
    q = 'insert into %s (`data_time`,`att_conf_id`,`value_r`,`quality`) ' % table
    q += 'SELECT data_time,att_conf_id,value_r,quality from %s ' % source
    
    for i in range(int(start),int(stop),86400):
        end = min((stop,i+86400))
        if int_time:
            where = 'where int_time between %d and %d order by int_time' % (i,end)
        else:
            where = 'where data_time between "%s" and "%s" order by data_time' % (
                fn.time2str(i),fn.time2str(end))
        print(q+where)
        api.Query(q+where)
        
    print(fn.now()-t0,'seconds')
    return
    

def decimate_partition_by_modtime(api, table, partition, period = 3, 
                       min_count = 30*86400/3, 
                       check = True,
                       start = 0, stop = 0):
    """
    This method uses (data_time|int_time)%period to delete all values with
    module >= 1 only if the remaining data will be bigger than min_count. 
    
    This is as destructive and unchecked method of decimation as
    it is to do a fixed polling; so it is usable only when data length to be kept
    is bigger than (seconds*days/period)
    
    A better method would be to use GROUP BY data_time DIV period; inserting
    the data in another table, then reinserting and repartitioning. But the cost
    in disk and time of that operation would be much bigger.
    """
    t0 = fn.now()
    api = pta.api(api) if fn.isString(api) else api
    print('%s: decimate_partition(%s, %s, %s, %s, %s), current size is %sG' % (
        fn.time2str(), api, table, partition, period, min_count,
        api.getPartitionSize(table, partition)/1e9))

    col = 'int_time' if 'int_time' in api.getTableCols(table) else (
            'CAST(UNIX_TIMESTAMP(data_time) AS INT)' )
    api.Query('drop table if exists tmpdata')
    
    api.Query("create temporary table tmpdata (attid int(10), rcount int(20));")
    q = ("insert into tmpdata select att_conf_id, count(*) as COUNT "
        "from %s partition(%s) " % (table,partition))
    q += " where "+col + "%" + str(period) + " = 0 " 
    if start and stop:
        q += " and %s between %s and %s " % (col, start, stop)
    q += "group by att_conf_id order by COUNT;"
    print(q)
    api.Query(q)

    ids = api.Query("select attid, rcount from tmpdata where rcount > %s order by rcount"
                    % min_count)
    print(ids)
    print('%s: %d attributes have more than %d values' 
          % (fn.time2str(), len(ids), min_count))
    if not len(ids):
        return ids
    
    mx = ids[-1][0]
    print(mx)
    try:
        if ids:
            print('max: %s(%s) has %d values' % (fn.tango.get_normal_name(
                api.get_attribute_by_ID(mx)),mx,ids[-1][1]))
    except:
        traceback.print_exc()

    ids = ','.join(str(i[0]) for i in ids)
    q = ("delete from %s partition(%s) " % (table,partition) + 
        "where att_conf_id in ( %s ) " % ids )
    if start and stop:
        q += " and %s between %s and %s " % (col, start, stop)
    q += "and " + col + "%" + str(period) + " > 0;" 
    print(q)
    api.Query(q)
    print(fn.time2str() + ': values deleted, now repairing')

    api.Query("alter table %s optimize partition %s" % (table, partition))
    nc = api.getPartitionSize(table, partition)
    print(type(nc),nc)
    print(fn.time2str() + ': repair done, new size is %sG' % (nc/1e9))

    q = "select count(*) from %s partition(%s) " % (table,partition)
    q += " where att_conf_id = %s " % mx
    if start and stop:
        q += " and %s between %s and %s " % (col, start, stop)
    nc = api.Query(q)[0][0]
    print('%s: %s data reduced to %s' % (fn.time2str(),mx,nc))
    
    api.Query('drop table if exists tmpdata')

    return ids.split(',')

CURR_YEAR = fn.time2str().split('-')[0]

def decimate_all(api, period, min_count, 
                in_tables="*(array|scalar)*",
                ex_tables="",
                in_partitions="",
                ex_partitions=CURR_YEAR):
    done = []
    for t in sorted(api.getTables()):
        print(t)
        if ex_tables and fn.clsearch(ex_tables,t):
            continue
        if fn.clmatch(in_tables,t):
            for p in sorted(api.getTablePartitions(t)):
                print(p)
                if not p or ex_partitions and fn.clsearch(ex_partitions,p):
                    continue
                if (not in_partitions or fn.clsearch(in_partitions,p)):
                    r = decimate_partition(api, t, p, 
                        period = period, min_count = min_count)
                    done.append((t,p,r))
    return done

def add_int_time_column(api, table):
    pref = pta.query.partition_prefixes(table)
    api.Query('alter table %s add column int_time INT generated always as '
              '(TO_SECONDS(data_time)-62167222800) PERSISTENT;' % table)
    api.Query('drop index att_conf_id_data_time on %s' % table)
    api.Query('create index i%s on %s(att_conf_id, int_time)' % (pref,table))
    return

def get_host_last_partitions(host, user, passwd, 
        exclude_db='information_schema|tdb*'):
    import fandango.db as fdb
    db = fdb.FriendlyDB(host=host,db_name='information_schema',
                        user=user, passwd=passwd)
    result = {}
    for d in db.Query('show databases'):
        if fn.clmatch(exclude_db,d[0]):
            continue
        q = ("select partition_name from partitions where "
            "table_schema = '%s' and partition_name is not NULL "
            "and data_length > 1024 order by partition_name DESC limit 1;"%d)
        r = db.Query(q)
        result[d] = r
    return result

def get_db_last_values_per_table(api, tables = None):
    db = pta.api(api) if fn.isString(api) else api
    tables = dict()
    for t in sorted(db.getTables()):
        if 'data_time' not in db.getTableCols(t):
            continue
        last = get_last_value_in_table(api,t)
        tables[t] = last
    return tables

def get_last_value_in_table(api, table, tref = -180*86400):
    """
    Returns a tuple containing:
    the last value stored in the given table, the size and the time needed
    """
    t0,last,size = fn.now(),0,0
    db = pta.api(api) if fn.isString(api) else api
    print('get_last_value_in_table(%s, %s)' % (db.db_name, table))
    last_part = db.get_last_partition(table)
    tref = tref if tref>0 else fn.now()+tref

    q = 'select UNIX_TIMESTAMP(data_time) from %s ' % table
    if last_part:
        q += ' partition (%s)' % last_part
        size = db.getPartitionSize(table,last_part)
        pt = db.get_partition_time_by_name(last_part)
        if pt not in (0,fn.END_OF_TIME):
            tref = pt
    else:
        size = db.getTableSize(table)

    ids = db.Query("select att_conf_id from att_conf,att_conf_data_type where"
        " data_type like '%s' and att_conf.att_conf_data_type_id = "
        "att_conf_data_type.att_conf_data_type_id" % (table.replace('att_','')))
    ids = list(fn.randomize(i[0] for i in ids))[:5]
    
    where = ' where att_conf_id in (%s) and ' % ','.join(map(str,ids))
    if 'int_time' in db.getTableCols(table):
        where += ('int_time between %d and %d'% (tref, fn.now()))
    elif 'data_time' in db.getTableCols(table):
        where += ("data_time between '%s' and '%s'"
                    % (fn.time2str(tref).split()[0], fn.time2str().split()[0]))
    
    if 'int_time' in db.getTableCols(table):
        order = ' order by int_time desc limit 1'
    elif 'data_time' in db.getTableCols(table):
        order = ' order by data_time desc limit 1'
        
    q = q + where + order

    if ids:
        last = db.Query(q)
        last = fn.first(last[0]) if len(last) else 0

    print('\tlast value at %s, check took %d secs' % (last, fn.now()-t0))
    return (last, size, fn.now()-t0)

def delete_data_older_than(api, table, timestamp, doit=False, force=False):
    if not doit:
        print('doit=False, nothing to be executed')
    if 'archiving04' in api.host and not force:
        raise Exception('deleting on archiving04 is not allowed'
            ' (unless forced)')
    
    query = lambda q: (api.Query(q) if doit else fn.printf(q))
    
    try:
        lg = api.getLogLevel()
        api.setLogLevel('DEBUG')
        partitions = sorted(api.getTablePartitions(table))
        for p in partitions[:]:
            t = api.get_partition_time_by_name(p)
            if t > timestamp:
                query('alter table %s drop partition %s' 
                        % (table, p))
                partitions.remove(p)
            
        cols = api.getTableCols(table)
        if 'int_time' in cols:
            query('delete from %s where int_time > %s' 
                    % (table, timestamp))
        elif 'data_time' in cols:
            query("delete from %s where data_time > '%s'" 
                    % (table, fn.time2str(timestamp)))
        else:
            query("delete from %s where time > '%s'" 
                    % (table, fn.time2str(timestamp)))
        if partitions:
            p = partitions[-1]
            query('alter table %s repair partition %s' % (table,p))
            query('alter table %s optimize partition %s' % (table,p))
        else:
            query('repair table %s' % table)
            query('optimize table %s' % table)
    finally:
        api.setLogLevel(lg)
        
def create_new_partitions(api,table,nmonths,partpermonth=1,
                          start_date=None,add_last=True):
    """
    This script will create new partitions for nmonths*partpermonth
    for the given table and key
    partpermonth should be 1, 2 or 3
    """
    if partpermonth > 3: 
        raise Exception('max partpermonth = 3')

    api = pta.api(api)
    npartitions = nmonths*partpermonth
    tables = pta.hdbpp.query.partition_prefixes
    t = table
    pref = tables[t]
    intcol = 'int_time'
    int_time = intcol in api.getTableCols(table)   
    eparts = sorted(api.getTablePartitions(t))

    if not start_date:
        nparts = [p for p in eparts if '_last' not in p]
        last = api.get_partition_time_by_name(nparts[-1]) if nparts else fn.now()
        nxt = fn.time2date(last)
        if nxt.month == 12:
            nxt = fn.str2time('%s-%s-%s' % (nxt.year+1,nxt.month,'01'))
        else:
            nxt = fn.str2time('%s-%s-%s' % (nxt.year,nxt.month+1,'01'))
        start_date = fn.time2str(nxt).split()[0]

    def inc_months(date,count):
        y,m,d = map(int,date.split('-'))
        m = m+count
        r = m%12
        if r:
            y += int(m/12)
            m = m%12
        else:
            y += int(m/12)-1
            m = 12
        return '%04d-%02d-%02d'%(y,m,d)

    if int_time:
        newc = ("alter table %s add column int_time INT "
            "generated always as (TO_SECONDS(data_time)-62167222800) PERSISTENT;")

        newi = ("drop index att_conf_id_data_time on %s;")
        newi += ("\ncreate index i%s on %s(att_conf_id, int_time);")
        head = "ALTER TABLE %s "
        comm = "PARTITION BY RANGE(int_time) ("
        line = "PARTITION %s%s VALUES LESS THAN (TO_SECONDS('%s')-62167222800)"
    else:
        head = "ALTER TABLE %s "
        comm = "PARTITION BY RANGE(TO_DAYS(data_time)) ("
        line = "PARTITION %s%s VALUES LESS THAN (TO_DAYS('%s'))"

    lines = []

    if int_time and (not api or not intcol in api.getTableCols(t)):
        lines.append(newc%t)
        lines.append(newi%(t,pref,t))

    lines.append(head%t)
    if not any(eparts):
        lines.append(comm)
    elif pref+'_last' in eparts:
        lines.append('REORGANIZE PARTITION %s INTO (' % (pref+'_last'))
    else:
        lines.append('ADD PARTITION (')
    
    counter = 0
    for i in range(0,nmonths):
        date = inc_months(start_date,i)
        end = inc_months(date,1)
        pp = pref+date.replace('-','') #prefix+date
        
        if partpermonth == 1:
            dates = [(date,end)]
            
        elif partpermonth == 2:
            dates = [(date, date.rsplit('-',1)[0]+'-16')
                     (date.rsplit('-',1)[0]+'-16', end)]
            
        elif partpermonth == 3:
            dates = [(date, date.rsplit('-',1)[0]+'-11'),
                (date.rsplit('-',1)[0]+'-11', date.rsplit('-',1)[0]+'-21'),
                (date.rsplit('-',1)[0]+'-21', end)]
            
        #for d in dates:
            #print(p,str(d))
            
        for jdate,jend in dates:
            jdate = jdate.replace('-','')
            l = line%(pref,jdate,jend)
            if counter<(npartitions-1):
                l+=','
            if (pref+jdate) not in eparts:
                lines.append(l)
            counter+=1

    if add_last and pref+'_last' not in eparts or 'REORGANIZE' in str(lines):
        if not lines[-1][-1] in ('(',','):
            lines[-1] += ','
        lines.append('PARTITION %s_last VALUES LESS THAN (MAXVALUE)'%pref)
            
    lines.append(');\n\n')    
    #print('\n'.join(lines))
    return '\n'.join(lines)
        

    

"""
cd $FOLDER
FILENAME=$SCHEMA.full.$(date +%F).dmp

echo "$(fandango time2str) Dump to $FOLDER/$FILENAME..."
mysqldump --single-transaction --force --compact --no-create-db --skip-lock-tables --quick -u manager -p $SCHEMA > $FILENAME
echo "$(fandango time2str) Compressing $FOLDER/$FILENAME"
tar zcvf $FILENAME.tgz $FILENAME
echo "$(fandango time2str) Removing $FOLDER/$FILENAME"
rm $FILENAME

"""


def main(*args,**opts):
    schema = args[0]
    api = pta.api(schema)
    tables = [a for a in api.getTables() if fn.clmatch('att_(scalar|array)_',a)]
    descriptions = dict((t,get_table_description(api,t)) for t in tables)
    partitioned = [t for t,v in descriptions.items() if 'partition' in str(v).lower()]
    print('%s: partitioned tables: %d/%d' % (schema,len(partitioned),len(tables)))
    
if __name__ == '__main__' :
    if sys.argv[1:] and (sys.argv[1]=='help' or sys.argv[1] in locals()):
        r = fn.call(locals_=locals())
        print(r)
    else:
        args,opts = fn.linos.sysargs_to_dict(split=True)
        main(*args,**opts)
    
