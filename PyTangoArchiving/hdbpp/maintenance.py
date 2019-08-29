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

def decimate_partition(api, table, partition, period = 3, 
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

def get_host_last_partitions(host, user, passwd, exclude_db='tdb*'):
    import fandango.db as fdb
    db = fdb.FriendlyDB(host=host,db_name='information_schema',
                        user=user, passwd=passwd)
    result = {}
    for d in db.Query('show databases'):
        print(d[0])
        if fn.clmatch(exclude_db,d[0]):
            continue
        q = ("select partition_name from partitions where "
            "table_schema = '%s' and partition_name is not NULL "
            "and data_length > 1024 order by partition_name DESC limit 1;"%d)
        print(q)
        r = db.Query(q)
        print(r)
        result[d] = r
    return result

def get_table_partitions(api, table, description=''):
    """
    DUPLICATED BY pta.dbs.get_partitions_from_query !!!
    """
    if fn.isString(api): api = pta.api(api)
    if not description: description = get_table_description(api,table)
    rows = [l for l in description.split('\n') if 'partition' in l.lower()]
    f = rows[0].split()[-1]
    data = (f,[])
    for i,l in enumerate(rows[1:]):
        try:
            l,n = l.split(),i and rows[i].split() or [0]*6
            data[-1].append((l[1],n[5],l[5]))
        except:
            print(fn.except2str())
            print(i,l,n)
    return(data)

def create_new_partitions(api,table,preffix,key,npartitions):
    """
    This script will create N new partitions, one for each month
    for the given table and key
    """
    pass
    

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
        print(fn.call(locals_=locals()))
    else:
        args,opts = fn.linos.sysargs_to_dict(split=True)
        main(*args,**opts)
    
