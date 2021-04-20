import sys, traceback, datetime
import fandango as fn
import PyTangoArchiving as pta
from fandango.db import FriendlyDB
import fandango.threads as ft
from fandango import time2str, str2time

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

CURR_YEAR = fn.time2str().split('-')[0]

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

def get_tables_stats(dbs=None,tables=None,period=365*86400):
    """
    obtains counts and frequencies stats from all data tables from all dbs
    """
    dbs = dbs or pta.multi.get_hdbpp_databases()
    result = fn.defaultdict(fn.Struct)
    date = int(fn.clsub('[^0-9]','',fn.time2str().split()[0]))
    if period:
        date0 = int(fn.clsub('[^0-9]','',
                             fn.time2str(fn.now()-period).split()[0]))
    else:
        date0 = 0
    print(date0,date)
    for d in dbs:
        api = pta.api(d)
        dbtables = tables or api.getTables()
        for t in dbtables:
            result[(d,t)].db = d
            result[(d,t)].table = t
            result[(d,t)].partitions = [p for p in api.getTablePartitions(t)
                if date0 < fn.str2int(p) < date]
            result[(d,t)].attributes = (api.get_attributes_by_table(t) 
                if t in api.get_data_tables() else [])
            result[(d,t)].last = (api.get_last_partition(t)
                if t in api.get_data_tables() else '')
            if len(result[(d,t)].partitions) > 1:
                result[(d,t)].size = sum(api.getPartitionSize(t,p)
                                    for p in result[(d,t)].partitions)
                result[(d,t)].rows = sum(api.getPartitionRows(t,p)
                                    for p in result[(d,t)].partitions)
            else:
                result[(d,t)].size = api.getTableSize(t)
                result[(d,t)].rows = api.getTableRows(t)
                
    for k,v in result.items():
        v.partitions = len(v.partitions)
        v.attributes = len(v.attributes)
        v.attr_size = float(v.size)/v.attributes if v.attributes else 0
        v.attr_rows = float(v.rows)/v.attributes if v.attributes else 0
        v.row_size = v.size/v.rows if v.rows else 0
        v.part_size = v.size/v.partitions if v.partitions else 0
        v.row_freq = v.rows/float(period) if period else 0
        v.size_freq = v.size/float(period) if period else 0
        v.attr_freq = v.row_freq/v.attributes if v.attributes else 0
        
    return result

def get_tables_ranges(db,tables=None):
    if fn.isString(db):
        db = pta.api(db)
    tables = tables or db.get_data_tables()
    r = []
    for t in db.get_data_tables():
        tt = db.get_table_timestamp(t,method='max')
        if tt[0] is not None:
            mt = db.get_table_timestamp(t,method='min')
            ps = db.get_partitions_at_dates(t,mt[0],tt[0])
            if ps:
                s = fn.avg([db.getPartitionRows(t,p) for p in ps])
            else:
                s = db.getTableRows(t)
            r.append((t,mt[1].split()[0],tt[1].split()[0],s,ps[-1] if ps else None))
    return r

def decimate_value_list(values,period=None,max_period=3600,method=None,N=1080):
    """
    used by decimate_into_new_table

    values must be a sorted (time,...) array
    it will be decimated in N equal time intervals 
    if method is not provided, only the first value of each interval will be kept
    if method is given, it will be applied to buffer to choose the value to keep
    first value of buffer will always be the last value kept
    """
    ## THIS METHOD IS A SIMPLIFICATION OF fandango.arrays.filter_array!
    # it allows rows to be as long as needed (if [0]=time and [1]=value)
    # (just check which is faster)

    if not len(values):
        return []
    
    if not period:
        tmin,tmax = (values[0][0],values[-1][0])
        period = float(tmax-tmin)/N

    end = len(values)-1
    
    if method is None:
        result = [values[0]]
        for i,v in enumerate(values):
            tdiff = v[0]-result[-1][0]
            #if i==end #distorts everything
            if tdiff>=max_period or (
                    v[1]!=result[-1][1] and tdiff>=period):
                result.append(v)
    else:
        result, buff = [],[]
        ref = values[0]
        for i,v in enumerate(values):
            tdiff = v[0]-ref[0]
            #if (i==end or #distorts!
            if (tdiff>=period) and len(buff):
                #if i==end: buff.insert(0,ref)
                v = list(buff[-1])
                buff = [t[1] for t in buff]
                v[1] = method(buff,ref[1])
                #print(fn.time2str(v[0]),ref,max(buff),min(buff),len(buff),method,v[1])
                result.append(tuple(v))
                ref = result[-1]
                buff = []
            else:
                #distinc values not filtered to not alter averages
                buff.append(v) 

    if not result:
        result = [values[-1]]
    #print(tmin,tmax,N,interval,len(values),len(result),method)
    return result
    
def decimate_table_inline(db, table, attributes = [], 
                   start = 0, stop = -1, partition = '', 
                   trange = 3, fmargin = 1):
    """
    BAD! Use decimate_into_new_db/table instead!

    @TODO
    This method decimates the table inline using MOD by int_time
    
    This is inefficient with MyISAM
    """
    db = pta.api(db) if fn.isString(db) else db
    l,_ = db.getLogLevel(),db.setLogLevel('DEBUG')
    t0 = fn.now()
    
    int_time = 'int_time' in db.getTableCols(t)
    
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

def decimate_into_new_db(db_in, db_out, min_period = 3, min_array_period = 10,
                         max_period = 3600, begin=None, end=None,
                         tables = None, method=None, 
                         remove_nones=True,
                         server_dec = True, 
                         bunch=86400/4,
                         use_files=True,
                         force_interval=False):
    if tables is None:
        tables = db_in.get_data_tables() #pta.hdbpp.query.partition_prefixes.keys()

    done = []
    for i,table in enumerate(sorted(tables)):
        print('%s decimating table %s.%s (%d/%d)' 
              % (fn.time2str(),db_in.db_name,table,i+1,len(tables)))
    
        begin = fn.str2time(begin) if fn.isString(begin) else begin
        end = fn.str2time(end) if fn.isString(end) else end
        
        tbegin = get_last_value_in_table(db_out,table,ignore_errors=True)[0]
        if not tbegin:
            tbegin = get_first_value_in_table(db_in,table,ignore_errors=True)[0]
        print(begin,tbegin)
        if force_interval:
            tbegin = begin
        elif begin is not None:
            tbegin = max((begin,tbegin)) #Query may start later

        tend = get_last_value_in_table(db_in,table,ignore_errors=True)[0]
        print(end,tend)
        if force_interval:
            tend = end
        elif end is not None:
            tend = min((end,tend)) #Query may finish earlier
        if tend is None:
            tend = tbegin
        print(end,tend)
            
        print('%s, syncing %s,%s to %s,%s' % (table,
            tbegin,fn.time2str(tbegin),tend,fn.time2str(tend)))

        if tend and tbegin and tend-tbegin < 600:
            db_out.warning('%s Tables already synchronized' % table)
            continue

        if 'array' in table:
            period = min_array_period
        else:
            period = min_period
        
        try:
            db_out.warning('Disabling keys on %s' % table)
            db_out.Query("ALTER TABLE `%s` DISABLE KEYS;" % table)

            try:
                decimate_into_new_table(db_in,db_out,table,
                    tbegin,tend,min_period=period, max_period = max_period,
                    method = method,remove_nones = remove_nones,
                    server_dec = server_dec, bunch = bunch,
                    use_files = use_files)

                done.append(table)
            finally:
                db_out.warning('Reenabling keys on %s' % table)
                db_out.Query("ALTER TABLE `%s` ENABLE KEYS;" % table)

        except:
            print(fn.time2str())
            traceback.print_exc()
            
    return done

def decimate_into_new_table(db_in, db_out, table, start, stop, ntable='', 
        min_period=1, max_period=3600, bunch=86400/4, suffix='_dec',
        drop=False, method=None, 
        remove_nones=True,
        server_dec=True, insert=True, use_files=True, use_process=True,
        ):
    """
    decimate by distinct value, or by fix period
    accept a min_resolution argument
    do selects in bunches 6 hours
    db_in is the origin database; db_out is where decimated data will be stored
    if db_in == db_out; then a temporary table with suffix is created

    bunch: queries will be split in bunch intervals
    """
    t0 = fn.now()
    # bigger = sorted((db.getTableSize(t),t) for t in dbr.getTables() 
    #   if 'scalar' in t)[-1]
    if (db_in.host,db_in.db_name) != (db_out.host,db_out.db_name):
        suffix = ''
    if not ntable:
        ntable = table+suffix
    l = db_in.getLogLevel()
    nattrs = len(db_in.get_attributes_by_table(table))
    
    if fn.isString(start):
        date0,date1 = start,stop
        start,stop = fn.str2time(start),fn.str2time(stop)
    else:
        date0,date1 = fn.time2str(start),fn.time2str(stop)
        
    db_in.setLogLevel('INFO')
    db_out.setLogLevel('INFO')               
        
    print('decimate_into_new_table(%s.%s => %s@%s.%s, %s to %s): periods (%s,%s)' % 
        (db_in.db_name,table,db_out.db_name,db_out.host,ntable,date0,date1,
         min_period,max_period))
    try:
        cpart = (db_in.get_partitions_at_dates(table,begin) or [None])[0]
        print('%s.%s.%s size = %s' % 
              (db_in,table,cpart,db.getPartitionSize(table,cpart)))
    except: 
        cpart = None
        
    # BUNCHING PROCEDURE!
    if stop-start > bunch:
        # Decimation done in separate processes to not overload memory
        # at the end, it returns values
        i = 0
        rs = (0,0,0,0,0) if insert else [] #tquery, tdec, tinsert, len(data), len(dec)
        while start+i*bunch < stop:
            r = ft.SubprocessMethod(decimate_into_new_table, db_in, db_out, table,
                start = start+i*bunch,stop = start+(i+1)*bunch,
                ntable=ntable, min_period=min_period, max_period=max_period,
                bunch=bunch, suffix=suffix, drop=drop, 
                server_dec=server_dec,insert=insert,use_process=False,
                timeout=3*3600,
                )
            i+=1 
            if insert:
                rs = tuple(map(sum,zip(rs,r)))
            else:
                rs.extend(rs)

        if insert:
            tquery,tdec,tinsert,ldata,ldec = rs
            print('%s.%s[%s] => %s.%s[%s] (tquery=%s,tdec=%s,tinsert=%s)' % (
                db_in.db_name,table,ldata,db_out.db_name,ntable,ldec,
                    tquery,tdec,tinsert))        

        return rs  
    
    ###########################################################################
    # PROCESS OF DECIMATION FOR EACH INDIVIDUAL BUNCH:
    ###########################################################################
    
    tables = db_out.getTables()
    
    if drop:
        db_out.Query('drop table if exists %s' % ntable)
    
    # Create Table if it doesn't exist
    if ntable not in tables:
        code = db_in.getTableCreator(table)
        qi = code.split('/')[0].replace(table,ntable)
        try:
            db_out.Query(qi)
        except Exception as e:
            db_in.warning('Unable to create table %s' % ntable)
            print(e)

    # Create partitions if they doesn't exist
    pass

    try:
        # Create Indexes if they doesn't exit
        add_int_time_column(db_out, table)
        # array index
        if 'array' in table:
            add_idx_index(db_out, table) #This method already checks if exists
    except:
        traceback.print_exc()

    ###########################################################################
    # Getting the data
    
    t0 = fn.now()
    db_in.info('Get %s values between %s and %s' % (table, date0, date1))
    aggr = 'value_r'
    if server_dec:
        if method in (max,'max'): aggr = 'max(value_r)'
        elif method in (min,'min'): aggr = 'min(value_r)'
        elif method in (fn.arrays.average,'avg'): aggr = 'avg(value_r)'

    what = ("att_conf_id,data_time,%s,quality"%aggr).split(',')
    # converting  times on mysql as python seems to be very bad at
    # converting datetime types
    float_time = "CAST(UNIX_TIMESTAMP(data_time) AS DOUBLE)"
    what.append(float_time)
    array = 'idx' in db_in.getTableCols(table)
    if array:
        what.extend('idx,dim_x_r,dim_y_r'.split(','))
        
    #-------------------------------------------------------------------------   
    attrs = db_in.get_attributes_by_table(table)
    ids = [db_in.get_attr_id_type_table(a)[0] for a in attrs]
    data = []
    #  QUERYING THE DATA PER ATTRIBUTE IS MUUUUUCH FASTER!
    for i,a in enumerate(attrs):
        q = db_in.get_attribute_values_query(a,
            what = ','.join(what),
            where = '',
            start_date = start,
            stop_date = stop,
            decimate = min_period if server_dec else 0,
            )
            
        if not i: print(q)

        if use_process:
            dd = ft.SubprocessMethod(db_in.Query,q,timeout=1800)
        else:
            dd = db_in.Query(q)
        db_in.info('%s values [%d] (%2.3f values/second)' 
                % (fn.tango.get_normal_name(a),len(dd),len(dd)/(stop-start)))
        data.extend(dd)

    ldata = len(data)
    tquery = fn.now()-t0

    ###########################################################################
    # Decimating
    
    t0 = fn.now()
    
    # Splitting data into attr or (attr,idx) lists
    # Creating empty dictionaries to store decimated data
    data_ids = fn.defaultdict(lambda:fn.defaultdict(list))
    # i,j : att_id, idx
    
    # Do not remove nones when using server-side decimation!
    tlimit = fn.now()+86400
    if 'array' in table or not server_dec or remove_nones:
        # Nones are inserted only if using server_decimation on scalars
        if array:
            [data_ids[aid][idx].append((t,v,d,q,x,y)) for aid,d,v,q,t,idx,x,y in data if v is not None
             and 1e9 < t < tlimit];
        else:
            [data_ids[aid][None].append((t,v,d,q)) for aid,d,v,q,t in data if v is not None
             and 1e9 < t < tlimit];
    else:
        if array:
            [data_ids[aid][idx].append((t,v,d,q,x,y)) for aid,d,v,q,t,idx,x,y in data
             and 1e9 < t < tlimit];
        else:
            [data_ids[aid][None].append((t,v,d,q)) for aid,d,v,q,t in data
             and 1e9 < t < tlimit];
       
    db_in.info('Decimating %d values, period = (%s,%s,[%s]), server_dec = %s' % 
               (len(data),min_period,max_period,bunch,server_dec))
    #print(data)
    #print(data_ids)

    data_dec = {}  
    for kk,vv in data_ids.items():
        data_dec[kk] = {}
        for k,v in vv.items():
            if server_dec: # and int(server_dec) == int(min_period):
                #if kk == data_ids.keys()[0]:
                    #print(kk,k,len(v))
                data_dec[kk][k] = v
            else:
                data_dec[kk][k] = decimate_value_list(v,
                    period=min_period, max_period=max_period, method=method)
            
    if array:
        # TODO: idx should go before value_r!!!
        #data_all = sorted((d,aid,v,q,idx,x,y) for aid in data_dec
        data_all = sorted((aid,idx,d,v,q,x,y) for aid in data_dec 
            for idx in data_dec[aid] for t,v,d,q,x,y in data_dec[aid][idx])
    else:
        #data_all = sorted((d,i,v,q) for i in data_dec for t,v,d,q in data_dec[i][None])
        data_all = sorted((i,d,v,q) for i in data_dec for t,v,d,q in data_dec[i][None])
            
    print(len(data_all))
    tdec = fn.now()-t0
    ldec = len(data_all)
    t0 = fn.now()
    
    if insert:
        if use_files:
            filename = '/tmp/%s.%s.bulk' % (db_out.db_name,ntable)
            columns = 'att_conf_id'
            if array:
                columns += ',idx'
            columns += ',data_time,value_r,quality'
            if array: 
                columns += ',dim_x_r,dim_y_r'
                if ntable.endswith('_rw'):
                    columns += ',dim_x_w,dim_y_w'
            insert_into_csv_file(data_all,columns,ntable,filename)
            load_from_csv_file(db_out,ntable,columns,filename)
        elif use_process:
            r = ft.SubprocessMethod(
                insert_into_new_table,db_out,ntable,data_all
                ,timeout = 1800)
        else:
            r = insert_into_new_table(db_out,ntable,data_all)
            
    tinsert = fn.now()-t0
    
    try:
        cpart = (db_out.get_partitions_at_dates(ntable,begin) or [None])[0]
        r = db.getPartitionSize(ntable,cpart)
        print('%s.%s.%s new size = %s' % 
            (db_out,ntable,cpart,r))
    except: 
        cpart = None    
        r = 0
        
    print('%s.%s[%s] => %s.%s[%s] (tquery=%s,tdec=%s,tinsert=%s)' % (
        db_in.db_name,table,len(data),db_out.db_name,ntable,ldec,
            tquery,tdec,tinsert))
    
    if insert:
        return tquery,tdec,tinsert,len(data),ldec
    else:
        return data_all
    
def insert_into_csv_file(data, columns, table, filename):

    t0 = fn.now()
    if fn.isString(columns):
        columns = columns.split(',')

    str_cols = [i for i,c in enumerate(columns) if 'data_time' in c 
                or ('value' in c and 'str' in table)]

    r = 0
    try:
        f = open(filename,'w')
        #f.write(','.join(columns) + '\n')
        
        for t in data:
            l = []
            for i,c in enumerate(columns):
                if i>=len(t):
                    l.append('"0"')
                else:
                    v = str(t[i])
                    #if v is None or i not in str_cols:
                        #l.append(str(v))
                    #else:
                    if 'str' in table and 'value' in c:
                        v = v.replace('"','').replace("'",'').replace(' ','_')[:80]

                    if c == 'data_time':
                        v = v.replace(' ','T')
                        
                    if v == 'None':
                        v = 'NULL'
                        
                    l.append('"%s"' % v)
            f.write(','.join(l) + '\n')
            r+=1
    except:
        traceback.print_exc()
    finally:
        f.close()
        
    tinsert = fn.now() - t0
    print('%d/%d values written to %s in %f seconds' % (r,len(data),filename,tinsert))
    return len(data),tinsert


def load_from_csv_file(api, table, columns, filename):
    if fn.isSequence(columns):
        columns = ','.join(columns)
    api.Query("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' "
              "ENCLOSED BY '\"' (%s);" % (filename,table,columns))
    return filename
    
                
                
            #if array:
                #d,i,v,q,j,x,y = t
            #else:
                #d,i,v,q = t

            #if 'string' in ntable and v is not None:
                #v = v.replace('"','').replace("'",'')[:80]
                #if '"' in v:
                    #v = "'%s'" % v
                #else:
                    #v = '"%s"' % v

            #if array:
                #if ntable.endswith('_rw'):
                    #s = "('%s',%s,%s,%s,%s,%s,%s,0,0)"%(d,i,v,q,j,x,y)
                #else:
                    #s = "('%s',%s,%s,%s,%s,%s,%s)"%(d,i,v,q,j,x,y)
            #else:
                #s = "('%s',%s,%s,%s)"%(d,i,v,q)

            #svals.append(s.replace('None','NULL'))

                #db_out.Query(qi % (ntable,','.join(svals)))        


def insert_into_new_table(db_out, ntable, data_all):
    ###########################################################################    
    # Inserting into database
    
    db_out.setLogLevel('INFO')
    array = 'array' in ntable
    
    if array:
        qi = 'insert into %s (`data_time`,`att_conf_id`,`value_r`,`quality`'
        qi = qi % ntable
        if ntable.endswith('_rw'):
            qi += ',`idx`,`dim_x_r`,`dim_y_r`,`dim_x_w`,`dim_y_w`) VALUES ' #%s'
            qi += "(%s,%s,%s,%s,%s,%s,%s,0,0)" #%s,%s)'
            
        else:
            qi += ',`idx`,`dim_x_r`,`dim_y_r`) VALUES ' #%s'
            qi += "(%s,%s,%s,%s,%s,%s,%s)"

    else:
        qi += ") VALUES (%s,%s,%s,%s)" #%s'
        
    db_out.info('Inserting %d values into %s' % (len(data_all), ntable))
    while len(data_all):
        #printout every 500 bunches
        bunch_size = 200
        db_out.info('Inserting values into %s (%d pending)' 
                    % (ntable, len(data_all)))

        for j in range(500):
            if len(data_all):
                vals = [] #data_all.pop(0) for i in range(bunch_size) if len(data_all)]
                for i in range(bunch_size):
                    if len(data_all):
                        v = data_all.pop(0)
                        vals.append(v)

                if db_out.db.__module__ == 'mysql.connector.connection':
                    cursor = db_out.db.cursor(prepared=True)
                else:
                    cursor = db_out.getCursor()

                cursor.executemany(qi,vals)
                db_out.db.commit()
                cursor.close()

    return

def compare_two_databases(db_in,db_out):
    ups0, ups1 = {},{}
    for t in sorted(db_in.get_data_tables()):
        ups0[t] = db_in.get_table_timestamp(t)
        ups1[t] = db_out.get_table_timestamp(t)
        if ups0[t][0] > 100:
            print('%s: %s:\t%s\t=>\t%s:\t%s\t:\t%s' %
                (t,db_in.db_name,ups0[t][1],db_out.db_name,ups1[t][1],
                 (ups0[t][0] or 0)-(ups1[t][0] or 0)))
    return dict((t,(ups0[t],ups1[t])) for t in ups0)

def plot_two_arrays(arr1,arr2,start=None,stop=None):
    import matplotlib.pyplot as plt
    plt.figure()
    x0,x1 = min((arr1[0][0],arr2[0][0])),max((arr1[-1][0],arr2[-1][0]))
    y0,y1 = min(t[1] for t in arr1+arr2),max(t[1] for t in arr1+arr2)
    plt.subplot(131)
    plt.plot([v[0] for v in arr1],[v[1] for v in arr1])
    plt.axis([x0,x1,y0,y1])
    plt.subplot(132)
    plt.plot([v[0] for v in arr2],[v[1] for v in arr2])
    plt.axis([x0,x1,y0,y1])
    plt.show()

def copy_between_tables(api, table, source, start, stop, step = 86400):
    
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
    

# def decimate_partition_by_modtime(api, table, partition, period = 3,
#                        min_count = 30*86400/3,
#                        check = True,
#                        start = 0, stop = 0):
#     """
#     This method uses (data_time|int_time)%period to delete all values with
#     module >= 1 only if the remaining data will be bigger than min_count.
#
#     This is as destructive and unchecked method of decimation as
#     it is to do a fixed polling; so it is usable only when data length to be kept
#     is bigger than (seconds*days/period)
#
#     A better method would be to use GROUP BY data_time DIV period; inserting
#     the data in another table, then reinserting and repartitioning. But the cost
#     in disk and time of that operation would be much bigger.
#     """
#     t0 = fn.now()
#     api = pta.api(api) if fn.isString(api) else api
#     print('%s: decimate_partition(%s, %s, %s, %s, %s), current size is %sG' % (
#         fn.time2str(), api, table, partition, period, min_count,
#         api.getPartitionSize(table, partition)/1e9))
#
#     col = 'int_time' if 'int_time' in api.getTableCols(table) else (
#             'CAST(UNIX_TIMESTAMP(data_time) AS INT)' )
#     api.Query('drop table if exists tmpdata')
#
#     api.Query("create temporary table tmpdata (attid int(10), rcount int(20));")
#     q = ("insert into tmpdata select att_conf_id, count(*) as COUNT "
#         "from %s partition(%s) " % (table,partition))
#     q += " where "+col + "%" + str(period) + " = 0 "
#     if start and stop:
#         q += " and %s between %s and %s " % (col, start, stop)
#     q += "group by att_conf_id order by COUNT;"
#     print(q)
#     api.Query(q)
#
#     ids = api.Query("select attid, rcount from tmpdata where rcount > %s order by rcount"
#                     % min_count)
#     print(ids)
#     print('%s: %d attributes have more than %d values'
#           % (fn.time2str(), len(ids), min_count))
#     if not len(ids):
#         return ids
#
#     mx = ids[-1][0]
#     print(mx)
#     try:
#         if ids:
#             print('max: %s(%s) has %d values' % (fn.tango.get_normal_name(
#                 api.get_attribute_by_ID(mx)),mx,ids[-1][1]))
#     except:
#         traceback.print_exc()
#
#     ids = ','.join(str(i[0]) for i in ids)
#     q = ("delete from %s partition(%s) " % (table,partition) +
#         "where att_conf_id in ( %s ) " % ids )
#     if start and stop:
#         q += " and %s between %s and %s " % (col, start, stop)
#     q += "and " + col + "%" + str(period) + " > 0;"
#     print(q)
#     api.Query(q)
#     print(fn.time2str() + ': values deleted, now repairing')
#
#     api.Query("alter table %s optimize partition %s" % (table, partition))
#     nc = api.getPartitionSize(table, partition)
#     print(type(nc),nc)
#     print(fn.time2str() + ': repair done, new size is %sG' % (nc/1e9))
#
#     q = "select count(*) from %s partition(%s) " % (table,partition)
#     q += " where att_conf_id = %s " % mx
#     if start and stop:
#         q += " and %s between %s and %s " % (col, start, stop)
#     nc = api.Query(q)[0][0]
#     print('%s: %s data reduced to %s' % (fn.time2str(),mx,nc))
#
#     api.Query('drop table if exists tmpdata')
#
#     return ids.split(',')

def add_int_time_column(api, table,do_it=True):
    # Only prefixed tables will be modified
    pref = pta.hdbpp.query.partition_prefixes.get(table,None)
    r = []
    if not pref:
        return 

    if 'int_time' not in api.getTableCols(table):
        q = ('alter table %s add column int_time INT generated always as '
                '(TO_SECONDS(data_time)-62167222800) PERSISTENT;' % table)
        if do_it: 
            print(q)
            api.Query(q)
        r.append(q)

    if not any('int_time' in idx for idx in api.getTableIndex(table).values()):
        q = 'drop index att_conf_id_data_time on %s' % table
        if do_it: 
            print(q)
            api.Query(q)
        r.append(q)
        q = ('create index i%s on %s(att_conf_id, int_time)' % (pref,table))
        if do_it: 
            print(q)
            api.Query(q)
        r.append(q)
        
    return '\n'.join(r)

def add_idx_index(api, table, do_it=True):
    try:
        if not 'idx' in api.getTableCols(table):
            return ''
        if any('idx' in ix for ix in api.getTableIndex(table).values()):
            return ''
        pref = pta.hdbpp.query.partition_prefixes.get(table,None)
        if not pref:
            return ''
        it = 'int_time' if 'int_time' in api.getTableCols(table) else 'data_time'
        #q = ('create index ii%s on %s(att_conf_id, idx, %s)' % (pref,table,it))
        # old index (aid/time) should go first!
        q = ('create index ii%s on %s(att_conf_id, idx, %s)' % (pref,table,it))
        if do_it: 
            print(api.db_name,q)
            api.Query(q)
        return q
    except:
        traceback.print_exc()
    
from PyTangoArchiving.hdbpp.query import MIN_FILE_SIZE

def get_db_last_values_per_table(api, tables = None):
    db = pta.api(api) if fn.isString(api) else api
    tables = dict()
    for t in sorted(db.getTables()):
        if 'data_time' not in db.getTableCols(t):
            continue
        last = get_last_value_in_table(api,t)
        tables[t] = last
    return tables

def get_last_value_in_table(api, table, method='max', 
                            ignore_errors = False,
                            trace = False): #, tref = -180*86400):
    """
    DEPRECATED, USE API.get_table_timestamp instead

    Returns a tuple containing:
    the last value stored in the given table, in epoch and date format
    """
    return api.get_table_timestamp(table, method, ignore_errors=ignore_errors)

    # t0,last,size = fn.now(),0,0
    # db = pta.api(api) if fn.isString(api) else api
    # #print('get_last_value_in_table(%s, %s)' % (db.db_name, table))
    #
    # int_time = any('int_time' in v for v in db.getTableIndex(table).values())
    #
    # # If using UNIX_TIMESTAMP THE INDEXING FAILS!!
    # field = 'int_time' if int_time else 'data_time'
    # q = 'select %s(%s) from %s ' % (method,field,table)
    #
    # size = db.getTableSize(table)
    # ids = db.get_attributes_by_table(table,as_id=True)
    # r = []
    #
    # for i in ids:
    #     qi = q+' where att_conf_id=%d' % i
    #     #if tref and int_time: where += ('int_time <= %d'% (tref))
    #     rr = db.Query(qi)
    #     if trace:
    #         print('%s[%s]:%s' % (table,i,rr))
    #     r.extend(rr)
    #
    # method = {'max':max,'min':min}[method]
    # r = [db.mysqlsecs2time(l[0]) if int_time else fn.date2time(l[0])
    #      for l in r if l[0] not in (0,None)]
    # r = [l for l in r if l if (ignore_errors or 1e9<l<fn.now())]
    #
    # if len(r):
    #     last = method(r) if len(r) else 0
    #     date = fn.time2str(last)
    # else:
    #     db.warning('No values in %s' % table)
    #     last, date = None, ''
    #
    # return (last, date, size, fn.now() - t0)    t0,last,size = fn.now(),0,0
    # db = pta.api(api) if fn.isString(api) else api
    # #print('get_last_value_in_table(%s, %s)' % (db.db_name, table))
    #
    # int_time = any('int_time' in v for v in db.getTableIndex(table).values())
    #
    # # If using UNIX_TIMESTAMP THE INDEXING FAILS!!
    # field = 'int_time' if int_time else 'data_time'
    # q = 'select %s(%s) from %s ' % (method,field,table)
    #
    # size = db.getTableSize(table)
    # ids = db.get_attributes_by_table(table,as_id=True)
    # r = []
    #
    # for i in ids:
    #     qi = q+' where att_conf_id=%d' % i
    #     #if tref and int_time: where += ('int_time <= %d'% (tref))
    #     rr = db.Query(qi)
    #     if trace:
    #         print('%s[%s]:%s' % (table,i,rr))
    #     r.extend(rr)
    #
    # method = {'max':max,'min':min}[method]
    # r = [db.mysqlsecs2time(l[0]) if int_time else fn.date2time(l[0])
    #      for l in r if l[0] not in (0,None)]
    # r = [l for l in r if l if (ignore_errors or 1e9<l<fn.now())]
    #
    # if len(r):
    #     last = method(r) if len(r) else 0
    #     date = fn.time2str(last)
    # else:
    #     db.warning('No values in %s' % table)
    #     last, date = None, ''
    #
    # return (last, date, size, fn.now() - t0)

def get_first_value_in_table(api, table, ignore_errors=False):
    """
    DEPRECATED, USE API.get_table_timestamp instead
    """
    return get_last_value_in_table(api, table, method='min',ignore_errors=ignore_errors)

def delete_att_parameter_entries(api,timestamp=None):
    """
    att_parameter table tends to grow and slow down startup of archivers
    """
    api = pta.api(api) if fn.isString(api) else api
    timestamp = timestamp or fn.now()-3*30*86400
    api.Query("delete from att_parameter where insert_time < '%s'" 
              % fn.time2str(timestamp))
    api.Query("optimize table att_parameter")
    return api.getTableSize('att_parameter')

def delete_data_older_than(api, table, timestamp, doit=False, force=False):
    delete_data_out_of_time(api, table, timestamp, fn.END_OF_TIME, doit, force)

def delete_data_out_of_time(api, table, tstart=1e9, tstop=None, doit=False, force=False):
    if not doit:
        print('doit=False, nothing to be executed')
    if 'archiving04' in api.host and not force:
        raise Exception('deleting on archiving04 is not allowed'
            ' (unless forced)')

    timestamp = fn.str2time(tstart) if fn.isString(tstart) else tstart
    tstop = fn.str2time(tstop) if fn.isString(tstop) else fn.notNone(tstop,fn.now()+86400)
    query = lambda q: (api.Query(q) if doit else fn.printf(q))
    
    try:
        lg = api.getLogLevel()
        api.setLogLevel('DEBUG')
        partitions = sorted(api.getTablePartitions(table))
        for p in partitions[:]:
            t = api.get_partition_time_by_name(p)
            if t < timestamp:
                query('alter table %s drop partition %s' 
                        % (table, p))
                partitions.remove(p)
            
        cols = api.getTableCols(table)
        col = (c for c in ('int_time','data_time','time') if c in cols).next()
        if col != 'int_time':
            timestamp,tstop = "'%s'" % timestamp, "'%s'"%tstop
        q = 'delete from %s where %s < %s' % (table, col, timestamp)

        if tstop != fn.END_OF_TIME:
            q += " and %s < %s " (col, tstop)
        query(q)

        if partitions:
            p = partitions[-1]
            query('alter table %s repair partition %s' % (table,p))
            query('alter table %s optimize partition %s' % (table,p))
        else:
            query('repair table %s' % table)
            query('optimize table %s' % table)
    finally:
        api.setLogLevel(lg)

def filter_from_epoch(epoch=None):
    t = epoch or fn.now()
    year,month = fn.time2str(t).split('-')[:2]
    return '%s%s' % (year,month)

def check_db_partitions(api,year='',month='',max_size=128*1e9/10):
    """
    year and month, strings to match on existing partitions
    e.g. '202102[0-9][0-9]' will match any partition from february

    :param api:
    :param filter:
    :return:
    """
    result = fn.Struct(db_name=api.db_name)
    nextf = '%s%s' % (year,month) if all((year,month)) else filter_from_epoch(fn.now()+365*86400)
    filter = '%s%s' % (year,month) if all((year,month)) else filter_from_epoch()
    tables = api.get_data_tables()
    sizes = dict(fn.kmap(api.getTableSize,tables))
    parts = dict(fn.kmap(api.getTablePartitions,tables))
    bigs = dict((t,max(api.getPartitionSize(t,p) for p in parts[t])) for t in tables if parts[t])
    match = dict((t,[p for p in parts[t] if fn.clsearch(filter,p)]) for t in tables)

    result.wrong = dict((t,[p for p in parts[t]
        if not p.startswith(pta.hdbpp.query.partition_prefixes[t])]) for t in tables)

    result.sizes, result.parts, result.match, result.bigs = sizes, parts, match, bigs
    result.miss = [t for t in tables if len(parts[t])
                   and any(fn.clsearch(nextf,p) for p in parts[t])]
    result.noparts = [t for t in tables if sizes[t]>max_size and not len(parts[t])]
    result.toobigs = [t for t in tables if bigs.get(t,0)>max_size and len(match[t])<2]
    result.nolasts = [t for t in tables if len(parts[t])
        and not any(p.endswith('_last') for p in parts[t])]

    print('%s: tables with no partitions: %s' % (result.db_name,result.noparts))
    print('%s: tables with no %s partition: %s' % (result.db_name, nextf, result.miss))
    print('%s: tables with too big partitions: %s' % (result.db_name, result.toobigs))
    print('%s: tables with no _last partition: %s' % (result.db_name, result.nolasts))
    print('%s: tables with wrong prefixes: %s' % (result.db_name,[t for t in result.wrong.items() if t[1]]))

    return result

def create_new_partitions(api,table,nmonths,partpermonth=1,
                          start_date=None,add_last=True,do_it=False):
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
    pref = tables.get(t,None)
    if not pref:
        print('table %s will not be partitioned' % t)
        return []
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
            dates = [(date, date.rsplit('-',1)[0]+'-16'),
                     (date.rsplit('-',1)[0]+'-16', end)]
            
        elif partpermonth == 3:
            dates = [(date, date.rsplit('-',1)[0]+'-11'),
                (date.rsplit('-',1)[0]+'-11', date.rsplit('-',1)[0]+'-21'),
                (date.rsplit('-',1)[0]+'-21', end)]
            
        #for d in dates:
            #print(p,str(d))
            
        for jdate,jend in dates:
            jdate = jdate.replace('-','')
            pname = (pref+jdate)
            l = line%(pref,jdate,jend)
            if counter<(npartitions-1):
                l+=','
            if not eparts or (pname not in eparts and not pname < eparts[0]):
                lines.append(l)
            counter+=1

    if add_last and pref+'_last' not in eparts or 'REORGANIZE' in str(lines):
        if not lines[-1][-1] in ('(',','):
            lines[-1] += ','
        lines.append('PARTITION %s_last VALUES LESS THAN (MAXVALUE)'%pref)
            
    lines.append(');\n\n') 
    r = '\n'.join(lines)
    if do_it:    
        print('Executing query .... %s' % r)
        api.Query(r)
    
    return r

def get_archiving_loads(schema,maxload=250):
    r = fn.Struct()
    if isinstance(schema,pta.hdbpp.HDBpp):
        api,r.schema = schema,schema.db_name
    else:
        api,r.schema = pta.api(schema),schema
    r.attrs = api.get_attributes()
    r.subs = api.get_subscribers()
    r.pers = api.get_periodic_archivers()
    r.evsubs = [d for d in api.get_subscribers() if 'null' not in d]
    r.nulls = [d for d in r.subs if 'null' in d]
    r.subsloads = dict((d,api.get_archiver_attributes(d)) for d in r.subs)
    r.subserrors = dict((d,api.get_archiver_errors(d)) for d in r.evsubs) 
    r.persloads = dict((d,api.get_periodic_archiver_attributes(d)) for d in r.pers)
    r.perserrors = dict((d,api.get_periodic_archiver_errors(d)) for d in r.pers)
    r.perattrs = api.get_periodic_attributes()
    r.pernoevs = [a for a in r.perattrs if not fn.tango.check_attribute_events(a)]
    r.perevs = [a for a in r.perattrs if a not in r.pernoevs]
    r.attrlists = dict((d,fn.get_device_property(d,'AttributeList'))
                                   for d in r.subs)
    r.perlists = dict((d,fn.get_device_property(d,'AttributeList'))
                                   for d in r.pers)
    r.subattrs = [a.split(';')[0] for v in r.attrlists.values() for a in v]
    r.evattrs = [a.split(';')[0] for v in r.subattrs if v not in r.pernoevs]
    r.miss = [a for a in r.attrs if a not in r.subattrs]
    r.dubs = len(r.subattrs)-len(list(set(r.subattrs)))
    r.both = r.perevs
    print('%d attributes in %s schema' % (len(r.attrs),schema))
    dbsize = api.getDbSize()
    print('DbSize: %f' % (dbsize/1e9))
    tspan = api.get_timespan()
    print('%s - %s ; %2.1f G/day' % (fn.time2str(tspan[0]),fn.time2str(tspan[1]),
        (dbsize/1e9)/((tspan[1]-tspan[0])/86400)))
    print('%d repeated attributes in archiver lists' % r.dubs)
    print('%d not on any archiver' % len(r.miss))
    print('%d on event archiving' % len(r.evattrs))
    print('%d on periodic archiving' % len(r.perattrs))
    print('%d(%d) have both' % (len(r.perattrs)-len(r.pernoevs),len(r.both)))
    print('')
    for k,v in sorted(r.subsloads.items()):
        print('%s: %d (%d errors)' % (k,len(v),len(r.subserrors.get(k,[]))))
    for k,v in sorted(r.persloads.items()):
        print('%s: %d (%d errors)' % (k,len(v),len(r.perserrors.get(k,[]))))
    return r
        
    

def redistribute_loads(schema,maxload=300,subscribers=True,periodics=True,
                       do_it=True):
    """
    It moves periodic attributes to a /null subscriber
    Then tries to balance load between archivers
    """
    if isinstance(schema,pta.hdbpp.HDBpp):
        api,schema = schema,schema.db_name
    else:
        api,schema = pta.api(schema),schema
    subs = api.get_subscribers()
    nulls = [d for d in subs if 'null' in d]
    if not nulls:
        api.add_event_subscriber('hdb++es-srv/%s-null'%api.db_name,
                                 'archiving/%s/null'%api.db_name)
    r = get_archiving_loads(schema)
    
    #subsloads = dict((d,api.get_archiver_attributes(d)) for d in subs)
    #pers = api.get_periodic_archivers()
    #persloads = dict((d,api.get_periodic_archiver_attributes(d)) for d in pers)
    #perattrs = api.get_periodic_attributes()
    #pernoevs = [a for a in perattrs if not fn.tango.check_attribute_events(a)]
    #subattrs = [a for a in api.get_attributes() if a not in perattrs]
    #attrlists = sorted(set(fn.join(fn.get_device_property(d,'AttributeList') 
                                   #for d in subs)))
    #evsubs = [d for d in api.get_subscribers() if 'null' not in d]
    
    #print('%d attributes, %d subscribed, %d periodic, %d subscribers, %d pollers' % 
          #(len(attrlists),len(subattrs),len(perattrs),len(evsubs),len(pers)))
    #print('Current loads')
    #print([(k,len(v)) for k,v in subsloads.items()])


    sublist = []
    # get generic archivers only
    for d in r.subs:
        if fn.clmatch('*([0-9]|null)$',d):
            sublist.extend(fn.get_device_property(d,'AttributeList'))
        
    nulllist = [a for a in sublist if a.split(';')[0] in r.pernoevs]
    sublist = [a for a in sublist if a.split(';')[0] not in r.pernoevs]
    
    if subscribers:
        print('Moving %d periodics to /null' % len(nulllist))
        if do_it:
            fn.tango.put_device_property('archiving/%s/null'%api.db_name,
                'AttributeList',nulllist)
        
        evsubs = [d for d in r.subs if fn.clmatch('*[0-9]$',d)]
        avgload = 1+len(sublist)/(len(evsubs))
        print('Subscriber load = %d' % avgload)
        if avgload>maxload:
            raise Exception('Load too high!, create archivers!')
        
        for i,d in enumerate(evsubs):
            attrs = sublist[i*avgload:(i+1)*avgload]
            print(d,len(attrs))
            if do_it:
                fn.tango.put_device_property(d,'AttributeList',attrs)
    
    r.nulllist = nulllist
    r.sublist = sublist
    
    if periodics:
        sublist = []
        for d in r.pers:
            sublist.extend(fn.get_device_property(d,'AttributeList'))        
        avgload = 1+len(sublist)/(len(r.pers))
        print('Periodic archiver load = %d' % avgload)
        if avgload>maxload:
            raise Exception('Load too high!, create archivers!')
        
        for i,d in enumerate(r.pers):
            attrs = sublist[i*avgload:(i+1)*avgload]
            print(d,len(attrs))
            if do_it:
                fn.tango.put_device_property(d,'AttributeList',attrs)        
    
    if not do_it:
        print('It was just a dry run, nothing done')
    return r

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
    descriptions = dict((t,api.getTableCreator(t)) for t in tables)
    partitioned = [t for t,v in descriptions.items() if 'partition' in str(v).lower()]
    print('%s: partitioned tables: %d/%d' % (schema,len(partitioned),len(tables)))
    
if __name__ == '__main__' :
    if sys.argv[1:] and (sys.argv[1]=='help' or sys.argv[1] in locals()):
        r = fn.call(locals_=locals())
        print(r)
    else:
        args,opts = fn.linos.sysargs_to_dict(split=True)
        main(*args,**opts)
    
