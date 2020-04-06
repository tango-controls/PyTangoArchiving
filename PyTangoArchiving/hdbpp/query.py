import fandango as fn
from fandango.objects import SingletonMap, Cached
from fandango.tango import *
import MySQLdb,traceback,re
from PyTango import AttrQuality

import PyTangoArchiving
from PyTangoArchiving.dbs import ArchivingDB
from PyTangoArchiving.common import CommonAPI
from PyTangoArchiving.reader import Reader
from PyTangoArchiving.utils import CatchedAndLogged
from PyTangoArchiving.schemas import Schemas

from .config import HDBppDB

__test__ = {}

def get_search_model(model):
    if model.count(':')<2:
        model = '%/'+model
    model = clsub('[:][0-9]+','%:%',model)
    return model

RAW = None

partition_prefixes = {
### BUT, NOT ALL TABLES ARE IN THIS LIST!
# I'm partitioning only the big ones, and ignoring the others
# boolean, encoded, enum, long64 uchar ulong64, ulong, ushort
# b, e, n, l64, ul6, ul, us, uc

    'att_array_devdouble_ro':'adr',
    'att_array_devdouble_rw':'adw',
    'att_array_devfloat_ro':'afr',
    'att_array_devfloat_rw':'afw',
    'att_array_devlong_ro':'alr',
    'att_array_devlong_rw':'alw',    
    'att_array_devshort_ro':'ahr',
    'att_array_devboolean_ro':'abr',    
    'att_array_devboolean_rw':'abw',        
    'att_array_devstring_ro':'asr',
    'att_array_devstate_ro':'atr',

    'att_scalar_devdouble_ro':'sdr',
    'att_scalar_devdouble_rw':'sdw',
    
    'att_scalar_devfloat_ro':'sfr',
    'att_scalar_devlong_ro':'slr',
    'att_scalar_devlong_rw':'slw',
    'att_scalar_devshort_ro':'shr',
    'att_scalar_devshort_rw':'shw',    
    'att_scalar_devboolean_ro':'sbr',
    'att_scalar_devboolean_rw':'sbw',

    'att_scalar_devstate_ro':'str',
    'att_scalar_devstring_ro':'ssr',
    'att_scalar_devstring_rw':'ssw',
    'att_scalar_devushort_ro':'sur',
    'att_scalar_devuchar_ro':'scr',
    }

MIN_FILE_SIZE = 16*1024 #hdbrf size in arch04
MAX_QUERY_SIZE = 256*1024 

class HDBppReader(HDBppDB):
    """
    Python API for accessing HDB++ archived values
    See HDBpp for configuration-related methods
    This api uses methods from devices or database
    
    THE METHODS IN THIS API SHOULD MATCH WITH PyTangoArchiving.Reader API
    """
    MIN_FILE_SIZE = MIN_FILE_SIZE
    
    @Cached(depth=1000,expire=600)
    def get_mysqlsecsdiff(self,date=None):
        """
        Returns the value to be added to dates when querying int_time tables
        """
        if date is None: 
            date = fn.time2str()
        if isinstance(date,(int,float)): 
            date = fn.time2str(date)
        return self.Query(
            "select (TO_SECONDS('%s')-62167222800) - UNIX_TIMESTAMP('%s')" 
            % (date,date))[0][0]
    
    def get_partition_time_by_name(self,partition):
        m = fn.clsearch('[0-9].*',partition)
        if m:
            d = fn.str2time(m.group(),cad='%Y%m%d')
            return d
        else:
            return fn.END_OF_TIME
    
    def get_last_partition(self, table, min_size = MIN_FILE_SIZE, 
                           add_last = True, tref = None):
        """
        Return the last partition updated (size > min_size)
        Returns None if the table is not partitioned
        """    
        parts = self.getTablePartitions(table)
        tref = fn.END_OF_TIME if tref is None else tref
        # Gets last partition used
        if not min_size:
            last_part = None
        else:
            last_part = fn.last(sorted(p for p in parts 
                if p and self.getPartitionSize(table,p) > min_size
                    and (add_last or '_last' not in p)), 
                    default=None)
        # Gets last partition declared
        if parts and not last_part:
            last_part = fn.last(sorted(p for p in parts 
                if (add_last and '_last' in p) 
                    or self.get_partition_time_by_name(p) < tref),
                        default=parts[0])
        return last_part
    
    def generate_partition_name_for_date(self, table, date):
        """
        generates the matching partition name for the date given
        """
        if not fn.isString(date):
            date = fn.time2str(date)
        p = partition_prefixes.get(table,None)
        if p:
            p += ''.join(date.split('-')[0:2]) + '01'
        return p
    
    def get_partitions_at_dates(self, table, date1, date2=None):
        """
        returns partitions containing data for date1-date2 interval
        if date2 is None, only partition at date1 is returned
        """
        p1 = self.generate_partition_name_for_date(table,date1)
        p2 = self.generate_partition_name_for_date(table,date2 or date1)
        parts = self.getTablePartitions(table)  
        if parts is None or not len(parts):
            return None
        
        p1 = parts[0] if p1 not in parts else p1
        p2 = parts[-1] if p2 not in parts else p2
        if None in (p1,p2): 
            return None
        else:
            parts = parts[parts.index(p1):parts.index(p2)+1]
            return parts if date2 or not len(parts) else parts[0]
        
    get_table_partitions_for_dates = get_partitions_at_dates   
    
    def get_table_timestamp(self, table, method='max', 
            epoch = None, ignore_errors = False): #, tref = -180*86400):
        """
        method should be min() for first value and max() for last
        this query goes directly to table indexes
        this doesn't access values (but it is much faster)
        
        if table is an attribute name, only these attribute is checked
        
        ignore_errors=True, it will ignore dates out of 1970-NOW interval
        
        epoch=timestamp, gets last timestamp before epoch
        
        Returns a tuple containing:
            (the first/last value stored, in epoch and date format, 
                size of table, time needed)
        """
        t0,last,size = fn.now(),0,0
        #print('get_last_value_in_table(%s, %s)' % (self.self_name, table))
        
        if table in self.get_data_tables():
            ids = self.get_attributes_by_table(table,as_id=True)
        else:
            aid,atype,table = self.get_attr_id_type_table(table)
            ids = [aid]

        int_time = any('int_time' in v for v in self.getTableIndex(table).values())
        # If using UNIX_TIMESTAMP THE INDEXING FAILS!!
        field = 'int_time' if int_time else 'data_time'
        q = 'select %s(%s) from %s ' % (method,field,table)      
        size = self.getTableSize(table)
        r = []

        for i in ids:
            qi = q+' where att_conf_id=%d' % i
            #if tref and int_time: where += ('int_time <= %d'% (tref))
            r.extend(self.Query(qi))
            
        method = {'max':max,'min':min}[method]
        r = [self.mysqlsecs2time(l[0]) if int_time else fn.date2time(l[0]) 
            for l in r if l[0] not in (0,None)]
        r = [l for l in r if l if (ignore_errors or 1e9<l<fn.now())]

        last = method(r) if len(r) else 0
        date = fn.time2str(last)

        return (last, date, size, fn.now()-t0) 
      
    def get_last_attribute_values(self,attribute,n=1,
            check_attribute=False,epoch=None,period=86400):
        """
        load_last_values provided to comply with Reader API
        get_last_attribute_values provided to comply with CommonAPI
        
        returns last n values (or just one if n=1)
        """
        if epoch is None:
            epoch = self.get_table_timestamp(attribute,method='max')[0]
        elif epoch < 0:
            epoch = fn.now()+epoch
        start = epoch - abs(period)

        vals = self.get_attribute_values(attribute, N=n, human=True, desc=True,
                        start_date=start, stop_date=epoch)
        if len(vals):
            return vals[0] if abs(n)==1 else vals
        else: 
            return vals
    
    def load_last_values(self,attributes=None,n=1,epoch=None,tref=86400):
        """
        load_last_values provided to comply with Reader API
        get_last_attribute_values provided to comply with CommonAPI 
        
        load_last_values returns a dictionary {attr:(last_time,last_value)}
        
        attributes: attribute name or list
        n: the number of last values to be retorned
        tref: time from which start searching values (-1d by default)
        epoch: end of window to search values (now by default)
        """       
        if attributes is None:
           attributes = self.get_archived_attributes()

        if epoch is not None:
            epoch = fn.str2time(epoch) if fn.isString(epoch) else epoch
            kwargs = {'epoch':epoch,'period':(epoch-tref) if tref>1e9 else abs(tref)}
        else:
            kwargs = {}

        vals = dict((a,self.get_last_attribute_values(a,n=n,**kwargs)) 
                    for a in fn.toList(attributes))

        for a,v in vals.items():
            if n!=1:
                v = v and v[0]
            self.attributes[a].last_date = v and v[0]
            self.attributes[a].last_value = v and v[1]
            
        return vals

    __test__['get_last_attribute_values'] = \
        [(['bl01/vc/spbx-01/p1'],None,lambda r:len(r)>0)] #should return array
    
    #@Cached(depth=10,expire=300.)
    #def Query(self,*args,**kwargs):
        #return HDBppDB.Query(self,*args,**kwargs)
        
    @Cached(depth=10000,expire=60.)
    def get_attribute_indexes(self,table):
        index = None
        if fn.isSequence(table):
            aid,tid,table = table
        else:
            if '[' in table:
                try:
                    index = int(fn.clsearch('\[([0-9]+)\]',table).groups()[0])
                    table = table.split('[')[0]
                except:
                    pass               
            aid,tid,table = self.get_attr_id_type_table(table)
        return aid,tid,table,index
    
    def str2mysqlsecs(self,date):
        """ converts given date to int mysql seconds() value """
        rt = fn.str2time(date)
        return int(rt+self.get_mysqlsecsdiff(date))
    
    def mysqlsecs2time(self,int_time,tref=0):
        """ converts a mysql secons() value to epoch """
        tref = tref or int_time
        return int_time - self.get_mysqlsecsdiff(fn.time2str(tref))
    
    INDEX_IN_QUERY = True
        
    def get_attribute_values_query(self,attribute,
            start_date=None,stop_date=None,
            desc=False,N=0,unixtime=True,
            extra_columns='quality',
            decimate=0,human=False,
            as_double=True,
            aggregate='', #'MAX',
            int_time=True,
            what='',
            where='',
            group_by='',
            **kwargs):

        if attribute in self.getTables():
            aid,tid,table,index = None,None,attribute,None
        else:
            aid,tid,table,index = self.get_attribute_indexes(attribute)
                                   
        if not what:
            what = 'UNIX_TIMESTAMP(data_time)' if unixtime else 'data_time'
            if as_double:
                what = 'CAST(%s as DOUBLE)' % what
            #what += ' AS DTS'
                
            value = 'value_r' if 'value_r' in self.getTableCols(table) \
                                    else 'value'
                
            if decimate and aggregate in ('AVG','MAX','MIN'):
                value = '%s(%s)' % (aggregate,value)
                
            what += ', ' + value
            if 'array' in table: 
                what += ", idx"
                
            if extra_columns: 
                what+=','+extra_columns #quality!

        if where:
            where = where+' and '
        if 'where' not in where:
            where = 'where '+where

        interval = 'att_conf_id = %s'%aid if aid is not None \
                                                else 'att_conf_id >= 0 '
                                            
        if index and self.INDEX_IN_QUERY:
            interval += ' and idx = %s ' % index
                                            
        int_time = int_time and 'int_time' in self.getTableCols(table)
        #if int_time: self.debug('Using int_time indexing for %s' % table)
            
        if start_date or stop_date:
            start_date,start_time,stop_date,stop_time = \
                Reader.get_time_interval(start_date,stop_date)
            
            if int_time:
                
                if start_date and stop_date:
                    interval += (" and int_time between %d and %d"
                            %(self.str2mysqlsecs(start_date),
                              self.str2mysqlsecs(stop_date)))
                
                elif start_date and fandango.str2epoch(start_date):
                    interval += (" and int_time > %d" 
                                 % self.str2mysqlsecs)
                
            else:
                if start_date and stop_date:
                    interval += (" and data_time between '%s' and '%s'"
                            %(start_date,stop_date))
                
                elif start_date and fandango.str2epoch(start_date):
                    interval += " and data_time > '%s'"%start_date
            
        where = where + interval
        query = 'select %s from %s %s' % (what,table,where)

        #self.warning('decimate = %s = %s' % (str(decimate),bool(decimate)))           
        if decimate:
            
            if isinstance(decimate,(int,float)):
                d = int(decimate) or 1
            else:
                d = int((stop_time-start_time)/MAX_QUERY_SIZE) or 1

            def next_power_of_2(x):  
                return 1 if x == 0 else 2**int(x - 1).bit_length()
            #d = next_power_of_2(d) #(d/2 or 1)
            
            # decimation on server side
            if not group_by:
                group_by = 'att_conf_id,' if 'att_conf_id' in str(what) else ''
                if int_time:
                    group_by += '(%s DIV %d)' % ('int_time', d)
                else:
                    group_by += '(FLOOR(%s/%d))' % ('UNIX_TIMESTAMP(data_time)', d)

                if 'array' in table:
                    group_by += ',idx'

            query += " group by %s" % group_by
            
        query += ' order by %s' % ('int_time' #, DTS' # much slower!
                            if int_time else 'data_time')
                    
        if N == 1:
            human = 1
        if N < 0 or desc: 
            query+=" desc" # or (not stop_date and N>0):
        if N: 
            query+=' limit %s' % (abs(N)) # if 'array' not in table else N*128)

        # too dangerous to remove always data by default, and bunching does not work
        #else: 
            #query+=' limit %s' % (MAX_QUERY_SIZE)

        return query
        
    
    @CatchedAndLogged(throw=True)
    def get_attribute_values(self,attribute,
                             start_date=None,stop_date=None,
                             desc=False,N=0,unixtime=True,
                             extra_columns='quality',decimate=0,human=False,
                             as_double=True,
                             aggregate='', #'MAX',
                             int_time=True,
                             what='',
                             where='',
                             **kwargs):
        """ 
        Returns archived values between dates for a given table/attribute.
        
        Parameters
        ----------
        attribute
            attribute or table
        start_date/stop_date
            if not stop_date, anything between start_date and now()
            start_date and stop_date float or str in a format valid for SQL
        desc
            controls the sorting of values
        N
            If 0, None or False, has no effect
            Query will return last N values if there's no stop_date
            If there is, then it will return the first N values (windowing?)
            If N is negative, it will return the last N values instead
        unixtime
            if True forces conversion of datetime to unix timestamp
            at query time. It speeds querying by a 60%!!!! 
        extra_columns
            adds columns to result ('quality' by default)
        decimate
            period or aggregation methods
            0 by default (the method will choose)
            if None (RAW), no decimation is done at all
            
        """
        t0 = time.time()
        N = N or kwargs.get('n',0)
        self.info('HDBpp.get_attribute_values(%s,%s,%s,N=%s,decimate=%s,'
                   'int_time=%s,%s)'
              %(attribute,start_date,stop_date,N,decimate,int_time,kwargs))

        aid,tid,table,index = self.get_attribute_indexes(attribute)
            
        if not all((aid,tid,table)):
            self.warning('%s is not archived' % table)
            return []
            
        human = kwargs.get('asHistoryBuffer',human)
        
        if start_date or stop_date:
            start_date,start_time,stop_date,stop_time = \
                Reader.get_time_interval(start_date,stop_date)
            
        query = self.get_attribute_values_query(
            attribute, start_date, stop_date, desc, N,  unixtime,
            extra_columns, decimate, human, as_double,
            aggregate, int_time, what, where, **kwargs
            )
        
        ######################################################################
        # QUERY
        
        t0 = time.time()
        is_array = 'array' in table
        self.debug(query.replace('where','\nwhere').replace(
            'group,','\ngroup'))
        try:
            result = []
            lasts = {}
            cursor = self.Query(query, export = False)
            while True:
                # Fetching/decimating data in blocks of 1024 rows
                v = cursor.fetchmany(1024)
                if v is None: 
                    break
                span = ((v[-1][0]-v[0][0]) if len(v)>1 else 0)
                density = len(v)/(span or 1)
                if decimate!=RAW and (density*(stop_time-start_time))>MAX_QUERY_SIZE:
                    if not decimate or type(decimate) not in (int,float):
                        decimate = float(stop_time-start_time)/MAX_QUERY_SIZE
                        self.warning('density=%s values/s!: enforce decimate every %s seconds'
                                 % (density,decimate))
                    for l in v:
                        ix = l[2] if is_array else None
                        if ((ix not in lasts)
                                or (None in (l[1],lasts[ix][1]))
                                or (l[0] >= (lasts[ix][0]+decimate))):
                            result.append(l)
                            lasts[ix] = l
                else:
                    result.extend(v)

                if len(v) < 1024:
                    break
            
            self.debug('read [%d] in %f s: %s' % 
                         (len(result),time.time()-t0,
                          len(result)>1 and (result[0],result[1],result[-1])))
                         
        except MySQLdb.ProgrammingError as e:
            result = []
            if 'DOUBLE' in str(e) and "as DOUBLE" in query:
                return self.get_attribute_values((aid,tid,table),start_date,
                    stop_date,desc,N,unixtime,extra_columns,decimate,human,
                    as_double=False,**kwargs)
            else:
                traceback.print_exc()
            
        if not result or not result[0]: 
            return []
        ######################################################################
        
        t0 = time.time()
        
        if is_array and (not index or not self.INDEX_IN_QUERY):
            max_ix = 0
            data = fandango.dicts.defaultdict(list)
            for t in result:
                data[float(t[0])].append(t[1:])
                if t[2] is not None and max_ix < t[2]:
                    max_ix = t[2]
                
            result = []
            last_arrs = [None]*(1+max_ix)
            for k,v in sorted(data.items()):
                # it forces all lines to be equal in length
                l = last_arrs[:]
                for i,t in enumerate(v):
                    if None in t: 
                        l = None
                        break
                    # t[1] is index, t[0] is value
                    l[t[1]] = t[0] #Ignoring extra columns (e.g. quality)
                    last_arrs[t[1]] = t[0]
                result.append((k,l))
                
            if N > 0: 
                #for k,l in result:
                    #print((k,l and len(l)))
                result = result[-N:]
            if N < 0 or desc:
                result = list(reversed(result))

            if index is not None:
                nr = []
                for i,r in enumerate(result):
                    try:
                        nr.append((r[0],r[1][index] 
                                   if r[1] is not None else r[1]))
                    except:
                        print(index,r)
                        traceback.print_exc()
                        break
                result = nr
                
            self.debug('array arranged [%d][%s] in %f s'
                         % (len(result),index,time.time()-t0))
        
        # Decimation to be done in Reader object, after caching
        if human: 
            result = [list(t)+[fn.time2str(t[0])] for t in result]

        if not desc and ((not stop_date and N>0) or (N<0)):
            #THIS WILL BE APPLIED ONLY WHEN LAST N VALUES ARE ASKED
            self.debug('reversing ...' )
            result = list(reversed(result))

        self.debug('result arranged [%d]: %s, %s' % 
            (len(result), result[0], result[-1]))
        return result
        
    def get_attributes_values(self,tables='',start_date=None,stop_date=None,
                desc=False,N=0,unixtime=True,extra_columns='quality',
                decimate=0,human=False):
        
        if start_date or stop_date:
            start_date,start_time,stop_date,stop_time = \
                Reader.get_time_interval(start_date,stop_date)        
        
        if not fn.isSequence(tables):
            tables = self.get_archived_attributes(tables)
            
        return dict((t,self.get_attribute_values(t,start_date,stop_date,desc,
                N,unixtime,extra_columns,decimate,human))
                for t in tables)

    def get_attribute_rows(self,attribute,start_date=0,stop_date=0):
        aid,tid,table = self.get_attr_id_type_table(attribute)
        int_time = 'int_time' in self.getTableCols(table)
        if start_date and stop_date:
            dates = map(time2str,(start_date,stop_date))
            if int_time:
                where = " and int_time between %d and %d" % (
                    self.str2mysqlsecs(dates[0]),self.str2mysqlsecs(dates[1]))
            else:
                where = " and data_time between '%s' and '%s'" % (
                    dates[0],dates[1])            
        else:
            where = ''
        r = self.Query('select count(*) from %s where att_conf_id = %s'
                          % ( table, aid) + where)
        return r[0][0] if r else 0
    
    @Cached(depth=10000,expire=60.)
    def get_attribute_modes(self,attr,force=None):
        """ force argument provided just for compatibility, replaced by cache
        """
        aid,tid,table = self.get_attr_id_type_table(attr)
        r = {'ID':aid, 'MODE_E':fn.tango.get_attribute_events(attr)}
        r['archiver'] = self.get_attribute_archiver(attr)
        return r        
    
    def get_attribute_errors_ids(self, attribute, start, end):
        what = 'data_time, att_error_desc_id'
        return self.get_attribute_values(attribute, start, end,
            what = what, where = 'att_error_desc_id is not NULL')
        
    
    def get_error_description(self, error_id):
        return str(hdbct.Query('select error_desc from att_error_desc '
            'where att_error_desc_id = %s' % error_id))
    
    def get_attributes_errors(self, regexp='*', timeout=3*3600, 
                              from_db=False, extend = False):
        """
        Returns a dictionary {attribute, error/last value}
        
        If from_db=True and extend=True, it performs a full attribute check
        """
        if regexp == '*':
            self.status = fn.defaultdict(list)
        if from_db or extend:
            timeout = fn.now()-timeout
            attrs = self.get_attributes(True)
            attrs = fn.filtersmart(attrs,regexp)
            print('get_attributes_errors([%d/%d])' 
                  % (len(attrs),len(self.attributes)))
            vals = self.load_last_values(attrs)
            for a,v in vals.items():
                if v and v[0] > timeout:
                    self.status['Updated'].append(a)
                    if v[1] is not None:
                        self.status['Readable'].append(a)
                    else:
                        rv = fn.read_attribute(a)
                        if rv is not None:
                            self.status['WrongNone'].append(a)
                        else:
                            self.status['None'].append(a)
                    vals.pop(a)

            if not extend:
                self.status['NotUpdated'] = vals.keys()
            else:
                for a,v in vals.items():
                    c = fn.check_attribute(a)
                    if c is None:
                        vals[a] = 'Unreadable'
                        self.status['Unreadable'].append(a)
                    elif isinstance(c,Exception):
                        vals[a] = str(c)
                        self.status['Exception'].append(a)
                    else:
                        ev = fn.tango.check_attribute_events(a)
                        if not ev:
                            vals[a] = 'NoEvents'
                            self.status['NoEvents'].append(a)
                        else:
                            d = self.get_attribute_archiver(a)
                            e = self.get_archiver_errors(d)
                            if a in e:
                                vals[a] = e[a]
                                self.status['ArchiverError'].append(a)
                            else:
                                rv = fn.read_attribute(a)
                                if v and str(rv) == str(v[1]):
                                    vals[a] = 'NotChanged'
                                    self.status['NotChanged'].append(a)
                                else:
                                    self.status['NotUpdated'].append(a)
                                
            if regexp == '*':
                for k,v in self.status.items():
                    print('%s: %s' % (k,len(v)))
            
            return vals
        else:
            # Should inspect the Subscribers Error Lists
            vals = dict()
            for d in self.get_archivers():
                err = self.get_archiver_errors(d)
                for a,e in err.items():
                    if fn.clmatch(regexp,a):
                        vals[a] = e
            return vals    
    
    def check_attributes(self,attrs = '', load = False, t0 = 0):
        
        db,t0,result,vals = self,t0 or fn.now(),{},{}
        print('Checking %s' % str(db))

        if fn.isDictionary(attrs):
            attrs,vals = attrs.keys(),attrs
            if isinstance(vals.values()[0],dict):
                vals = dict((k,v.values()[0]) for k,v in vals.items())
        else:
            if fn.isString(attrs):
                attrs = fn.filtersmart(db.get_attributes(),attrs)
                load = True

        if load:
            [vals.update(db.load_last_values(a)) for a in attrs]

        print('\t%d attributes'%len(attrs))
        result['attrs'] = attrs
        result['vals'] = vals
        result['novals'] = [a for a,v in vals.items() if not v]
        result['nones'],result['down'],result['lost'] = [],[],[]
        for a,v in vals.items():
            if not v or [1] is None:
                if not fn.read_attribute(a): #USE read not check!!
                    result['down'].append(a)
                else:
                    result['novals' if not v else 'nones'].append(a)
            elif v[0] < (t0 - 7200):
                result['lost'].append(a)
        
        print('\t%d attributes have no values'%len(result['novals']))
        print('\t%d attributes are not readable'%len(result['down']))
        print('\t%d attributes are not updated'%len(result['lost']))
        print('\t%d attributes have None values'%len(result['nones']))
        
        return result    
    
    def get_attributes_not_updated(self,t=7200):
        vals = self.load_last_values(self.get_attributes())
        nones = [k for k,v in vals.items() 
                    if (not v or v[1] is None)]
        nones = [k for k in nones if fn.read_attribute(k) is not None]
        lost = [k for k,v in vals.items() 
                if k not in nones and v[0] < fn.now()-t]
        lost = [k for k in lost if fn.read_attribute(k) is not None]
        failed = nones+lost
        return sorted(failed)    
