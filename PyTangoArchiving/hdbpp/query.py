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

class HDBppReader(HDBppDB):
    """
    Python API for accessing HDB++ archived values
    See HDBpp for configuration-related methods
    This api uses methods from devices or database
    """
    
    def get_mysqlsecsdiff(self,date):
        """
        Returns the value to be added to dates when querying int_time tables
        """
        return self.Query(
            "select (TO_SECONDS('%s')-62167222800) - UNIX_TIMESTAMP('%s')" 
            % (date,date))[0][0]
      
    def get_last_attribute_values(self,table,n=1,
                                  check_table=False,epoch=None):
        if epoch is None:
            start,epoch = None,fn.now()+600
        elif epoch < 0:
            start,epoch = fn.now()+epoch,fn.now()+600
        if start is None:
            #Rounding to the last month partition
            start = fn.str2time(
                fn.time2str().split()[0].rsplit('-',1)[0]+'-01')
        vals = self.get_attribute_values(table, N=n, human=True, desc=True,
                        start_date=start, stop_date=epoch)
        if len(vals):
            return vals[0] if abs(n)==1 else vals
        else: 
            return vals
    
    def load_last_values(self,attributes=None,n=1,epoch=None):
        if attributes is None:
            attributes = self.get_archived_attributes()
        vals = dict((a,self.get_last_attribute_values(a,n=n,epoch=epoch)) 
                    for a in fn.toList(attributes))
        for a,v in vals.items():
            if n!=1:
                v = v and v[0]
            self.attributes[a].last_date = v and v[0]
            self.attributes[a].last_value = v and v[1]
            
        return vals

    __test__['get_last_attribute_values'] = \
        [(['bl01/vc/spbx-01/p1'],None,lambda r:len(r)>0)] #should return array
    
    @CatchedAndLogged(throw=True)
    def get_attribute_values(self,table,start_date=None,stop_date=None,
                             desc=False,N=0,unixtime=True,
                             extra_columns='quality',decimate=0,human=False,
                             as_double=True,aggregate='MAX',int_time=True,
                             **kwargs):
        """
        This method returns values between dates from a given table.
        If stop_date is not given, then anything above start_date is returned.
        desc controls the sorting of values
        
        unixtime = True enhances the speed of querying by a 60%!!!! 
            #(due to MySQLdb implementation of datetime)
        
        If N is specified:
        
            * Query will return last N values if there's no stop_date
            * If there is, then it will return the first N values (windowing?)
            * IF N is negative, it will return the last N values instead
            
        start_date and stop_date must be in a format valid for SQL
        """
        t0 = time.time()
        self.debug('HDBpp.get_attribute_values(%s,%s,%s,%s,decimate=%s,'
                   'int_time=%s,%s)'
              %(table,start_date,stop_date,N,decimate,int_time,kwargs))
        if fn.isSequence(table):
            aid,tid,table = table
        else:
            index = None
            if '[' in table:
                try:
                    table = table.split('[')[0]
                    index = int(fn.clsearch('\[([0-9]+)\]',table).groups()[0])
                except:
                    pass
            aid,tid,table = self.get_attr_id_type_table(table)
            
        if not all((aid,tid,table)):
            self.warning('%s is not archived' % table)
            return []
            
        human = kwargs.get('asHistoryBuffer',human)
            
        what = 'UNIX_TIMESTAMP(data_time)' if unixtime else 'data_time'
        if as_double:
            what = 'CAST(%s as DOUBLE)' % what
            
        value = 'value_r' if 'value_r' in self.getTableCols(table) \
                                else 'value'
            
        if 'array' in table: 
            what+=",idx"
            # arrays cannot be aggregated !
            decimate = False
            
        elif decimate and aggregate in ('AVG','MAX','MIN'):
            value = '%s(%s)' % (aggregate,value)
            
        what += ', ' + value
        if extra_columns: 
            what+=','+extra_columns

        interval = 'where att_conf_id = %s'%aid if aid is not None \
                                                else 'where att_conf_id >= 0 '
                                            
        #self.info('%s : %s' % (table, self.getTableCols(table)))
        int_time = int_time and 'int_time' in self.getTableCols(table)
        if self.db_name == 'hdbrf': int_time = False #@TODO HACK
        if int_time:
            self.info('Using int_time indexing for %s' % table)
        if start_date or stop_date:
            start_date,start_time,stop_date,stop_time = \
                Reader.get_time_interval(start_date,stop_date)
            
            if int_time:
                
                def str2mysqlsecs(date):
                    rt = fn.str2time(date)
                    return int(rt+self.get_mysqlsecsdiff(date))
                
                if start_date and stop_date:
                    interval += (" and int_time between %d and %d"
                            %(str2mysqlsecs(start_date),
                              str2mysqlsecs(stop_date)))
                
                elif start_date and fandango.str2epoch(start_date):
                    interval += (" and int_time > %d" 
                                 % str2mysqlsecs)
                
            else:
                if start_date and stop_date:
                    interval += (" and data_time between '%s' and '%s'"
                            %(start_date,stop_date))
                
                elif start_date and fandango.str2epoch(start_date):
                    interval += " and data_time > '%s'"%start_date
            
        query = 'select %s from %s %s' % (what,table,interval)
        if decimate:
            if isinstance(decimate,(int,float)):
                d = int(decimate) or 1
            else:
                d = int((stop_time-start_time)/10800) or 1
            def next_power_of_2(x):  
                return 1 if x == 0 else 2**int(x - 1).bit_length()
            d = next_power_of_2(d/2)
            # decimation on server side
            query += ' group by FLOOR(%s/%d)' % (
                'int_time' if int_time else 'UNIX_TIMESTAMP(data_time)',d)
            
        query += ' order by %s' % ('int_time' if int_time else 'data_time')
                    
        if N == 1:
            human = 1
        if N < 0 or desc: 
            query+=" desc" # or (not stop_date and N>0):
        if N: 
            query+=' limit %s'%abs(N if 'array' not in table else N*1024)
        
        ######################################################################
        # QUERY
        t0 = time.time()
        self.debug(query.replace('where','\nwhere').replace(
            'group,','\ngroup'))
        try:
            result = self.Query(query)
            self.info('read [%d] in %f s'%(len(result),time.time()-t0))
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
        if 'array' in table:
            data = fandango.dicts.defaultdict(list)
            for t in result:
                data[float(t[0])].append(t[1:])
            result = []
            for k,v in sorted(data.items()):
                l = [0]*(1+max(t[0] for t in v))
                for i,t in enumerate(v):
                    if None in t: 
                        l = None
                        break
                    l[t[0]] = t[1] #Ignoring extra columns (e.g. quality)
                result.append((k,l))
            if N > 0: 
                #for k,l in result:
                    #print((k,l and len(l)))
                result = result[-N:]
            if N < 0 or desc:
                result = list(reversed(result))

            if index is not None:
                result = [r[index] for r in result]
            self.debug('array arranged [%d] in %f s'
                         % (len(result),time.time()-t0))
            t0 = time.time()
          
        # Converting the timestamp from Decimal to float
        # Weird results may appear in filter_array comparison if not done
        # Although it is INCREDIBLY SLOW!!!
        #result = []
        #nr = []
        #if len(result[0]) == 2: 
            #for i,t in enumerate(result):
                #result[i] = (float(t[0]),t[1])
        #elif len(result[0]) == 3: 
            #for i,t in enumerate(result):
                #result[i] = (float(t[0]),t[1],t[2])
        #elif len(result[0]) == 4: 
           #for i,t in enumerate(result):
                #result[i] = ((float(t[0]),t[1],t[2],t[3]))
        #else:
            #for i,t in enumerate(result):
                #result[i] = ([float(t[0])]+t[1:])
        
        self.debug('timestamp arranged [%d] in %f s'
                     % (len(result),time.time()-t0))
        t0 = time.time()
            
        # Decimation to be done in Reader object
        #if decimate:
            ## When called from trends, decimate may be the decimation method
            ## or the maximum sample number
            #try:
                #N = int(decimate)
                ##decimate = data_has_changed
                #decimate = 
                #result = PyTangoArchiving.reader.decimation(
                                        #result,decimate,window=0,N=N)                
            #except:
                ##N = 1080
                #result = PyTangoArchiving.reader.decimation(result,decimate) 
        
        if human: 
            result = [list(t)+[fn.time2str(t[0])] for t in result]

        if not desc and ((not stop_date and N>0) or (N<0)):
            #THIS WILL BE APPLIED ONLY WHEN LAST N VALUES ARE ASKED
            self.warning('reversing ...' )
            result = list(reversed(result))
        #else:
            ## why
            #self.getCursor(klass=MySQLdb.cursors.SSCursor)

        self.debug('result arranged [%d]'%len(result))            
        return result
        
    def get_attributes_values(self,tables='',start_date=None,stop_date=None,
                desc=False,N=0,unixtime=True,extra_columns='quality',
                decimate=0,human=False):
        
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
            where = " and %s between '%s' and '%s'" % (
                'int_time' if int_time else 'data_time',
                int(str2time(dates[0])) if int_time else dates[0],
                int(str2time(dates[1])) if int_time else dates[1])
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
