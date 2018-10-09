#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################################
## This file is part of Tango Control System:  http://www.tango-controls.org/
##
## $Author: Sergi Rubio Manrique, srubio@cells.es
## copyleft :    ALBA Synchrotron Controls Section, www.cells.es
##
## Tango Control System is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as published
## by the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## Tango Control System is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""
PyTangoArchiving.dbs: Module for managing raw access to archiving database
"""

import traceback,time
import MySQLdb,sys

import fandango
import fandango as fn
from fandango.objects import Object
from fandango.log import Logger
import PyTangoArchiving.utils as utils

try:
    from fandango.db import FriendlyDB
except:
    raise Exception,'import FriendlyDB failed, is MySQLdb module installed?'



class ArchivingDB(FriendlyDB):
    """ 
    Class for managing the direct access to the database 
    """
    
    #def __init__(self,api,db_name,user='',passwd='', host=''):
        #if not api or not database:
            #self.log.error('ArchivingAPI and database are required arguments '
            # 'for ArchivingDB initialization!')
            #return
        #self.api=api
        #self.db_name=db_name
        #self.host=api.host
        #self.log=Logger('ArchivingDB(%s)'%db_name)
        #self.setUser(user,passwd)
        #self.__initMySQLconection()
    #def setUser(self,user,passwd):
        #self.user=user
        #self.passwd=passwd
    #def cursor(self):
        #return self.db.cursor()
    #def __initMySQLconnection(self):
        #try:
            #self.db=MySQLdb.connect(db=self.db_name,host=self.host,user=self.user,passwd=self.passwd)
        #except Exception,e:
            #self.log.error( 'Unable to create a MySQLdb connection to "%s"@%s.%s: %s'%(self.user,self.host,self.db_name,str(e)))
            
    def check(self):
        return bool(self.Query('describe adt'))
    
    @staticmethod
    def get_table_name(ID):
        ID = int(ID)
        return 'att_%05d'%ID if ID<10000 else 'att_%06d'%ID
        
    def get_attribute_ID(self,name):
        #return fandango.first(self.get_attributes_IDs(name).values())
        try:
            return self.get_attributes_IDs(name).values()[0]
        except Exception as e:
            print(name,e)
            raise e
    
    def get_attributes_IDs(self,name=''):
        q =  "select full_name,ID from adt"+(" where full_name like '%s'"%name if name else '')
        return dict((a.lower(),v) for a,v in self.Query(q))
    
    def get_attribute_names(self,active=False):
        """
        active=True will return only attributes with an archiver currently assigned
        """
        if not active:
            query = 'select full_name from adt;'
        else:
            query = 'select full_name from adt,amt where amt.stop_date is NULL and adt.ID=amt.ID;'
        return [a[0].lower() for a in self.Query(query) if a and a[0]]
            
    def get_attribute_descriptions(self,attribute=None):
        """
        w/out arguments it will return name,ID,type,format,writable for all attributes
        If attribute is an string it will be used as filter to the query. 
        """
        query = 'SELECT full_name,ID,data_type,data_format,writable from adt'
        if attribute: query+=' where full_name like "%s"'%attribute
        return self.Query(query)
    
    def get_attribute_properties(self,ID=None):
        if ID is None: return self.Select('*','apt',asDict=True)
        else: return self.Select('*','apt','ID = %s'%ID,asDict=True)
        
    AMT_COLUMNS = {
        'ID':['archiver,start_date,stop_date'],#ID,archiver,start,stop=line[0:4]
        'per_mod':['per_per_mod'],#MODE_P = line[4] and line[5]
        'abs_mod':['per_abs_mod', 'dec_del_abs_mod', 'gro_del_abs_mod'],#MODE_A = line[6] and line[7:10] 
        'rel_mod':['per_rel_mod', 'n_percent_rel_mod', 'p_percent_rel_mod'],#MODE_R = line[10] and line[11:14]
        'thr_mod':['per_thr_mod', 'min_val_thr_mod', 'max_val_thr_mod'],#MODE_T = line[14] and line[15:18]
        'cal_mod':['per_cal_mod', 'val_cal_mod', 'type_cal_mod', 'algo_cal_mod'],#MODE_C = line[18] and line[19:23] 
        'dif_mod':['per_dif_mod'],#MODE_D = line[23] and line[24] 
        'ext_mod':['ext_mod'],#MODE_E = line[25]
        }
            
    def get_attribute_modes(self,attribute='',modes=[],asDict=False):
        '''
        This method reads the contents of the table AMT and returns a dictionary if asDict is True {ID:[ID,MODES]} 
        
        The argument modes can be used to filter the returned modes (e.g ('per_mod','abs_mod','rel_mod')
        
        adt.keys() at 2013:
          ID,archiver,start_date,stop_date,per_mod,per_per_mod,
          abs_mod,per_abs_mod,dec_del_abs_mod,gro_del_abs_mod,
          rel_mod,per_rel_mod,n_percent_rel_mod,p_percent_rel_mod,
          thr_mod,per_thr_mod,min_val_thr_mod,max_val_thr_mod,
          cal_mod,per_cal_mod,val_cal_mod,type_cal_mod,algo_cal_mod,
          dif_mod,per_dif_mod,ext_mod
        '''
        if not attribute:
            val = self.Select('*','amt','stop_date is NULL',asDict=asDict)
        else:
            val = self.Select('*','amt',"stop_date is NULL and ID in (select ID from adt where full_name like '%s')"%attribute,asDict=asDict)
        if not val:
            return {} if asDict else []
        elif not asDict:
            #ID,archiver,start,stop=line[0:4]
            #start_date = time.mktime(time.strptime(str(start),'%Y-%m-%d %H:%M:%S'))
            #arch_mode='MODE_P,'+str(line[5])
            #if line[6]: arch_mode+=(',MODE_A,'+','.join(str(l) for l in line[7:10]))
            #if line[10]: arch_mode+=(',MODE_R,'+','.join(str(l) for l in line[11:14]))
            #if line[14]: arch_mode+=(',MODE_T,'+','.join(str(l) for l in line[15:18]))
            #if line[18]: arch_mode+=(',MODE_C,'+','.join(str(l) for l in line[19:23]))
            #if line[23]: arch_mode+=(',MODE_D,'+str(line[24]))
            #if line[25]: arch_mode+=(',MODE_E')
            #att=self.getAttributeByID(ID)
            #self.attributes[att.name].setArchiver(archiver,start_date,arch_mode)
            if len(val)==1: return val[0]
            else: return dict((line[0],line) for line in val)
        else:
            result = {}
            for row in val:
                result[row['ID']] = dict((k,row[k]) for k in ('ID','archiver','start_date','stop_date'))
                result[row['ID']].update((k,[row[j] for j in v]) for k,v in self.AMT_COLUMNS.items() if '_mod' in k and row[k])
            return result if len(result)!=1 else result.popitem()[1]
    
    def get_last_attribute_values(self,table,n,check_table=False):
        """
        Check table set to False as sometimes order of insertion is not the same as expected, BE CAREFUL WITH THIS ARGUMENT!
        """
        query = table
        if check_table:
            table_size = self.getTableSize(table)
            if table_size>1e3:
                x = max((2*n,20))
                query = '(select * from %s limit %d,%d)'%(table,table_size-x,x)
        if 'read_value' in self.getTableCols(table):
            return self.Query('SELECT time,read_value from %s T order by T.time desc limit %d'%(query,n))
        else:
            return self.Query('SELECT time,value from %s T order by T.time desc limit %d'%(query,n))
    
    def get_attribute_values(self,table,start_date=None,stop_date=None,
                             desc=False,N=0,unixtime=True):
        """
        This method returns values between dates from a given table.
        If stop_date is not given, then anything above start_date is returned.
        desc controls the sorting of values
        
        unixtime = True enhances the speed of querying by a 60%!!!! 
        (due to MySQLdb implementation of datetime)
        
        If N is specified:
        
         * Query will return last N values if there's no stop_date
         * If there is, then it will return the first N values
         
        start_date and stop_date must be in a format valid for SQL
        """
        what = 'UNIX_TIMESTAMP(time)' if unixtime else 'time'
        what += ',read_value' if 'read_value' in self.getTableCols(table) else ',value'
        interval = ''
        if stop_date:
            interval = "where time between '%s' and '%s'"%(start_date,stop_date)
        elif start_date and fandango.str2epoch(start_date):
            interval = "where time > '%s'"%start_date
        query = 'select %s from %s %s order by time' % (what,table,interval)
        if desc or (not stop_date and N>0) or (N<0):
            query+=" desc"
        if N!=0: query+=' limit %s'%abs(N)
        
        if not desc and ((not stop_date and N>0) or (N<0)):
            #THIS WILL BE APPLIED ONLY WHEN LAST N VALUES ARE ASKED
            return list(reversed(self.Query(query)))
        else:
            self.getCursor(klass=MySQLdb.cursors.SSCursor)
            return self.Query(query)
        
    def create_attribute_tables(self,attrlist=None):
        """
        The Java equivalent to this codes was in the following files:
        - ./hdbtdbArchivingApi/fr/soleil/hdbtdbArchivingApi/ArchivingApi/ConfigConst.java:403
        - ./hdbtdbArchivingApi/fr/soleil/hdbtdbArchivingApi/ArchivingApi/AttributesManagement/AttributeExtractor/DataGetters/MySqlDataGetters.java
        - ./hdbtdbArchivingApi/fr/soleil/hdbtdbArchivingApi/ArchivingApi/AttributesManagement/AttributeExtractor/GenericExtractorMethods.java
        - ./hdbtdbArchivingApi/fr/soleil/hdbtdbArchivingApi/ArchivingApi/AttributesManagement/AdtAptAttributes/MySqlAdtAptAttributes.java
        """
        from PyTango import AttrDataFormat,AttrWriteType,ArgType
        TAB_SCALAR_RO = [ "time", "value" ]
        TAB_SCALAR_WO = [ "time", "value" ]
        TAB_SCALAR_RW = [ "time", "read_value", "write_value" ]
        TAB_SPECTRUM_RO = [ "time", "dim_x", "value" ]
        TAB_SPECTRUM_RW = [ "time", "dim_x", "read_value", "write_value" ]
        TAB_IMAGE_RO = [ "time", "dim_x", "dim_y", "value" ]
        TAB_IMAGE_RW = [ "time", "dim_x", "dim_y", "read_value", "write_value" ]
        
        existing = self.getTables()
        done = {}
        adt = dict((k,(aid,atype,aformat,awrite)) for k,aid,atype,aformat,awrite in self.get_attribute_descriptions())
        if attrlist is None: attrlist = adt.keys()
        for a in attrlist:
            aid,atype,aformat,awrite = adt[a]
            table_name = self.get_table_name(aid)
            if table_name not in existing:
                query = "CREATE TABLE `" + table_name + "` (`time` datetime NOT NULL default '0000-00-00 00:00:00', "
                if aformat in (AttrDataFormat.SPECTRUM,AttrDataFormat.IMAGE,):
                    query += "`dim_x` " + "SMALLINT NOT NULL, "
                if aformat in (AttrDataFormat.IMAGE,):
                    query += "`dim_y` " + "SMALLINT NOT NULL, "
                stype = "LONGBLOB" if aformat==AttrDataFormat.IMAGE \
                        else ("BLOB" if aformat==AttrDataFormat.SPECTRUM
                        else ("varchar(255)" if atype in (ArgType.DevString,ArgType.DevBoolean,) 
                        else ("double")))
                if awrite in (AttrWriteType.READ,AttrWriteType.WRITE):
                    query += "`value` " + stype + " default NULL "
                else:
                    query += "`read_value` " + stype + " default NULL, "+"`write_value` " + stype + " default NULL"
                query += ") ENGINE = MyIsam" #CRITICAL!!!
                print 'Creating %s: %s'%(a,query)
                self.Query(query)
                done[a]=query
        return done.keys()
    
    def clean_attribute_modes(self,date):
        """
        Cleanup all unactive modes from DB if stop_date is older than date
        """
        try: 
            self.db.Query("DELETE FROM amt WHERE stop_date IS NOT NULL AND stop_date < '%s'"%date)
        except Exception,e: 
            print 'ArchivingDB(%s).clean_attribute_modes(%s) failed!: %s'%(self.db_name,date,e)
            return False
        return True
    
    def get_table_updates(self,name=''):
        if name and not str(name).startswith('att_'):
            n = self.get_table_name(name if isinstance(name,int) else self.get_attribute_ID(name))
            print '%s => %s'  % (name,n)
            name = n
        q = 'select table_name,update_time from information_schema.tables where table_schema like "%s"'%self.db_name
        if name: q+=" and table_name like '%s'"%name
        updates = dict((a,fandango.date2time(t) if t else 0) for a,t in self.Query(q))
        return updates


###############################################################################

###############################################################################
# DB Methods

SCHEMAS = ('hdb','tdb','snap')

from fandango import time2date,str2time
    
def repair_attribute_name(attr):
    """
    Remove "weird" characters from attribute names
    """
    import re
    return re.sub('[^a-zA-Z-_\/0-9\*]','',attr)
            
def get_table_name(ID):
    ID = int(ID)
    return 'att_%05d'%ID if ID<10000 else 'att_%06d'%ID

def get_table_updates(api='hdb'):
    import PyTangoArchiving
    if fun.isString(api): 
        api = PyTangoArchiving.ArchivingAPI(api)
    if isinstance(api,PyTangoArchiving.ArchivingAPI):
        db = api.db
    if isinstance(api,fandango.db.FriendlyDB):
        db = api
    updates = db.Query('select table_name,update_time from information_schema.tables where table_schema like "%s"'%api.schema)
    updates = dict((a,fun.date2time(t) if t else 0) for a,t in updates)    
    return updates

def get_partitions_from_query(db, q):
    eq = 'explain partitions '+q
    c = db.Query(eq,export=False)
    i = (i for i,r in enumerate(c.description) if 'partitions' in str(r)).next()
    r = c.fetchone()[i]
    c.close()
    return r

def decimate_db_table_by_time(db,table,att_id,tstart,tend,period=1,
        id_column="att_conf_id",time_column='data_time',min_to_delete=3,
        optimize = False):
    """
    This simplified method will remove all values in a table that are nearer than a given period
    It doesnt analyze values, it just gets the last value within the interval
    
    It is the most suitable for hdb++ and arrays
    
    Partition optimization and repair should be called afterwards
    
    https://dev.mysql.com/doc/refman/5.6/en/partitioning-maintenance.html
    
    ALTER TABLE t1 REBUILD PARTITION p0, p1;
    ALTER TABLE t1 OPTIMIZE PARTITION p0, p1;
    ALTER TABLE t1 REPAIR PARTITION p0,p1;
    """
    t0 = fn.now()
    s0 = db.getTableSize(table)
    if fn.isNumber(tstart):
        tstart,tend = fn.time2str(tstart),fn.time2str(tend)
    q = "select distinct CAST(UNIX_TIMESTAMP(%s) AS DOUBLE) from %s where %s = %s and %s between '%s' and '%s'" % (
        time_column, table, id_column, att_id, time_column, tstart, tend)
    partitions = get_partitions_from_query(db,q)
    print('Query: '+q)
    print('table size is %s, partitions affected: %s' % (s0, partitions))
    vals = db.Query(q)
    t1 = fn.now()
    print('query took %d seconds, %d rows returned' % ((t1-t0), len(vals)))
    if not vals: 
        return
    goods,p = [vals[0][0]],vals[0][0]
    for i,v in enumerate(vals):
        v = v[0]
        if v > period+goods[-1] and p!=goods[-1]:
            goods.append(p)
        p = v
        
    print(fn.now()-t1)
    print('%d rows to delete, %d to preserve' % (len(vals)-len(goods), len(goods))) 
    for i in range(len(goods)-1):
        s,e = goods[i],goods[i+1]
        s,e = fn.time2str(s,us=True),fn.time2str(e,us=True)
        dq = "delete from %s where %s = %s and %s > '%s' and %s < '%s'" % (
            table, id_column, att_id, time_column, s, time_column, e)
        if not i%1000: print(dq)
        db.Query(dq)
        
    t2 = fn.now()
    s1 = db.getTableSize(table)
    print('deleting %d rows took %d seconds' % (s0-s1, t2-t1))
    if optimize:# or (goods[-1] - goods[0]) > 86400*5:
        rq = 'alter table %s optimize partition %s' % (table,partitions)
        print(rq)
        db.Query(rq)
        print('Optimizing took %d seconds' % (fn.now()-t2))
        
    return s1-s0
    
    

def decimate_db_table(db,table,host='',user='',passwd='',start=0,end=0,period=300,iteration=1000,condition='',cols=None,us=True,test=False, repeated = False):
    """ 
    This method will remove all values from a MySQL table that seem duplicated 
    in time or value.
    All values with a difference in time lower than period will be kept.
    
    To use it with hdb++:
    
    decimate_db_table('hdbpp',user='...',passwd='...',
      table = 'att_scalar_devdouble_ro',
      start = 0,
      end = now()-600*86400,
      period = 60, #Keep a value every 60s
      condition = 'att_conf_id = XX',
      iteration = 1000,
      columns = ['data_time','value_r'],
      us=True,
      )
    """
    print('Decimating all repeated values in %s(%s) with less '
      'than %d seconds in between.'%(table,condition,period))
    
    db = FriendlyDB(db,host,user,passwd) if not isinstance(db,FriendlyDB) else db
    #rw = 'write_value' in ','.join([l[0] for l in db.Query("describe %s"%table)]).lower()
    #date,column = 'read_value,write_value' if rw else 'value'
    columns = cols or ['time','value']
    date,column = columns[0],columns[1:]
    start = time2date(start) if isNumber(start) else time2date(str2time(start))
    t0,vw0,now = start,None,time2date(time.time())
    end = time2date(end) if isNumber(end) else time2date(str2time(end))
    removed,pool,reps = 0,[],[]
    count = 0
    
    ## WHY T0 AND END ARE DATES!?!? : to be easy to compare against read values

    while t0<(end or now):

        query = "select %s,%s from %s where" %(date,','.join(column),table)
        query += " '%s' < %s"%(date2str(t0,us=True),date)#,date2str(end))
        if condition: query+=' and %s'%condition
        query += ' order by %s'%date
        query += ' limit %d'%iteration
        values = db.Query(query)
        #print(query+': %d'%len(values))
        #print('inspecting %d values between %s and %s'%(len(values),date2str(t0),date2str(end)))
        
        if not values: 
            break
          
        for i,v in enumerate(values):
            count += 1
            t1,vw1 = v[0],v[1:1+len(column)] #v[1],(rw and v[2] or None)
            #print((i,count,t1,vw0,vw1))
            e0,e1 = 1e-3*int(1e3*date2time(t0)),1e-3*int(1e3*date2time(t1)) #millisecs
            tdelta = e1-e0
            is_last = i >= (len(values)-1) or t1 >= end
            buff = len(pool)

            if is_last or tdelta>=period or vw0!=vw1:
                #if tdelta>=period: print('%s >= %s'%(tdelta,period))
                #elif vw0!=vw1: print('%s != %s'%(vw0,vw1))
                #else: print('i = %s/%s'%(i,len(values)))
                # End of repeated values, apply decimation ...
                if buff:
                    # Dont apply remove on windows < 1 second
                    e1 = date2time(values[i-1][0]) #previous value
                    if True: #(int(e1)-int(e0))>1:
                        #print('remove %d values in pool'%len(pool))
                        if not test:
                            #Don't use the between syntax!!
                            q = "delete from %s where "%table
                            if condition:
                                q+= condition+' and '
                            #e0,e1 = e0+1,e1-1 #t0 should not be removed!
                            q+= "%s > '%s' and "%(date,time2str(e0,us=us)) 
                            q+= "%s < '%s'"%(date,time2str(e1,us=us))
                            #print(q)
                            #removed += buff
                            db.Query(q)

                        #print('t0: %s; removed %d values' % (date2str(t0),buff-1))
                        #print('pool:%s'%str(pool))
                        
                if reps:
                    if not test:
                        #print('repeated timestamp: %s,%s == %s,%s'%(t0,vw0,t1,vw1))
                        q = "delete from %s where "%(table)
                        if condition:
                            q+= condition+' and '
                        q+= "%s = '%s' limit %d" % (
                          date,date2str(reps[-1],us=us),len(reps))
                        #print(q)
                        db.Query(q)                
 
                pool,reps = [],[]
                #print('%s => %s'%(t0,t1))
                t0,vw0 = t1,vw1

            else:
                # repeated values with tdiff<period will be removed in a single query
                    
                # This should apply only if values are different and timestamp equal?
                # if timestamp is repeated the condition t < d < t is useless
                # repeated timestamps are removed directly
                #print(tdelta)
                if repeated and not tdelta:
                    reps.append(t1)
                    #print(('reps',t1))
                        
                elif vw0 == vw1:
                    #if buff and not buff%100:
                    #    print('%s repeated values in %s seconds'%(buff,tdelta))
                    pool.append(t1)

                    #removed +=1  
                
                else: pass
                #print((vw0,vw1))                  
                    
            if is_last: break
    
    query = "select count(*) from %s where" %(table)
    query += " '%s' < %s and %s < '%s'"%(date2str(start,us=us),date,date,date2str(end,us=us))
    if condition: query+=' and %s'%condition   
    cur =  db.Query(query)[0][0]
    removed = count-cur

    print('decimate_db_table(%s,%s) took %d seconds to remove %d = %d - %d values'%(
      table,condition,time.time()-date2time(now),removed,count,cur))

    return removed


def create_attribute_tables(attribute):
    raise 'Method moved to PyTangoArchiving.dbs module'

def import_into_db(db,table,data,delete=False,offset=0):
    """
    db = a FriendlyDB instance
    table = table name
    data = [(time,value)] array
    offset = offset to apply to time values
    delete = boolean, if True the data between t0 and t-1 will be deleted from db before inserting.
    """
    #raise '@TODO:TEST THIS IN ARCHIVING02 BEFORE COMMIT'
    from fandango import time2str,date2str,date2time
    print 'import_into_db(%s,%s,[%s],%s,%s)'%(db,table,len(data),delete,offset)
    if delete: 
        limits = data[0][0],data[-1][0]
        t = db.Query("select count(*) from %s where time between '%s' and '%s'"%(table,time2str(limits[0]),time2str(limits[1])))[0]
        print('deleting %s values from %s'%(t,table))
        db.Query("delete from %s where time between '%s' and '%s'"%(table,time2str(limits[0]),time2str(limits[1])))
    if not db.Query('SHOW INDEX from %s'%table):
        try: db.Query('create index time on  %s (time)'%table)
        except: pass
    print('inserting %d values into %s ...'%(len(data),table))
    #for i,d in enumerate(data):
        #t = (fandango.time2str(d[0]+offset),d[1])
        #q = "INSERT INTO %s VALUES('%s',%s)"%(table,t[0],t[1])
        #db.Query(q)
    l,total = [],0
    for i,d in enumerate(data):
        l.append(d)
        if not (len(data)-(i+1))%100:
            q = "INSERT INTO `%s` VALUES %s ;"%(table,', '.join("('%s',%s)"%(fun.time2str(d[0]+offset),d[1] if 'none' not in str(d[1]).lower() else 'NULL') for d in l))
            #print q[:160]
            db.Query(q)
            total += len(l)
            print i,len(l),total
            l = []
    return total,len(data)

    #net = fandango.db.FriendlyDB('net6020a',user='...',passwd='...')
    #hdb = PyTangoArchiving.archiving.ArchivingAPI('hdb')
    #dev_table = dict((t[1].lower()[:4],t[0].lower()) for t in net.Query('select device,id from devices'))
    #def insert_data(r,offset,delete=''):
    #def get_data(r,dt=dev_table):
        #i = r.split('/')[-1]
        #if i[:4] not in dt:
            #print('no device found for %s'%i)
            #return
        #d = dt[i[:4]]
        #t = 'val_10sec_%s_0913'%d
        #a = 'NeutronDRMean' if r.endswith('n') else 'AccDRMean'
        #data = [(fandango.date2time(d[0]),d[1]) for d in net.Query("select time,%s from %s"%(a,t))]
        #return data
        
## @name Methods for repairing the databases
# @{


def RepairColumnNames():
    db = MySQLdb.connect(db='hdb',user='root')
    q = db.cursor()
    
    q.execute('select ID,full_name,writable from adt')
    adt=q.fetchall()
    
    print 'There are %d attribute_ID registered in the database'%len(adt)
    done=0
    for line in adt:
        ID = line[0]
        full_name = line[1]
        writable = line[2]
        print 'ID %05d: %s, w=%d'%(ID,full_name,writable)
        q.execute('describe %s'%get_table_name(ID))
        describe=q.fetchall()
        col_name=describe[1][0]
        col_type=describe[1][1]
        if writable==int(PyTango.AttrWriteType.READ) and col_name!='value':
            query='ALTER TABLE %s CHANGE COLUMN %s value %s AFTER time'%(get_table_name(ID),col_name,col_type)
            print 'query: ',query
            q.execute(query)
            done+=1
            
    print 'Attributes repaired: %d'%done    
                
##@}

def listLastTdbTime():
    db = MySQLdb.connect(db='tdb')
    q = db.cursor()
    q.execute('show tables')
    #It returns a TUPLE of TUPLES!!!, not a list!
    alltables = q.fetchall()
    
    q.execute('select ID,archiver from amt where stop_date is NULL')
    attribs = q.fetchall()
    
    attrtables = [ get_table_name(i[0]) for i in attribs ]
    print str(len(attribs))+' attributes being archived.'
    
    print 'Searching newest/oldest timestamps on attribute tables ...'
    results = []
    tmin,tmax = None,None
    
    for i,a in enumerate(attrtables):
        q.execute('select max(time),min(time) from '+a);
        #q.execute('select time from '+a+' order by time desc limit 1')
        #type returned is datetime.datetime
        row = q.fetchone()
        date,date2 = row[0],row[1]
        if tmax is None or date>tmax:
            tmax = date
        if tmin is None or date2<tmin:
            tmin = date2
        results.append((date,date2,a))
        print '\r%05d/%05d:\tOldest:%s;\tNewest:%s'%(i,len(attrtables),str(tmin),str(tmax)),
        sys.stdout.flush()
        
    results.sort()
    """
    print 'The last updated time found in database is '+str(results[0][1])+'-'+str(results[0][0])
    print 'Difference with newest is '+str(results.pop()[0]-results[0][0])
    
    print '\n'
    """
    
    
def RemoveWrongValues(db,table,column,null_value,ranges,dates,extra_clauses='',check=False):
    ''' Sets the specified null_value for all values in columnd out of specified ranges
    Usage (for removing all temperatures above 200 degrees): 
     * RemoveWrongValues('hdb','att_00001','value',None,[0,200])
    @remark Values cannot be deleted from archiving tables, NULL values must be inserted instead
    
    #EXAMPLE: 
    #In [42]:tables = [v.table for k,v in api.attributes.items() if re.match('ws/ct/plctest3/.*',k)]
    #In [44]:[PyTangoArchiving.utils.RemoveWrongValues('hdb',t,'value',None,[0,500],['2009-03-26','2009-04-07']) for t in tables]
    #In [48]:[PyTangoArchiving.utils.RemoveWrongValues('hdb',t,'value',None,[50,150],['2009-03-30 19:00:00','2009-04-01 19:00:00']) for t in tables]
    '''
    result = False
    start,stop=dates
    if type(start) is not str: start=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(start))
    if type(stop) is not str: stop=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(stop))
    ranges = type(ranges) in (list,set,tuple,dict) and ranges or list(ranges)
    max_,min_ = max(ranges),(len(ranges)>1 and min(ranges) or None)
       
    db = MySQLdb.connect(db='hdb',user='root')
    q = db.cursor()
    if check: query = "SELECT count(*) FROM %s" % (table)
    else: query = "UPDATE %s SET %s=%s" % (table,column,null_value is None and 'NULL' or null_value)
    where = " WHERE (%s" % ("%s > %s"%(column,max_))
    where += min_ is not None and " OR %s < %s)" % (column,min_) or ")"
    where += " AND (time BETWEEN '%s' AND '%s')" % (start,stop)
    if extra_clauses:
        where = " AND (%s)" % extra_clauses

    print 'the query is : ', query+where
    q.execute(query+where)
    if check:
        result = q.fetchone()[0]
        print 'result is %s; type is %s'%(result,type(result))
        print 'Values to remove: %d'%int(result)
    else:
        result = True
    db.close()
    return result
    
    
    #adt=q.fetchall()    
    #print 'There are %d attribute_ID registered in the database'%len(adt)
    #done=0
    #for line in adt:
        #ID = line[0]
        #full_name = line[1]
        #writable = line[2]
        #print 'ID %05d: %s, w=%d'%(ID,full_name,writable)
        #q.execute('describe att_%05d'%ID)
        #describe=q.fetchall()
        #col_name=describe[1][0]
        #col_type=describe[1][1]
        #if writable==int(PyTango.AttrWriteType.READ) and col_name!='value':
            #query='ALTER TABLE att_%05d CHANGE COLUMN %s value %s AFTER time'%(ID,col_name,col_type)
            #print 'query: ',query
            #q.execute(query)
            #done+=1
            
    
def rename_archived_attributes(attribs,load=False,restart=False,modes={'MODE_P':[10000]},schemas=('hdb','tdb')):
    """
    Renaming attributes in archiving 
    PyTangoArchiving.utils.rename_archived_attributes({oldname:newname}) 
    The following actions must be automated for both HDB and TDB
    """
    import archiving
    attribs = dict((k.lower(),v.lower()) for k,v in attribs.items())
    for schema in schemas:
        api = archiving.ArchivingAPI(schema)
        api.load_dedicated_archivers()
        #Get the list of old names 
        targets = dict((a,api[a].ID) for a in api if a in attribs)
        #Search for archivers 
        archivers = fandango.dicts.defaultdict(set)
        servers = fandango.dicts.defaultdict(set)
        for a in targets:
            arch = api[a].archiver
            if arch:
                servers[fandango.tango.get_device_info(arch).server].add(arch)
                archivers[arch].add(a)
        astor = fandango.Astor()
        if load: astor.load_from_devs_list(archivers.keys())
        
        #Check if they are dedicated 
        dedicated = dict((a,api[a].dedicated.lower()) for a in targets if api[a].dedicated)
        print('>> update dedicated')
        properties = []
        for arch in set(dedicated.values()):
            prop = map(str.lower,api.tango.get_device_property(arch,['reservedAttributes'])['reservedAttributes'])
            nprop = [attribs.get(p,p) for p in prop]
            properties.append((arch,nprop))
        print properties
        if load: [api.tango.put_device_property(arch,{'reservedAttributes':nprop}) for arch,nprop in properties]
            
        #Store the list of modes, 
        #NOP!, instead we will try to use the new modes provided as argument.
        #modes = dict.fromkeys(modes_to_string(api[a].modes) for a in targets)
        #[modes.__setitem__(k,[attribs[a] for a in targets if modes_to_string(api[a].modes)==k]) for k in modes.keys()]
        
        for server,archs in servers.items():
            if restart or modes is not None:
                for arch in archs:
                    atts = archivers[arch]
                    print('>> stopping archiving: %s'%atts)
                    if load: api.stop_archiving(atts)
            print('>> stopping archiver %s: %s'%(server,archs))
            if load: astor.stop_servers(server)
            for arch in archs:
                atts = archivers[arch]
                print('>> modifying adt table for %s attributes (%d)'%(arch,len(atts)))
                queries = []
                for name in atts:
                    ID = targets[name]
                    name = attribs[name]
                    device,att_name = name.rsplit('/',1)
                    domain,member,family = device.split('/')
                    queries.append("update adt set full_name='%s',device='%s',domain='%s',family='%s',member='%s',att_name='%s' where ID=%d" % (name,device,domain,family,member,att_name,ID))
                print '\n'.join(queries[:10]+['...'])
                if load: [api.db.Query(query) for query in queries]
            print('>> start %s archivers '%server)
            if load: 
                time.sleep(10)
                astor.start_servers(server)
                
        if load:
            fandango.Astor("ArchivingManager/*").stop_servers()
            time.sleep(15)
            fandango.Astor("ArchivingManager/*").start_servers()
            time.sleep(20)
        if restart or modes:
            print('>> start archiving: %s'%modes)
            if load: 
                api.start_archiving(attribs.values(),modes)
                #for m,atts in modes.items():
                    #m = modes_to_dict(m)
                    #api.start_archiving(atts,m)
    return archivers

def repair_attribute_names(db,attrlist=None,upper=False,update=False):
    """ 
    This method sets all domain/family/member names to upper case in the ADT table 
    db must be a FriendlyDB object like, db = FriendlyDB(db_name,host,user,passwd)
    """
    allnames = db.Query('SELECT full_name,device,att_name,ID FROM adt ORDER BY full_name',export=True)
    failed = 0
    device,attrs = '',[]
    if attrlist: attrlist = [a.lower() for a in attrlist]
    for line in sorted(allnames):
        fname,dev,att_name,ID = line
        if attrlist and fname.lower() not in attrlist:
            continue
        if dev.lower()!=device.lower(): #Device changed
            try:
                dp = PyTango.DeviceProxy(dev)
                device = dp.name()
                attrs = dp.get_attribute_list() #Getting real attribute names
            except:
                attrs = []
            
        try:
            if attrs: #If attribute list is not available we should not modify attribute names
                eq = [a for a in attrs if a.lower()==att_name.lower()]
                if eq: att_name = eq[0]
            #full_name = device+'/'+str(att_name) #Using real device name does not solve the problem when attributes are being re-inserted
            device = dev.upper() if upper else dev.lower()
            full_name = device+'/'+str(att_name)
            if full_name.rsplit('/',1)[0] == fname.rsplit('/',1)[0]: continue #Nothing to update
            domain,family,member = device.split('/')
            q = "update adt set domain = '%s',family = '%s',member = '%s',device = '%s',full_name = '%s', att_name = '%s' where id=%s" % (domain,family,member,device,full_name,att_name,ID)
            print "%s: %s"%(fname,q)
            if update: db.Query(q) 
        except Exception,e:
            print '%s: %s'%(fname,e)
            print traceback.format_exc()
            break
            failed += 1
    
    if update: db.Query('COMMIT')
    ok = len(allnames)-failed
    if update: print '%d names updated' % (len(attrlist or allnames)-failed)
    return ok    
