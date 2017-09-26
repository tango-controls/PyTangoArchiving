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
        return fandango.first(self.get_attributes_IDs(name).values())
    
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
                             desc=False,N=-1,unixtime=True):
        """
        This method returns values between dates from a given table.
        If stop_date is not given, then anything above start_date is returned.
        desc controls the sorting of values
        
        unixtime = True enhances the speed of querying by a 60%!!!! (due to MySQLdb implementation of datetime)
        
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
        if desc or (not stop_date and N>0): query+=" desc"
        if N>0: query+=' limit %s'%N
        
        if not desc and not stop_date and N>0:
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
