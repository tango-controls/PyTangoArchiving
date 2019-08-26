#!/usr/bin/env python2.5
''' @if gnuheader
#############################################################################
##
## file :       object.py
##
## description : see below
##
## project :     Tango Control System
##
## $Author: sergi_rubio $
##
##
## $Revision: 30514 $
##
## copyleft :    ALBA Synchrotron Controls Section, CELLS
##               Bellaterra
##               Spain
##
#############################################################################
##
## This file is part of Tango Control System
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
###########################################################################
@endif

@package PyTangoArchiving.snap

@htmlinclude SnapUsage.html

'''

# by Sergi Rubio Manrique, srubio@cells.es
# ALBA Synchrotron Control Group

import time,traceback,os,re,threading,datetime,sys
from random import randrange
import MySQLdb
import fandango
from fandango import notNone
from fandango import Object,Singleton,CaselessDict
from fandango.log import Logger
from fandango.db import FriendlyDB   
from PyTangoArchiving.common import PyTango,getSingletonAPI

__all__ = ['snap_db','SnapDB','SnapAPI','SnapContext','Snapshot']

TABLES={
    'context':['id_context','time','name','author','reason','description'],
    'ast':['ID','time','full_name','device','domain','family','member','att_name','data_type','data_format','writable','max_dim_x','max_dim_y','levelg','facility','archivable','substitute'],
    }

class SnapAPI(Logger,Singleton):
    """ 
    This object encapsulates the methods used to manage the SnapArchiving System.
    
    An SnapDB object is needed to manage the persistency of 
    MySQL connections and tango proxies.
    
    SnapAPI(user='...',passwd='...',api=None,SINGLETON=True,load=True)
    """

    __instantiated__ = 0

    def __init__(self,user='',passwd='',api=None,SINGLETON=True,load=True):
        """ The SnapAPI is a Singleton object; is instantiated only one time
        @param[in] api An ArchivingAPI object is required.
        """
        print 'Creating SnapAPI object ...'
        if SINGLETON and self.__class__.__instantiated__: return

        self.call__init__(Logger,'SnapDict',
            format='%(levelname)-8s %(asctime)s %(name)s: %(message)s')
        
        try:
            self.tango = fandango.get_database()
            user,host = user.split('@') if '@' in (user or '') else (user,'')
            get_prop = (lambda p: (list(self.tango.get_class_property(
                'SnapArchiver',[p]).values()[0] or ['']))[0])
            user = user or get_prop('DbUser')
            host = host or get_prop('DbHost')
            passwd = passwd or get_prop('DbPassword')
            
        except Exception, e:
            print traceback.format_exc()
            print('ERROR: Unable to get Snapshoting DB settings: ',e)
        
        if api is None:
            if not host: 
                raise Exception('Snap DbHost not defined!')
             ##THIS IS A CommonAPI object!
            api = getSingletonAPI(schema = 'snap',
                host = host, user = user, passwd = passwd)
            
        self.api=api
        print('get manager')
        self.manager=api.get_manager()        
        self.archivers=dict([(a,api.proxies[a]) 
            for a in api.servers.get_class_devices(api.ArchiverClass)])
        self.extractors=dict([(a,api.proxies[a]) 
            for a in api.servers.get_class_devices(api.ExtractorClass)])

        self.contexts={} #{ID:{'author','name','attributes':[]}}
        self.attributes = fandango.CaselessDict()
        try:
            print('conecting')
            print 'Connecting to %s as %s ...' % (host,user)
            self.set_db_config(api,host,user,passwd)
            if load: 
                self.load_contexts()
        except:
            print traceback.format_exc()
            print('ERROR: Unable to load host/contexts')

        if SINGLETON: 
            SnapAPI.__singleton = self
        self.__class__.__instantiated__+=1

    def set_db_config(self,api,host,user,passwd):
        self.db=SnapDB(api,host=host,user=user,passwd=passwd)

    ## @name Get methods
    # @{

    def get_contexts(self,wildcard='*',update=False):
        """ Returns a dicitonary of loaded contexts; if the list was empty it loads all contexts with name matching argument ('*' allowed)
        @param wildcard The argument allows to filter wich contexts are loaded; '*' can be used for All. Lists and dict can be used also.
        """
        if update or not self.contexts: 
            assert wildcard,'SnapAPI_WildcardArgumentRequired'
            ids = self.load_contexts(wildcard)
        if wildcard == '*':
            return self.contexts
        if wildcard in self.contexts:
            return {wildcard:self.contexts[wildcard]}
        return dict((k,c) for k,c in self.contexts.items() if fandango.matchCl(wildcard,c.name))

    def get_context(self, ID=None, name=None, load=False):
        """ Return a single context matching the given int(ID) or str(name)
        """
        if ID is None and name is None:
            raise Exception('SnapAPI_get_context_ArgumentRequired')
        
        if not fandango.isString(ID): #not name and fandango.isNumber(ID):
            #print('get_context(%s(%s))' % (type(ID),ID))
            if not load and ID in self.contexts: 
                return self.contexts[ID]
            else:
                cts = self.db.get_id_contexts(ID)
                if not cts:
                    raise Exception('SnapAPI_Context%sDoesntExist' % ID)
                return SnapContext(self, ID, ct_info=cts[0])
        
        else: 
            #Getting context by name
            name = name or str(ID)
            #print('get_context(%s(%s))' % (type(name),name))
            ids = (sorted(k for k, c in self.contexts.items() 
                        if name.lower()==c.name.lower()) 
                    or sorted(self.load_contexts(name)))
            if ids:
                return fandango.first(self.get_context(i) for i in ids)
            else:
                return None

    def get_random_archiver(self):
        return self.archivers.values()[randrange(len(self.archivers))]

    def get_random_extractor(self):
        return self.extractors.values()[randrange(len(self.extractors))]

    ## @}

    ## @name Load methods
    # @{

    def load_contexts(self, wildcard=None, attributes = False):
        """ Loads a list of contexts from the database using a wildcard
        """
        print('load_contexts(%s)' % wildcard)
        ids = self.db.get_context_ids(wildcard or '*')
        for k in ids:
            try:
                if k not in self.contexts:
                    self.contexts[k] = SnapContext(self, k, load = False)

                elif attributes:
                    self.contexts[k].get_attributes()
                    #self.contexts[k].get_snapshots()
            except:
                print('SnapContext(%s) failed!: %s'%(k,traceback.format_exc()))

            if attributes:
                self.get_attributes(update=True, load=False) #avoid recurse

        if wildcard in (None,'*'):
            to_delete = [k for k in self.contexts if k not in ids]
            if to_delete:
                print 'deleting %d contexts ...'%len(to_delete)
                [self.contexts.pop(k) for k in to_delete]

        return ids
    
    def get_attributes(self,filters=None,update=True,load=False):
        if load: 
            self.load_contexts(attributes = True)
            
        if update or load or not self.attributes: 
            for i,c in self.contexts.items():
                for k,a in c.get_attributes().items():
                    name = a['full_name']
                    #Caching
                    contexts = self.attributes.get(name,{}).get('contexts',set()).union((i,))
                    snaps = self.attributes[name]['snapshots'] if name in self.attributes else None
                    #Updating
                    self.attributes[name] = a
                    if snaps: self.attributes[name]['snapshots'] = self.attributes[name]['snapshots'].union(snaps)
                    self.attributes[name]['contexts'] = contexts

        return sorted(a for a in self.attributes 
            if not filters or fandango.matchCl(filters,a))
    
    def get_attribute_snapshots(self,attribute ,start_date=None, 
            stop_date=None, update = False):
        """
        attribute id or attribute name can be used
        if start_date/stop_date are not given it returns all snapshots
        if given, dates must be in unix time seconds
        """
        self.get_attributes(update = update);
        if isinstance(attribute,int): 
            attribute = fandango.first(a for a,v in self.attributes.items() 
                if v['ID']==attribute)

        values = dict((t[0],t[1:]) for t in 
            self.db.get_snapshots_for_attribute(
                self.attributes[attribute]['ID'],
                table=self.attributes[attribute]['table'],
                start_date=start_date,stop_date=stop_date))

        comments = dict((t[0],t[1:]) for t in 
            self.db.get_snapshots(values.keys()))

        return sorted((fandango.date2time(comments[i][0]),
            {'value':values[i],'comment':comments[i][1],
             'ID':i,'date':comments[i][0]}) 
            for i in values)

    ##@}

    ## @name Context/Snapshot creation
    # @{
    def check_attribute_allowed(self,attr,allow_exceptions=False):
        """ attr must be the full attribute name, it returns True if the snapshoting of the attribute is available. """
        try:     
            if isinstance(attr,PyTango.AttributeInfoEx) or isinstance(attr,PyTango.AttributeInfo):
                ac = attr
            else: 
                ac = PyTango.AttributeProxy(attr).get_config()
            if ac.data_format == PyTango.IMAGE:
                return False
            if str(PyTango.ArgType.values[ac.data_type]) in ('DevBoolean','DevState','DevShort','DevLong','DevUShort','DevULong','DevDouble','DevFloat'):
                return True
            if str(PyTango.ArgType.values[ac.data_type]) == 'DevString' and ac.data_format == PyTango.SCALAR:
                return True
            else:
                ##The attribute is not supported by the actual release of the snapshoting system
                return False
        except Exception,e:
            print 'SnapAPI.check_attribute_allowed(%s): Exception trying to check attribute: %s'%(attr,str(e))
            return allow_exceptions or None

    def filter_attributes_allowed(self,attributes,allow_exceptions=False):
        """ It checks a list of attributes and returns which are allowed to snap. Full name of attribute is required."""
        return [attr for attr in attributes if self.check_attribute_allowed(attr,allow_exceptions)]

    def filter_device_attributes_allowed(self,device,allow_exceptions=False):
        """ It cheks all the attributes of a device. """
        goods = []
        try:
            alq = PyTango.DeviceProxy(device).attribute_list_query()
            [goods.append(device+'/'+attr.name) for attr in alq if self.check_attribute_allowed(attr,allow_exceptions)]
            return goods
        except Exception,e:
            print 'SnapAPI.filter_device_attributes_allowed(%s): Exception trying to check device: %s'%(device,str(e))
            return allow_exceptions and goods or []

    def create_context(self,author,name,reason,description,attributes):
        """ Creates a new context in the database
        @todo Before inserting a new context it should check if another one with same name exists!
        @return the created SnapContext object
        """
        date = time.strftime('%Y-%m-%d',time.localtime())
        nattrs = str(len(attributes))
        self.info('%s; SnapAPI.create_context(%s,%s,%s,%s,%s)'%(date,author,name,reason,description,attributes))
        #args to CreateNewContext with %d attributes are: %s'%(len(attributes),str([author,name,nattrs,date,reason,description]))
        self.manager.set_timeout_millis(60000)
        #sid=self.manager.command_inout('CreateNewContext',[author,name,nattrs,date,reason,description]+[a.lower() for a in attributes])
        sid=self.manager.command_inout('CreateNewContext',[author,name,'0',date,reason,description]+[a.lower() for a in attributes])
        self.debug('\tContext created with id = %s'%sid)
        self.db.renewMySQLconnection()
        #self.db.getCursor(renew=True)
        self.contexts[sid] = self.get_context(sid)
        attrs = self.contexts[sid].get_attributes().values()
        for att in attrs:
            if att['full_name'].lower() not in [a.strip().lower() for a in attributes]:
                self.warning('Unable to add %s attribute to context!'%att['full_name'])
        return self.contexts[sid]
            
    def modify_context(self,context_id,author=None,name=None,reason=None,description=None,attributes=None):#context_id):
        """ 
        modifies the context table in the database 
        
        Based on SnapContext constructor:
            public SnapContext(String[] argin) {
            setAuthor_name(argin[0]);
            setName(argin[1]);
            setId(Integer.parseInt(argin[2]));
            setCreation_date(java.sql.Date.valueOf(argin[3]));
            setReason(argin[4]);
            setDescription(argin[5]);
            // Attribute list construction
            for (int i = 6; i < argin.length; i++) {
            attributeList.add(new SnapAttributeLight(argin[i]));
            }         
        """
        date = time.strftime('%Y-%m-%d',time.localtime())
        context = self.contexts[context_id]
        old_attrs = sorted(a['full_name'].lower() for a in context.get_attributes().values())
        if attributes is None: attributes = old_attrs
        else: attributes = sorted(str(a).strip().lower() for a in attributes)
        nattrs = str(len(attributes))
        self.info('%s; SnapAPI.modify_context(%s,%s,%s,%s,%s)'%(date,author,name,reason,description,attributes))
        #args to CreateNewContext with %d attributes are: %s'%(len(attributes),str([author,name,nattrs,date,reason,description]))
        if attributes!=old_attrs:
            self.manager.set_timeout_millis(60000)
            sid=self.manager.command_inout('CreateNewContext',[notNone(author,context.author),notNone(name,context.name),str(context_id),
                date,notNone(reason,context.reason),notNone(description,context.description)]+[a.lower() for a in attributes]
                )
            self.debug('\tContext created with id = %s'%sid)
            self.db.renewMySQLconnection()
            self.contexts[sid] = self.get_context(sid)
            attrs = [a['full_name'] for a in self.contexts[sid].get_attributes().values()]
            for att in attributes:
                if att.strip().lower() not in attrs:
                    self.warning('Unable to add %s attribute to context!'%att)
            return self.contexts[sid]
        else:
            context.author = author
            context.name = name
            context.reason = reason
            context.description = description
            self.db.update_context(context)
            self.db.renewMySQLconnection()
            return context
        
    def remove_context(self, context_id,load=True):
        """ removes the existing context in the database """
        print "remove context" , context_id
        self.db.remove_context(context_id,snap=True)
        if load: self.load_contexts()
    
    def modify_snapshot(self,snapid,comment=''):
        try:
            print '%s: ArchivingManager.UpdateSnapComment(%s,%s)'%(time.ctime(),snapid,comment)
            self.manager.command_inout('UpdateSnapComment',[[snapid],[str(comment)]])
        except:
            print 'SnapAPI.set_snap_comment(%s): Failed!: %s'%(snapid,traceback.format_exc())

    ## @}
    ## @name Argument conversion
    # @{

    def get_ctxlist_as_dict(self,author,name,date,reason,description,attributes):
        """ converts a list of arguments in a single dictionary """
        return {'author':author,'name':name,'date':date,'reason':reason,'description':description,'attributes':attributes}

    def get_dict_as_ctxlist(self,context):
        """ converts a dictionary in a list of arguments """
        return [context[k] for k in ['author','name','date','reason','description']]+context['attributes']

    ## @}

class SnapContext(object):
    """SnapContext refers to a set of attributes that are stored/reloaded together.
    """

    def __init__(self,api,ID,ct_info={},load=True):
        self.api = api ##WARNING! ... this api is an SnapAPI object, not a CommonAPI
        self.info = self.api.info
        self.db = api.db
        self.ID = ID
        self.get_info(ct_info) # ct_info is loaded here
        self.attributes = {}
        self.snapshots = {}

        if load:
            self.get_attributes()
            self.get_snapshots()

    def __repr__(self):
        return ('SnapContext(%s,%s,%s,%s,Attributes[%d],Snapshots[%d])'%
            (self.ID,self.name,self.author,self.reason,
             len(self.attributes),len(self.snapshots)))

    ## @name Get methods
    # @{

    def get_info(self,ct_info={}):
        if not ct_info:
            cts = self.db.get_id_contexts(self.ID)
            ct_info = cts[0] if cts else None
        self.time = ct_info['time'] if ct_info else 0.
        self.name = ct_info['name'] if ct_info else ''
        self.author = ct_info['author'] if ct_info else ''
        self.reason = ct_info['reason'] if ct_info else ''
        self.description = ct_info['description'] if ct_info else ''
        return ct_info

    def get_attributes(self,update=False):
        if update: 
            self.attributes = {}
        if not self.attributes:
            print('SnapContext(%s).get_attributes(True)' % self.name)
            #[self.attributes.__setitem__(line[0],line[1]) for line in self.db.get_context_attributes(self.ID)]
            ids = sorted(self.db.get_context_attributes(self.ID))
            if ids:
              formats = sorted(self.db.get_attributes_data([i[0] for i in ids]).items())
              tables = self.db.get_attributes_tables(v[1] for v in formats)
              for i,f,t in zip(ids,formats,tables):
                self.attributes[i[0]] = f[1]
                self.attributes[i[0]]['ID'] = i[0]
                self.attributes[i[0]]['table'] = t
                self.attributes[i[0]]['context'] = self.ID
                self.attributes[i[0]]['snapshots'] = set()
        return self.attributes

    def get_snapshots(self, date=None, latest=0):
        """ Returns a dict with {SnapID:(time,comment)} values; it performs a Database Call!
        @date load values FROM this date
        @latest load only this amount of values
        @return By default all snapshots are returned.
        @todo latest argument must be used to query only the latest values from the database
        """
        #print 'In SnapContext.get_snapshots(%s,%s)'%(date,latest)
        kwargs = {'context_id':self.ID}
        if not date and not latest:
            self.snapshots = {}
        if date:
            if type(date) in [int,float]: date = datetime.datetime.fromtimestamp(date)
            last_date = max([s[0] for s in self.snapshots.values()]) if self.snapshots else datetime.datetime.fromtimestamp(0)
            kwargs['dates']=(last_date,date)
        if latest:
            kwargs['limit']=latest

        result = self.db.get_context_snapshots(**kwargs)
        for element in result: #Each element is an (ID,time,Comment) tuple
            self.snapshots[element[0]]=element[1:]
            [v['snapshots'].add(element[0]) 
                for v in self.get_attributes().values()] ##It may be wrong if context have been modified!!
            
        return self.snapshots

        ## @todo SnapExtractor should be used instead of direct attack to the DB
        #try: 
            #self.snapshots = self.api.get_random_extractor().command_inout('GetSnapsForContext',self.ID)
            #raise Exception,'GetSnapsForContext_ResultNotManaged'
        #except Exception,e:
            #print 'SnapContext.get_snapshots: SnapExtractor.GetSnapsForContext failed!: %s' % e

    def get_snapshot(self,snapid=None):
        """Returns an Snapshot dictionary with succesfully read attributes.
        @param snapid The long ID of the snapshot to return
        @return An Snapshot object
        """
        #print 'In SnapContext.get_snapshot(%s)'%snapid
        if snapid is None:
            if not self.snapshots: self.get_snapshots()
            snapid = sorted(self.snapshots.keys())[-1]
        if snapid not in self.snapshots:
            print '.get_snapshot(%d): Unknown snap_id, loading from db ...'%snapid
            self.get_snapshots();
            if snapid not in self.snapshots:
                raise Exception('SnapIDNotFoundInThisContext! {0}'.format(snapid))
        date,comment = self.snapshots[snapid]
        attributes = self.db.get_snapshot_attributes(snapid).values()
        if not attributes:
            self.api.warning('SnapContext.get_snapshot(%d): The attribute list is empty!'%snapid)
        
        self.get_attributes();
        attr_names = [self.attributes[a['id_att']]['full_name'] 
                      for a in attributes]
        attr_values = []

        for a in attributes:
            values = Snapshot.cast_snapshot_values(a,self.attributes[a['id_att']]['data_format'],self.attributes[a['id_att']]['data_type'])
            attr_values.append(tuple(values))

        return Snapshot(snapid,self.ID,time.mktime(date.timetuple()),
                        zip(attr_names,attr_values),comment)

        ## @todo SnapExtractor should be used instead of direct attack to the DB
        #try:
            #attributes = self.api.get_random_extractor().command_inout('GetSnap',snapid)
            #raise Exception,'GetSnap_ResultNotManaged'
        #except Exception,e:
            #print 'SnapContext.get_snapshot: SnapExtractor.GetSnap failed!: %s' % e

    def get_snapshot_by_date(self,date,update=True):
        """ It returns the ID of the last snapshot in db with timestamp below date;
        @param date It can be datetime or time epoch, date=-1 returns the last snapshot; date=0 or None returns the first
        """
        print 'In SnapContext.get_snapshot_by_date(%s)'%date
        if update:
            self.get_snapshots(\
                date=(None if date in (0,-1) else date),\
                ) #latest=(1 if date==-1 else 0))
        
        if not self.snapshots: 
            self.api.debug("There's no snapshots to search for!")
            #print "There's no snapshots to search for!"
            return None

        keys = sorted(self.snapshots.keys(),key=int)
        #Checking limits
        if not date:
            return self.get_snapshot(keys[0])
        if date==-1:
            return self.get_snapshot(keys[-1])
        #Checking dates
        if type(date) in [int,float]:
            date = datetime.datetime.fromtimestamp(date)

        if date>self.snapshots[keys[-1]][0]:
            return self.get_snapshot(keys[-1])
        elif date<self.snapshots[keys[0]][0]:
            #print 'No matching snapshot found'
            return None #
        else:
            for i in range(len(keys)-1):
                if self.snapshots[keys[i]][0]<=date<self.snapshots[keys[i+1]][0]:
                    return self.get_snapshot(keys[i])
            self.api.warning('No matching snapshot found for date = %s'%date)
            print 'No matching snapshot found for date = %s'%date
            return None
    # @}
    #--------------------------------------------------------------------------------------

    def take_snapshot(self,comment='',archiver=None, timewait=1.):
        """ Executes an snapshot for the given context; it could be a context name or ID
        @param comment Text to be inserted in the database.
        """
        self.info('In SnapContext.take_snapshot(%s,%s)'%(comment,None))
        try:
            #last_snap = self.get_snapshot_by_date(-1,update=True)
            #if last_snap: last_snap = last_snap.ID
            last_snap = self.api.db.get_last_snapshot()
            self.info('\tlast snap = %s' % (last_snap))
            
            if archiver and isinstance(archiver,str):
                try:
                    if archiver in self.api.archivers: 
                        archiver = self.api.archivers[archiver]
                    else: 
                        archiver = PyTango.DeviceProxy(archiver)#self.api.get_random_archiver()
                except: 
                    archiver=None

            if not archiver and self.api.manager:
                self.info('Trying to use SnapManager.LaunchSnapshot '
                        '(java archiving release>1.4)')
                try:
                    if 'launchsnapshot' in [str(cmd.cmd_name).strip().lower() 
                            for cmd in self.api.manager.command_list_query()]:
                        #print '... using %s'%self.api.manager
                        archiver = self.api.manager
                    else: archiver = None
                except: archiver = None
            if not archiver:
                self.info('Getting a random archiver (java archiving release<1.4)')
                archiver = self.api.get_random_archiver()

            self.info('\t%s.LaunchSnapshot(%s)'%(archiver.name(),self.ID))
            archiver.set_timeout_millis(60000)
            result = archiver.command_inout('LaunchSnapShot',self.ID) #Awfully, it returns None instead of SnapID
            print '%s.LaunchSnapshot(%s) = %s'%(archiver,self.ID,result)
            time.sleep(timewait) #Waiting for the database to update ...
            
            bID = archiver.getSnapShotResult(self.ID) #It may release some lock?
            nextID = self.api.db.get_last_snapshot() 
            print('New Snapshot(%s): %s, %s?' % (self.ID, nextID, bID))

            if last_snap == nextID: #(snapshot and last_snap == snapshot.ID):
                self.api.error('Launch Snapshot has not added '
                    'a new entry in the database (last == %s!)'%nextID)
                nextID = last_snap + 1
            #else:
            if comment:

                    self.info('\t%s.UpdateSnapComment(%s,%s)'
                              %(archiver.name(), nextID,comment))
                    self.api.manager.command_inout(
                        'UpdateSnapComment',[[nextID],[comment]])

                    if nextID in self.snapshots: 
                        self.snapshots[nextID][1] = comment

            return nextID #self.get_snapshot(nextID)
        except Exception,e:
            msg = traceback.format_exc()
            self.api.error('Exception in LaunchSnapshot: %s'%msg)
            raise Exception(traceback.format_exc())

    #def reload_snapshot(self, key=None):
        #"""Writes snapshot for date to attributes. Shorthand for get_snapshot(before=date).write()"""
        #if type(key) in [int,long] and key not in self.snapshots: self.get_snapshots()

        #self.api.manager.command_inout('SetEquipmentsWithSnapshot')
        #return True

class Snapshot(CaselessDict):
    """Snapshot refers to a number of values taken at the same point in time.
    Snapshots with a comment set will have d.comment = "Comment" and
    a date d.date = time.ctime(), the attributes will be accessible by []
    """
    def __init__(self,ID,ctxID,time_,values,comment=''):
        self.ID = ID
        self.ctxID = ctxID
        self.time = time_
        self.comment = comment
        CaselessDict(self)
        self.update(dict(values))

    def set_comment(self,comment=''):
        try:
            print '%s: ArchivingManager.UpdateSnapComment(%s,%s)'%(time.ctime(),self.ID,comment)
            getSingletonAPI(schema='snap').get_manager().command_inout('UpdateSnapComment',[[self.ID],[comment]])
            self.comment = comment
        except:
            print 'Snapshot(%s).set_comment(...): Failed!: %s'%(self.ID,traceback.format_exc())
            
    @staticmethod
    def cast_snapshot_values(attr_value,data_format,data_type):
        values = [attr_value['value'],] if 'value' in attr_value else [attr_value['read_value'],attr_value['write_value']]
        for i,value in enumerate(values):
            if type(value) is not float:
                if data_format in (PyTango.SPECTRUM,PyTango.IMAGE):
                    value = value.split(',')
                    #if self.attributes[a['id_att']]['data_type'] in (PyTango.DevString,PyTango.DevBoolean,PyTango.DevState):
                        #@todo
                    if data_type in (PyTango.DevFloat,PyTango.DevDouble):
                        try: value = [float(v) for v in value]
                        except: pass
                    if data_type in (PyTango.DevShort,PyTango.DevUShort,PyTango.DevLong,PyTango.DevULong):
                        try: value = [int(v) for v in value]
                        except: pass
                    if data_format is PyTango.IMAGE:
                        x,y = attr_value['dim_x'],attr_value['dim_y']
                        try: value = [value[j:j+x] for j in range(len(value))[::x]]
                        except: pass
            values[i] = value
        return values

    def __repr__(self):
        return 'Snapshot(%s,%s,%s,{%s})'%(self.ID,time.ctime(self.time),self.comment,','.join(self.keys()))
    #def __init__(self,attr_list = [],time_=time.time(),comment=''):
        #self.attr_list = attr_list
        #self.date = time_
        #self.comment = comment
    pass

class SnapDB(FriendlyDB,Singleton):
    """
    This class simplifies the direct access to the Snapshot database.
    It takes care of the interaction with the SQL database.
    tau.core.utils.Singleton forbids to create multiple SnapDB instances.
    """
    
    #[snap.db.getTableCols(t) for t in snap.db.getTables()]
    TABLES = {
        'ast': ['ID',
            'time',
            'full_name',
            'device',
            'domain',
            'family',
            'member',
            'att_name',
            'data_type',
            'data_format',
            'writable',
            'max_dim_x',
            'max_dim_y',
            'levelg',
            'facility',
            'archivable',
            'substitute'],
        'context': ['id_context', 'time', 'name', 'author', 'reason', 'description'],
        'list': ['id_context', 'id_att'],
        'snapshot': ['id_snap', 'id_context', 'time', 'snap_comment'],
        't_im_1val': ['id_snap', 'id_att', 'dim_x', 'dim_y', 'value'],
        't_im_2val': ['id_snap','id_att','dim_x','dim_y','read_value','write_value'],
        't_sc_num_1val': ['id_snap', 'id_att', 'value'],
        't_sc_num_2val': ['id_snap', 'id_att', 'read_value', 'write_value'],
        't_sc_str_1val': ['id_snap', 'id_att', 'value'],
        't_sc_str_2val': ['id_snap', 'id_att', 'read_value', 'write_value'],
        't_sp_1val': ['id_snap', 'id_att', 'dim_x', 'value'],
        't_sp_2val': ['id_snap', 'id_att', 'dim_x', 'read_value', 'write_value']
        }

    def __init__(self,api=None,host='',user='',passwd='',db_name='snap'):
        """SnapDB Singleton creator
        @param api it links the SnapDB object to its associated SnapAPI (unneeded?)
        """
        print 'Creating SnapDB object ... (%s,%s)' % (host,db_name)
        self._api=api if api else None
        FriendlyDB.__init__(self,db_name,host or (api and api.arch_host) 
                            or 'localhost',user,passwd)
        assert hasattr(self,'db'),'SnapDB_UnableToCreateConnection'
        [self.getTableCols(t) for t in self.getTables()]
        self.setLogLevel('INFO')


    def check_snap_db(self):
        """
        This method should check and correct:
         - Contexts with no attributes
         - Contexts with no snapshots
         - Snapshots with no values
         - Snapshots with no context assigned
        """
        raise Exception('@TODO:NotImplemented!')
    
    ## @name Context methods
    # @{

    def search_context(self,id_context=None,clause=''):
        """ Get all elements from context table
        @return A dictionary with all context table columns is returned
        """
        print('In search_context(%s, %s)' % (id_context,clause))
        if not id_context:
            clause1=''
        elif type(id_context) is int:
            clause1 = 'context.id_context=%d'%id_context
        elif type(id_context) is str:
            clause1 = "context.name like '%s'"%id_context
        elif type(id_context) is dict:
            clause1=''
            for i in range(len(id_context)):
                k,v = id_context.items()[i]
                clause1+= "%s like '%s'"%(k,v) if type(v) is str else '%s=%s'%(k,str(v))
                if (i+1)<len(id_context): clause1+=" and "
        else:
            raise Exception("SnapDB_getContextList_ArgTypeNotSuported")

        values = self.Select('*','context',clause1,asDict=True)
        print('%d contexts found' % len(values))
        return values

    def get_context_ids(self,context):
        """returns all the context IDs with a name matching the given string or regular expression"""
        if any(s in context for s in '.*@$%&!|[{(<:^'): #It is a regular expression; comparing with all context names!
            #WARNING! ... it should be compared with a pre-loaded list of contexts! ... at SnapAPI
            els = self.Select('name,id_context','context')
            ids = [l[1] for l in els if fandango.matchCl(context,l[0])]
            return ids
        else:    #It is a normal SQL clause
            return [t[0] for t in (self.Select('id_context','context',"name like '%s'"%context) or [])]
        
    def get_context_snapshots(self,context_id,dates=None,limit=0):
        """
        @remarks It returns newest snapshot first!
        """
        cols = ['id_snap','time','snap_comment']
        clause = 'id_context=%d' % context_id
        if dates: clause+=" AND time BETWEEN '%s' AND '%s'"%dates
        self.db.commit() ## @remark it forces a db.commit() to be sure that last inserted values are there
        result = self.Select(cols,'snapshot',clause,order='time desc',limit=limit)
        return result

    def get_id_contexts(self,id_context):
        return self.Select('*','context','id_context=%d'%id_context,asDict=True)

    def get_id_for_name(self, ctx_name):
        return self.Select('id_context','context','name=%s'%ctx_name)

    def get_context_attributes(self, context):
        """ Gets a list of ID,attribute_name pairs for a given context
        @param context it can be either an int or an string, contexts can be 
        searched by ID or name.
        """
        if type(context) in [int,long]:
            clause1='context.id_context=%d'%context
        elif type(context) is str:
            clause1="context.name like '%s'"%context
        return self.Select('ast.ID,ast.full_name','ast,list,context',
            [clause1,'context.id_context=list.id_context','list.id_att=ast.ID']
            ,distinct=True)

    def get_number_of_attributes(self, id_context):
        """Returns number of attributes for given context.
        """
        return self.Query('SELECT COUNT(id_att) FROM list WHERE id_context=%d'%id_context)[0][0]

    def get_number_of_snapshots(self, id_context):
        """Returns number of snapshots for given context.
        """
        return self.Query('SELECT COUNT(id_context) FROM snapshot WHERE id_context=%d'%id_context)[0][0]

    def print_context_info(self, filter='%'):
        """Displays information about the contexts.
           List of attributes, list of snapshots, number of attributes, number
           snapshots and date of last taken snapshot fro each context from the
           database
        """
        q = self.db.cursor()
        q.execute("SELECT id_context,name,reason from context where context.name like '%s'"%filter)
        context_ids = q.fetchall()
        context_ids = dict((a[0],(a[1],a[2])) for a in context_ids if a)
        for id_context in context_ids:
            snaps = self.get_context_snapshots(id_context)
            print 'Displaying information about context %d: %s'%(id_context,context_ids[id_context])
            print 'Contexts\' %d attributes:'%id_context
            print self.get_context_attributes(id_context)
            print 'Contexts\' %d snapshots:'%id_context
            print snaps
            print 'Summary:'
            print 'Context number %d'%id_context
            print 'Number of attributes:',self.get_number_of_attributes(id_context)
            print 'Number of snapshots:', self.get_number_of_snapshots(id_context)
            if self.get_number_of_snapshots(id_context) > 0:
                snap_dt = snaps[0][1]
                print snap_dt.strftime('Last snapshot taken on day %F at %T.')
            print ''
        self.info('thats all')
        return True

    def search_attribute(self,att_id=None):
        """ Get a list of registered attributes matching the given ID or wildcard
        @param att_id: if it is an int is used as an unique ID, if it is an string is compared with attribute names using Sql wildcard '%'
        @return 
        """
        if not att_id:
            clause1=''
        elif type(att_id) is int:
            clause1 = 'ast.ID=%d'%att_id
        elif type(att_id) is str:
            clause1 = att_id.replace('*','%')
            clause1 = "ast.full_name like '%s'"%att_id
        else:
            raise Exception("SnapDB_getAttributesList_ArgTypeNotSuported")
        return self.Select('*','ast',clause1,asDict=True)

    def get_attribute_id(self,attribute):
        return self.Select('ID','ast',"full_name like '%s'"%attribute)[0][0]
    
    def get_attributes_ids(self,filters=None):
        """
        This method returns a {full_name:int(ID)} dictionary
        """
        data = self.Select('full_name,ID','ast',"full_name like '%s'"%filters if filters else '')
        return dict((n,int(i)) for n,i in data)

    def get_attribute_name(self,att_id):
        return self.Select('full_name','ast',"ID like '%d'"%att_id)[0][0]

    def get_attribute_contexts(self,attribute):
        return self.Select('context.id_context',['ast','context','list'],
            ["ast.full_name like '%s'"%attribute,'context.id_context=list.id_context','list.id_att=ast.ID'])

    def get_attributes_data(self,attr_id):
        """ For a given ID it retrieves attribute name, type and dimensions from the database
        @param attr_id could be either a single ID or a list of attribute IDs
        @return an {AttID:{full_name,Type,Writable,max_x,max_y}} dict
        """
        if not attr_id: 
            print('get_attributes_data(%s): SnapDB_EmptyArgument!'%attr_id)
            return {}
        if type(attr_id) is not list: attr_id = [attr_id]
        attrs = ','.join(['%d'%a for a in attr_id])
        values = self.Select(['ID','full_name','data_type','data_format','writable','max_dim_x','max_dim_y'],'ast','ID in (%s)'%attrs,asDict=True)
        result = {}
        for data in values:
            result[data['ID']] = {'full_name':data['full_name'],
                'data_format':PyTango.AttrDataFormat.values[data['data_format']],
                'data_type':PyTango.ArgType.values[data['data_type']],
                'writable':PyTango.AttrWriteType.values[data['writable']],
                'max_dim_x':data['max_dim_x'],'max_dim_y':data['max_dim_y']
                }
        return result

    def get_attributes_tables(self,attributes_data):
        """ Returns the list of tables to be read for the given attributes
        @param attributes_data should be a list of dictionaries containing data_type,writable,max_dim_x,max_dim_y keys
        Value tables are:
        {'t_im_1val': ['id_snap', 'id_att', 'dim_x', 'dim_y', 'value'],
        't_im_2val': ['id_snap', 'id_att', 'dim_x', 'dim_y', 'read_value', 'write_value'],
        't_sc_num_1val': ['id_snap', 'id_att', 'value'],
        't_sc_num_2val': ['id_snap', 'id_att', 'read_value', 'write_value'],
        't_sc_str_1val': ['id_snap', 'id_att', 'value'],
        't_sc_str_2val': ['id_snap', 'id_att', 'read_value', 'write_value'],
        't_sp_1val': ['id_snap', 'id_att', 'dim_x', 'value'],
        't_sp_2val': ['id_snap', 'id_att', 'dim_x', 'read_value', 'write_value']}
        """
        result = []
        for attr in attributes_data:
            if attr['data_format'] == PyTango.AttrDataFormat.IMAGE:
                if PyTango.AttrWriteType.values[attr['writable']]==PyTango.AttrWriteType.READ:
                    result.append('t_im_1val')
                else: result.append('t_im_2val')
            elif attr['data_format'] == PyTango.AttrDataFormat.SPECTRUM:
                if PyTango.AttrWriteType.values[attr['writable']]==PyTango.AttrWriteType.READ:
                    result.append('t_sp_1val')
                else: result.append('t_sp_2val')
            elif PyTango.ArgType.values[attr['data_type']]==PyTango.ArgType.DevString:
                if PyTango.AttrWriteType.values[attr['writable']]==PyTango.AttrWriteType.READ:
                    result.append('t_sc_str_1val')
                else: result.append('t_sc_str_2val')
            elif PyTango.AttrWriteType.values[attr['writable']]==PyTango.AttrWriteType.READ:
                result.append('t_sc_num_1val')
            else: result.append('t_sc_num_2val')
        return result

    def find_context_for_attribute(self, att_list):
        """ not ready yet
        """
        self.info('Searching for context(s)..')
        ctable=[]
        count,counter,maxpack = 0,0,10
        while counter<len(att_list):
            query="select distinct id_context from list where "
            part = att_list[count:count+maxpack]
            count = count + maxpack if (len(att_list)>(count+maxpack)) else len(part)
            for i,att in enumerate(part):
                if type(att) is str:
                    try:
                        att_id=self.get_attribute_id(att)
                    except:
                        print(att+' - attribute without snapshot')
                else:
                    att_id=att
                query+="id_att=%d" %att_id
                query=query+" or " if i+1 != count else query+";"
            try:
                q = self.db.cursor()
                q.execute(query)
                data=q.fetchall()
                for id in data:
                    ctable.append(id[0])
            except Exception,e:
                self.error('Exception : %s'%traceback.format_exc())
            counter+=count
        return ctable
    ## @}
    #------------------------------------------------------------------------------------------------

    ## @name Snapshot methods
    # @{
    
    def get_snapshots(self,snap_filter):
        cols = ['id_snap','time','snap_comment']
        if fandango.isNumber(snap_filter):
            clause = 'id_snap=%s' % snap_filter
        elif fandango.isSequence(snap_filter):
            clause = 'id_snap in ( %s )' % ' , '.join(map(str,snap_filter))
        else:
            snap_filter = snap_filter.replace('*','%')
            if '%' not in snap_filter: snap_filter = '%'+snap_filter+'%'
            clause = "snap_comment like '%s'"%snap_filter
            
        #if dates: clause+=" AND time BETWEEN '%s' AND '%s'"%dates
        self.db.commit() ## @remark it forces a db.commit() to be sure that last inserted values are there
        return self.Select(cols,'snapshot',clause,order='time desc') #,limit=limit)
    
    def get_last_snapshot(self):
        try:
            return self.Query('select max(id_snap) from snapshot')[0][0]
        except:
            traceback.print_exc()
            return 0

    def get_snapshot_attributes(self,snapid,tables=[]):
        """ For a given snapid it returns all values found in the database
        @param snapid Snapshot ID
        @param tables If allows to restrict tables to be searched, all tables by default
        @return It returns {AttID:{value,read_value,write_value}}
        """
        values = {}
        tables = tables or [t for t in self.tables if t.startswith('t_')]
        for table in tables:
            data = self.Select(self.tables[table],table,'id_snap=%d'%snapid,asDict=True)
            for d in data:
                values[d['id_att']]=d
        return values

    def get_snapshots_for_attribute(self, attr_id,table=None,start_date=None,stop_date=None):
        """ For given attribute name function retrieves all snapshots
        containing that attribute with corresponding values of it.
        In case of READ attributes function returns all existing pairs of
        snapshot ID and corresponding value of given attribute
        In case of READ-WRITE attributes it returns treesomes :D of 
        snapshot ID and corresponding write and read values of it.
        All results are sorted by time descending.
        """
        try:
            table = table or self.get_attributes_tables(self.get_attributes_data(attr_id).values()[0])[0]
            what = 'value' if '_1val' in table else 'read_value,write_value'
            q = self.db.cursor()
            if start_date and stop_date:
                query = "SELECT snapshot.id_snap,%s from snapshot,%s where "%(what,table)
                query += "%s.id_att=%d and snapshot.id_snap=%s.id_snap and time between '%s' and '%s' ORDER BY snapshot.id_snap DESC"%(table,attr_id,table,fandango.time2str(start_date),fandango.time2str(stop_date))
            else:
                query = 'SELECT id_snap,%s from %s where id_att=%d ORDER BY id_snap DESC'%(what,table,attr_id)
            print(query)
            q.execute(query)
            return q.fetchall()
        except Exception,e:
            self.error('Exception : %s'%traceback.format_exc())
            return False

    def get_diff_between_snapshots(self,snap_id_1, snap_id_2):
        self.info('Comparing contexts...')
        try:
            q = self.db.cursor()
            q.execute('select distinct ast.full_name, t_im_1val.value, "None" as None, temp.value, "None" as None, t_im_1val.value-temp.value, "None" as None from t_im_1val left join t_im_1val as temp on t_im_1val.id_att=temp.id_att left join ast on ast.ID=t_im_1val.id_att where t_im_1val.id_snap=%s && temp.id_snap=%s order by ast.full_name' %(snap_id_1, snap_id_2))
            data1valIM = q.fetchall()
            q.execute('select distinct ast.full_name, t_im_2val.read_value, t_im_2val.write_value, temp.read_value, temp.write_value, t_im_2val.read_value-temp.read_value, t_im_2val.write_value-temp.write_value from t_im_2val left join t_im_2val as temp on t_im_2val.id_att=temp.id_att left join ast on ast.ID=t_im_2val.id_att where t_im_2val.id_snap=%s && temp.id_snap=%s order by ast.full_name' %(snap_id_1, snap_id_2))
            data2valIM = q.fetchall()

            q.execute('select distinct ast.full_name, t_sc_num_1val.value, "None" as None, temp.value, "None" as None, t_sc_num_1val.value-temp.value, "None" as None from t_sc_num_1val left join t_sc_num_1val as temp on t_sc_num_1val.id_att=temp.id_att left join ast on ast.ID=t_sc_num_1val.id_att where t_sc_num_1val.id_snap=%s && temp.id_snap=%s order by ast.full_name' %(snap_id_1, snap_id_2))
            data1valNUM = q.fetchall()
            q.execute('select distinct ast.full_name, t_sc_num_2val.read_value, t_sc_num_2val.write_value, temp.read_value, temp.write_value, t_sc_num_2val.read_value-temp.read_value, t_sc_num_2val.write_value-temp.write_value from t_sc_num_2val left join t_sc_num_2val as temp on t_sc_num_2val.id_att=temp.id_att left join ast on ast.ID=t_sc_num_2val.id_att where t_sc_num_2val.id_snap=%s && temp.id_snap=%s order by ast.full_name' %(snap_id_1, snap_id_2))
            data2valNUM = q.fetchall()

            q.execute('select distinct ast.full_name, t_sc_str_1val.value, "None" as None, temp.value, "None" as None, t_sc_str_1val.value-temp.value, "None" as None from t_sc_str_1val left join t_sc_str_1val as temp on t_sc_str_1val.id_att=temp.id_att left join ast on ast.ID=t_sc_str_1val.id_att where t_sc_str_1val.id_snap=%s && temp.id_snap=%s order by ast.full_name' %(snap_id_1, snap_id_2))
            data1valSTR = q.fetchall()
            q.execute('select distinct ast.full_name, t_sc_str_2val.read_value, t_sc_str_2val.write_value, temp.read_value, temp.write_value, t_sc_str_2val.read_value-temp.read_value, t_sc_str_2val.write_value-temp.write_value from t_sc_str_2val left join t_sc_str_2val as temp on t_sc_str_2val.id_att=temp.id_att left join ast on ast.ID=t_sc_str_2val.id_att where t_sc_str_2val.id_snap=%s && temp.id_snap=%s order by ast.full_name' %(snap_id_1, snap_id_2))
            data2valSTR = q.fetchall()

            q.execute('select distinct ast.full_name, t_sp_1val.value, "None" as None, temp.value, "None" as None, t_sp_1val.value-temp.value, "None" as None from t_sp_1val left join t_sp_1val as temp on t_sp_1val.id_att=temp.id_att left join ast on ast.ID=t_sp_1val.id_att where t_sp_1val.id_snap=%s && temp.id_snap=%s order by ast.full_name' %(snap_id_1, snap_id_2))
            data1valSP = q.fetchall()
            q.execute('select distinct ast.full_name, t_sp_2val.read_value, t_sp_2val.write_value, temp.read_value, temp.write_value, t_sp_2val.read_value-temp.read_value, t_sp_2val.write_value-temp.write_value from t_sp_2val left join t_sp_2val as temp on t_sp_2val.id_att=temp.id_att left join ast on ast.ID=t_sp_2val.id_att where t_sp_2val.id_snap=%s && temp.id_snap=%s order by ast.full_name' %(snap_id_1, snap_id_2))
            data2valSP = q.fetchall()

            data1val = data1valIM+data1valNUM+data1valSTR+data1valSP
            data2val = data2valIM+data2valNUM+data2valSTR+data2valSP
                  
            if not (data1val or data2val):
                raise Exception,'No common attributes !'
            else:
                return(data1val + data2val)
        except Exception,e:
            self.error('Error: %s'%traceback.format_exc())
            return False
        self.info('done')

    ## @}
    #------------------------------------------------------------------------------------------------

    ## @name Remove methods
    # @{

    def remove_context(self, id_context, snap=True):
        """
        @param snap remove also any snapshot in that context (warning dangerous)
        * deletes a context from the Database:
            - Select the id_context.
            - Select all the attributes ids from list table.
            - Get all snapshots ids from snapshot table.
         * deleteSnapshot from the Database:
            - Delete from list where id_context in the list.
            - Delete from context where id_context in the list.
            - Delete from ast if ID in atts_list and not in id_att from list.
        """
        self.info('In SnapDB.remove_context(%s)'%id_context)
        if not self.get_id_contexts(id_context):
            self.error('Unknown Context Id %s'%id_context)
            raise Exception( 'ERROR:UnknownContextId!')
        snap_ids = [t[0] for t in self.get_context_snapshots(id_context)]
        result = False
        ac = self.autocommit
        self.setAutocommit(False)
        try:
            q = self.db.cursor()
            if snap and snap_ids:
                self.info('removing snaps')
                for sid in snap_ids:
                    if not self.remove_snapshot(sid): 
                        raise Exception,'Exception removing %s snapshot!'%sid
            self.info('deleting from list')
            q.execute('DELETE FROM list WHERE id_context=%d'%id_context)
            self.info('deleting from context'    )
            q.execute('DELETE FROM context WHERE id_context=%d'%id_context)

            self.db.commit()
            self.info('Context %d erased from database'%id_context)
            result = True
        except Exception,e:
            traceback.print_exc()
            self.error('Exception in remove_context: %s'%traceback.format_exc())
            self.db.rollback()
        finally:
            self.setAutocommit(ac)
        return result

    def remove_snapshots_of_context(self, id_context):
        """Removes all snapshots for the given context id.
        """
        self.info('In SnapDB.remove_snapshots_of_context(%s)'%id_context)
        if not self.db:
            self.__initMySQLconnection()
        self.db.autocommit(False)
        try:
            q = self.db.cursor()
            self.info('listing snaps')
            q.execute('SELECT id_snap from snapshot where id_context=%d'%id_context)
            snap_ids=q.fetchall()
            snap_ids=[a[0] for a in snap_ids if a]
            for snap in snap_ids:
                if not self.remove_snapshot(snap): 
                    raise Exception,'Exception removing snapshot!'
            self.db.commit()
            self.info('Snapshots for context %d erased from database'%id_context)
            return True
        except Exception,e:
            self.error('Exception in remove_snapshot: %s'%traceback.format_exc())
            self.db.rollback()
            return False

    def remove_empty_contexts(self):
        """ @todo search for contexts with no snapshots and no attributes and 
            remove them.
        """
        raise Exception,'SnapAPI_NotImplemented'

    def remove_snapshot(self,id_snap):
        """
        - Delete from all value tables where snapshot id is in the list.
            - DON'T USE ATTRIBUTE ID TO DELETE, COULD BE USED IN MORE CONTEXTS!
                - It will be verified at the end ...
            - Delete from snapshot where id_snap in the list.
        """
        self.info( 'In DB_removeSnapshot(%s)'%id_snap)
        result,ac = False,self.autocommit
        self.setAutocommit(False)
        try:
            q = self.db.cursor()
            for table in ['snapshot']+[t for t in self.getTables() if t.startswith('t_')]:
                q.execute('DELETE FROM %s WHERE id_snap=%d'%(table,id_snap))
            self.db.commit()
            self.info('Snapshot %d erased from database'%id_snap)
            result = True
        except Exception,e:
            self.error('Exception in DB_removeSnapshot query: %s'%traceback.format_exc())
            self.db.rollback()
        finally:
            self.setAutocommit(ac)
        return result

    def update_context(self, ctx):
        """Modifies an existing context.
           ctx is an SnapContext object
        """
        self.info( 'Modifying context number %d'%ctx.ID)
        global dtas
        result,ac = False,self.autocommit
        self.setAutocommit(False)
        try:
            q = self.db.cursor()
            if isinstance(ctx.time, datetime.date):
              dtas = ctx.time.isoformat()
            else:
              dtas = str(dtas)
            q.execute('UPDATE context SET time=%r, name=%r, author=%r, reason=%r, description=%r WHERE id_context=%s'%(dtas,ctx.name,ctx.author,ctx.reason,ctx.description,ctx.ID))
            self.db.commit()
            result = True
        except Exception,e:
            self.error('Exception while modifying context: %s' % traceback.format_exc())
            self.db.rollback()
        finally:
            self.setAutocommit(ac)
        return result

    def update_context_attributes(self, ctx_id, attribute_list):
        """ Updates the attributes for given context; but only if they already exist in ast table!
            First checks if attributes exists in 'ast' table and if so changes
            the 'list' table by deleting the existing attributes ids for given 
            context and adding new ones provided as arguments
        """
        self.info( 'update_context_attributes(%s,%s): Validating attributes..'%(ctx_id,attribute_list))
        result,ac = False,self.autocommit
        self.setAutocommit(False)
        q = self.db.cursor()
        att_ids=[]
        for a in attribute_list:
            try:
                q.execute('SELECT ID FROM ast WHERE full_name=%r'%a)
                att_id = q.fetchone()[0]
                if att_id not in att_ids:
                    att_ids.append(att_id)  
                    self.info('Attribute \'%s\' confirmed, id: %d'%(a, att_id)) 
            except Exception,e:
                self.error('Given attribute \'%s\' doesn\'t exist. Error: %s'%(a, str(e)))
                return False
        self.info( 'Updating context %s attributes.. '%ctx_id)
        try:
            q.execute('DELETE FROM list WHERE id_context=%d'%ctx_id)
            for att_id in att_ids:
                q.execute( 'INSERT INTO list VALUES (%r, %r)' %(ctx_id,att_id) )
            self.db.commit()
            self.info( 'context updated..' )
            result = True
        except Exception,e:
            self.error('Exception while updating context\'s attributes: %s' % traceback.format_exc())
            self.db.rollback()
        finally:
            self.setAutocommit(ac)
        return result

    def rename_context(self, ctxid, new_name):
        """ Changes the name of the context to new_name.
        """
        new_name = str(new_name)
        ctxid = int(ctxid)
        self.info( 'In DB_rename_context(%s,%s)' % (ctxid, new_name))
        result,ac = False,self.autocommit
        self.setAutocommit(False)
        try:
            q = self.db.cursor()
            q.execute('UPDATE context SET name=%r WHERE id_context=%d' %(new_name,ctxid))
            self.db.commit()
            self.info('changed name of context %d to %s' %(ctxid,new_name))
            result = True
        except Exception,e:
            self.error('Exception in DB_rename_context query: %s' % traceback.format_exc())
            self.db.rollback()
        finally:
            self.setAutocommit(ac)
        return result
    ## @}
    
###############################################################################

def __test__():
    def test_step(n,msg):
        print '-'*80
        print ' '+'Test %s: %s'%(n,msg)
        print '-'*80
        
    try:
        print '\nPyTangoArchiving.Snap.test()\n'+'-'*80
        
        api = SnapAPI()
        if 'get' in str(sys.argv):
            aid = 42
            name = api.db.get_attribute_name(aid)
            api.get_attributes();
            print name,'\n',api.attributes[name]
            print 'contexts\n',[api.contexts[c] for c in api.attributes[name]['contexts']]
            print 'snapshots\n','\n'.join(map(str,api.get_attribute_snapshots(name)))
            #print 'snapshots\n',api.db.get_snapshots(list(api.attributes[name]['snapshots'])[0])
            print api.attributes[name]['snapshots']
            print 'snapshots\n',api.db.get_snapshots((47,46))
            
        if 'create' in str(sys.argv):
            print api.get_contexts()
            api.get_attributes();
            attrs = fandango.get_matching_attributes('sys/tg_test/1/(long|double)*(scalar_rww|spectrum)',fullname=False)
            try:
                test_step(1,'CreateContext')
                ctx = api.create_context('test','test','TEST','test',attrs)
                test_step(2,'Take Snapshots')
                s = ctx.take_snapshot('Trying first')
                s = ctx.take_snapshot('Trying second')
                s = ctx.take_snapshot('Trying third')
                test_step('2b','Modify Snapshot')
                api.modify_snapshot(s.ID,'third to fourth')
                test_step(3,'Report Context')
                api.db.print_context_info(ctx.name)
                test_step(4,'Delete Snap')
                api.db.remove_snapshot(s.ID)
                print ctx.get_snapshots()
            except:
                test_step('ERROR',traceback.print_exc())
            test_step(5,'Delete Context')

        if 'remove' in str(sys.argv):
            ctx = api.get_context(name='test')
            if ctx:
                print ctx
                api.remove_context(ctx.ID)
                api.db.print_context_info(ctx.name)
            else:
                print '"test" context not found'
    except:
        print '!'*80
        print 'PyTangoArchiving.Snap.test() failed!!\n'
        print traceback.format_exc()
    return

if __name__ == '__main__':
    if 'test' in str(sys.argv):
        __test__()
