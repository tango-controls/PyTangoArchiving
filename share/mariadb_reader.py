#!/usr/bin/env python3

import sys, re, traceback
from timeutils import *
import AbstractReader

try:
    import pymysql as mariadb
except:
    import MySQLdb as mariadb
    

    
class MariadbReader(AbstractReader.AbstractReader):
    """
    read-only API for hdb++ databases, based on PyTangoArchiving AbstractReader
    """
    
    def __init__(self,config='',**kwargs):
        """
        Arguments accepted by pymysql connections:

        :param host: Host where the database server is located
        :param user: Username to log in as
        :param password: Password to use.
        :param database: Database to use, None to not use a particular one.
        :param port: MySQL port to use, default is usually OK. (default: 3306)
        :param bind_address: When the client has multiple network interfaces, specify
            the interface from which to connect to the host. Argument can be
            a hostname or an IP address.
        :param unix_socket: Optionally, you can use a unix socket rather than TCP/IP.
        :param read_timeout: The timeout for reading from the connection in seconds (default: None - no timeout)
        :param write_timeout: The timeout for writing to the connection in seconds (default: None - no timeout)
        :param charset: Charset you want to use.
        :param sql_mode: Default SQL_MODE to use.
        :param read_default_file:
            Specifies  my.cnf file to read these parameters from under the [client] section.
        :param conv:
            Conversion dictionary to use instead of the default one.
            This is used to provide custom marshalling and unmarshaling of types.
            See converters.
        :param use_unicode:
            Whether or not to default to unicode strings.
            This option defaults to true for Py3k.
        :param client_flag: Custom flags to send to MySQL. Find potential values in constants.CLIENT.
        :param cursorclass: Custom cursor class to use.
        :param init_command: Initial SQL statement to run when connection is established.
        :param connect_timeout: Timeout before throwing an exception when connecting.
            (default: 10, min: 1, max: 31536000)
        :param ssl:
            A dict of arguments similar to mysql_ssl_set()'s parameters.
        :param read_default_group: Group to read from in the configuration file.
        :param compress: Not supported
        :param named_pipe: Not supported
        :param autocommit: Autocommit mode. None means use server default. (default: False)
        :param local_infile: Boolean to enable the use of LOAD DATA LOCAL command. (default: False)
        :param max_allowed_packet: Max size of packet sent to server in bytes. (default: 16MB)
            Only used to limit size of "LOAD LOCAL INFILE" data packet smaller than default (16KB).
        :param defer_connect: Don't explicitly connect on contruction - wait for connect call.
            (default: False)
        :param auth_plugin_map: A dict of plugin names to a class that processes that plugin.
            The class will take the Connection object as the argument to the constructor.
            The class needs an authenticate method taking an authentication packet as
            an argument.  For the dialog plugin, a prompt(echo, prompt) method can be used
            (if no authenticate method) for returning a string from the user. (experimental)
        :param server_public_key: SHA256 authenticaiton plugin public key value. (default: None)
        :param db: Alias for database. (for compatibility to MySQLdb)
        :param passwd: Alias for password. (for compatibility to MySQLdb)
        :param binary_prefix: Add _binary prefix on bytes and bytearray. (default: False)
        """
        if config and isinstance(config,(str,bytes)):
            config = self.parse_config(config)

            
        self.config = config or {}
        self.config.update(kwargs)
            
        self.database = self.config.get('database','hdbpp')
        self.user = self.config.get('user','')
        self.password = self.config.get('password','')
        self.port = int(self.config.get('port','3306'))
        self.host = self.config.get('host','localhost')
        
        #print([(k,v) for k,v in self.__dict__.items() 
              #if k not in type(self).__dict__()])
        
        self.db = mariadb.connect(database=self.database,
            user=self.user, password=self.password, port=self.port, 
            host=self.host)
        self._cursor = self.db.cursor()
        
    def __del__(self):
        self._cursor.close()
        self.db.close()
        
    def _query(self,query,prune=False):
        """
        query: SQL code
        """
        #print(query)
        self._cursor.execute(query)
        if prune:
            r,l = [],True
            while l:
                try:
                    l = self._cursor.fetchone()
                    if l and (not r or l[1:] != r[-1][1:]):
                        r.append(l)
                except:
                    print(r[-1:], l)
                    traceback.print_exc()
                    break
            return r
        else:
            return self._cursor.fetchall()
    
    def parse_config(self,config):
        """
        config string as user:password@host:port/database
        or dictionary like
        """
        try:                
            if re.match('.*[:].*[@].*',config):
                h = config.split('@')
                u,p = h[0].split(':')
                config = {'user':u,'password':p}
                if '/' in h[1]:
                    config['host'],config['database'] = h[1].split('/')
                else:
                    config['host'] = h[1]
                if ':' in config['host']:
                    config['host'],config['port'] = config['host'].split(':')
            else:
                if '{' not in config:
                    config = '{%s}' % config.replace(';',',')
                config = dict(eval(config))
        except:
            raise Exception('Wrong format in config, should be dict-like')
        return config        

    def get_attributes(self, active=False, pattern=''):
        """
        Queries the database for the current list of archived attributes.
        arguments:
            active: True: only attributes currently archived
                    False: all attributes, even the one not archiving anymore
            regexp: '' :filter for attributes to retrieve
        """
        q = 'select att_name from att_conf'
        if pattern:
            q += " where att_name like '%s'" % pattern.replace('*','%')
        print(q)
        return [str(a[0]).lower() for a in self._query(q) if a]
    
    def get_attribute_name(self,attribute):
        attribute = str(attribute).lower()
        if ':' not in attribute:
            attribute = '%' + '/' + attribute

        elif '.' not in attribute:
                attribute = attribute.rsplit(':',1)
                attribute = attribute[0] + '.%' + attribute[1]
            
        if 'tango' not in attribute:
            attribute = '%' + '/' + attribute
            
        attrs = self.get_attributes(pattern=attribute)
        if len(attrs)!=1:
            raise Exception('MultipleAttributeMatches')
        
        return attrs[0] if attrs else ''

    def is_attribute_archived(self, attribute, active=False):
        """
        Returns if an attribute has values in DB.

        arguments:
            attribute: fqdn for the attribute.
            active: if true, only check for active attributes,
                    otherwise check all.
        """
        return bool(self.get_attribute_name(attribute))
    
    def get_attribute_id_table(self, attribute=''):
        """
        for each matching attribute returns name, ID and table name
        """
        q = "select att_name,att_conf_id,data_type "
        q += " from att_conf, att_conf_data_type where "
        q += "att_conf.att_conf_data_type_id = att_conf_data_type.att_conf_data_type_id"
        if attribute:
            q += " and att_name like '%s'" % attribute
            
        return [(a,i,'att_'+t) for (a,i,t) in self._query(q)]    

    def get_last_attributes_values(self, attributes, columns = '', n = 1):
        """
        Returns last values inserted in DB for a list of attributes

        arguments:
            attribute: fqdn for the attribute.
            columns: requested columns separated by commas
        returns:
            {'att1':(epoch, r_value, w_value, quality, error_desc),
             'att2':(epoch, r_value, w_value, quality, error_desc),
             ...
            }
        """
        data = {}
        columns = columns or 'data_time, value_r, quality, att_error_desc_id'
        
        for a in attributes:
            try:
                a,i,t = self.get_attribute_id_table(a)[0]
                tdesc = str(self._query('describe %s'%t))
                tcol = ('int_time' if 'int_time' in tdesc else 'data_time')
                cols = ','.join(c for c in columns.split(',') 
                                if c.strip() in tdesc)
                data[a] = self._query('select %s from %s where '
                    'att_conf_id = %s order by %s desc limit %s'
                    % (cols, t, i, tcol, n))
            except:
                raise Exception('AttributeNotFound: %s' % a) 

        return data

    def get_attributes_values(self, attributes,
            start_date, stop_date=None,
            decimate=None,
            correlate = False,
            columns = '',
            **params):
        """
        Returns attributes values between start and stop dates
        , using decimation or not, correlating the values or not.

        arguments:
            attributes: a list of the attributes' fqdn
            start_date: datetime, beginning of the period to query.
            stop_date: datetime, end of the period to query.
                       if None, now() is used.
            decimate: aggregation function to use in the form:
                      {'timedelta0':(MIN, MAX, …)
                      , 'timedelta1':(AVG, COUNT, …)
                      , …}
                      if None, returns raw data.
            correlate: if True, data is generated so that
                       there is available data for each timestamp of
                       each attribute.
            columns: columns separated by commas
                    time, r_value, w_value, quality, error_desc

        returns:
            {'attr0':[(epoch0, r_value, w_value, quality, error_desc),
            (epoch1, r_value, w_value, quality, error_desc),
            ... ],
            'attr1':[(…),(…)]}
        """
        data = {}
        columns = columns or 'data_time, value_r, quality, att_error_desc_id'
        if isinstance(start_date,(int,float)):
            start_date = time2str(start_date) 
        if stop_date is None:
            stop_date = now()
        if isinstance(stop_date,(int,float)):    
            stop_date = time2str(stop_date)
        
        for a in attributes:
            try:
                a,i,t = self.get_attribute_id_table(a)[0]
                tdesc = str(self._query('describe %s'%t))
                tcol = ('int_time' if 'int_time' in tdesc else 'data_time')
                if tcol == 'int_time':
                    b,e = str2time(start_date),str2time(stop_date)
                else:
                    b,e = "'%s'" % start_date, "'%s'" % stop_date
                    
                cols = ','.join(c for c in columns.split(',') 
                                if c.strip() in tdesc)
                print(cols)
                if 'data_time,' in cols:
                    cols = cols.replace('data_time,',
                                 'CAST(UNIX_TIMESTAMP(data_time) AS DOUBLE),')
                data[a] = self._query('select %s from %s where '
                    'att_conf_id = %s and %s between %s and %s '
                    'order by data_time'
                    % (cols, t, i, tcol, b, e), prune=decimate)
            except:
                import traceback
                traceback.print_exc()
                #raise Exception('AttributeNotFound: %s' % a) 

        return data        
        
        return {'attr0': [(time.time(), 0., 0., 0, '')]
                , 'attr1': [(time.time(), 0., 0., 0, '')]}
    
    
##############################################################################
           
if __name__ == '__main__':
    AbstractReader.main(apiclass=MariadbReader,timeformatter=time2str)
    
