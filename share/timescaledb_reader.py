#!/usr/bin/env python3

import sys, re, traceback
import AbstractReader
import os
import time
import psycopg2
from psycopg2 import sql

import logging
import logging.handlers
    
from datetime import timedelta

    

    
class TimescaledbReader(AbstractReader.AbstractReader):
    """
    read-only API for hdb++ databases, based on PyTangoArchiving AbstractReader
    """
    
    def __init__(self,config='',**kwargs):
        """
        """
        self.logger = logging.getLogger('TimescaledbReader')

        if config and isinstance(config,(str,bytes)):
            config = self.parse_config(config)

            
        self.config = config or {}
        self.config.update(kwargs)
            
        self.database = self.config.get('database','hdbpp')
        self.user = self.config.get('user','')
        self.password = self.config.get('password','')
        self.port = self.config.get('port', self._get_default_port())
        self.host = self.config.get('host','localhost')
        
        try:
            self.logger.debug("Attempting to connect to server: {}".format(self.host))
               
            # attempt to connect to the server
            self.db = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database)
                                     
            self.db.autocommit = True
                                        
            self.logger.debug("Connected to database at server: {}".format(self.host))
                                                 
            self._cursor = self.db.cursor()
        except (Exception, psycopg2.Error) as error:
            self.logger.error("Error: {}".format(error))

        
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
            rows, next_row = [],True
            while next_row:
                try:
                    next_row = self._cursor.fetchone()
                    if next_row and (not next_row or rows[1:] != rows[-1][1:]):
                        rows.append(next_row)
                except:
                    self.logger.error(rows[-1:], row)
                    traceback.print_exc()
                    break
            return rows
        else:
            return self._cursor.fetchall()
    
    
    def _get_default_port(self):
        """
        Get the default port to connect to the database
        """
        return 5001

    
    def parse_config(self,config):
        """
        config string as user:password@host:port/database
        or dictionary like
        """
        try:                
            if re.match('.*[:].*[@].*',config):
                h = config.split('@')
                user, password = h[0].split(':')
                config = {'user':user,'password':password}
                if '/' in h[1]:
                    config['host'], config['database'] = h[1].split('/')
                else:
                    config['host'] = h[1]
                if ':' in config['host']:
                    config['host'], config['port'] = config['host'].split(':')
            else:
                if '{' not in config:
                    config = '{%s}' % config.replace(';',',')
                config = dict(eval(config))
        except:
            raise Exception('Wrong format in config, should be dict-like')

        if 'port' in config:
            config['port'] = int(config['port'])

        return config        

    
    def get_attributes(self, active=False, pattern=''):
        """
        Queries the database for the current list of archived attributes.
        arguments:
            active: True: only attributes currently archived
                    False: all attributes, even the one not archiving anymore
            regexp: '' :filter for attributes to retrieve
        """
        query = 'SELECT att_name FROM att_conf'
        
        if pattern:
            query += " WHERE att_name LIKE '{}'".format(pattern.replace('*','%'))
        
        self.logger.debug(query)
        
        return [str(attribute[0]).lower() for attribute in self._query(query)]
    
    
    def is_attribute_archived(self, attribute, active=False):
        """
        Returns true if an attribute has values in DB.

        arguments:
            attribute: fqdn for the attribute.
            active: if true, only check for active attributes,
                    otherwise check all.
        """
        self._cursor.execute("SELECT att_conf_id FROM att_conf WHERE att_name LIKE %s AND hide=%s;", ('%'+attribute+'%', active))
        
        att_id = self._cursor.fetchall()

        # if we get more than one attribute an error occured.
        if len(att_id) > 1:
            self.logger.debug("Fetched more than 1 attribute with this name {}".format(attribute))

        return len(att_id)
    
    def get_last_attributes_values(self, attributes, columns = [], n = 1):
        """
        Returns last values inserted in DB for a list of attributes

        arguments:
            attribute: fqdn for the attribute.
            columns: list of requested columns
        returns:
            {'att1':(epoch, r_value, w_value, quality, error_desc),
             'att2':(epoch, r_value, w_value, quality, error_desc),
             ...
            }
        """
        data = {}
        columns = columns or ["value_r", "value_w", "quality", "att_error_desc_id"]
        
        for attr in attributes:
            try:
                # get the att_conf_id and the table where the data is from hdb
                self._cursor.execute("SELECT att_conf_id, table_name, att_name FROM att_conf WHERE att_name LIKE %s;", ('%'+attr+'%',))

                att_id = self._cursor.fetchall()

                if len(att_id) is 0:
                    self.logger.debug("Attribute {} not found".format(attr))
                    raise Exception("Attribute {} not found.".format(attr))

                elif (len(att_id) > 1):
                    self.logger.debug("Fetched more than 1 attribute with this name {}".format(attr))
                
                else:
                    self._cursor.execute(
                        sql.SQL("SELECT data_time, {fields} FROM {table} WHERE att_conf_id=%s ORDER BY data_time DESC LIMIT %s").format(
                            fields = sql.SQL(',').join([sql.Identifier(field) for field in columns]),
                            table = sql.Identifier(att_id[0][1]))
                        , (att_id[0][0], n))
                    
                    if n is 1:
                        data[attr] = self._cursor.fetchall()[0]
                    else:
                        data[attr] = self._cursor.fetchall()
            
            except (Exception, psycopg2.Error) as error:
                self.logger.error("Error extracting data for attribute: {}: {}".format(attr, error))
                raise error

        return data

    def get_attributes_values(self, attributes,
            start_date, stop_date=None,
            decimate=None,
            correlate = False,
            columns = [],
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
            columns: list of columns
                    [time, r_value, w_value, quality, error_desc]

        returns:
            {'attr0':[(epoch0, r_value, w_value, quality, error_desc),
            (epoch1, r_value, w_value, quality, error_desc),
            ... ],
            'attr1':[(…),(…)]}
        """
        data = {}
        columns = columns or ["value_r", "value_w", "quality", "att_error_desc_id"]
        
        for attr in attributes:
            try:
                # get the att_conf_id and the table where the data is from hdb
                self._cursor.execute("SELECT att_conf_id, table_name, att_name FROM att_conf WHERE att_name LIKE %s;", ('%'+attr+'%',))

                att_id = self._cursor.fetchall()

                if (len(att_id) > 1):
                    self.logger.debug("Fetched more than 1 attribute with this name {}".format(attr))
                else:
                    if stop_date is None:
                        stop_date = datetime.datetime.now()

                    # extract data.
                    self.logger.debug("Extracting data for attribute: {} in table: {}".format(att_id[0][2], att_id[0][1]))

                    self._cursor.execute(
                        sql.SQL("SELECT data_time, {fields} FROM {table} WHERE att_conf_id=%s AND data_time BETWEEN %s AND %s ORDER BY data_time DESC").format(
                            fields = sql.SQL(',').join([sql.Identifier(field) for field in columns]),
                            table = sql.Identifier(att_id[0][1]))
                        , (att_id[0][0], start_date, stop_date))
                    
                    data[attr] = self._cursor.fetchall()

            except (Exception, psycopg2.Error) as error:
                self.logger.error("Error extracting data for attribute: {}: {}".format(attr, error))

        return data        
        
    
##############################################################################
           
if __name__ == '__main__':
    AbstractReader.main(apiclass=TimescaledbReader)
    
