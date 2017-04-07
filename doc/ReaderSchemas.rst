Declaring Schemas for PyTangoArchiving.Reader
=============================================

.. contents::

Just one schema, using DbConfig
-------------------------------

You should setup your PyTangoArchiving.DbConfig property

In Jive:

Edit => Create Free Property => PyTangoArchiving

Tabs => Property => New Property => DbConfig = user:password@host/db_name

It should be enough, to test it do:


import PyTangoArchiving as pta
rd = pta.Reader()
print(rd.get_attributes())


Your archived attributes should appear in the list, then just read the values from
the API or the GUI:

rd.get_attribute_values('lmc/c01/fecb/…',-600)

Using multiple schemas
----------------------


The Tango free property PyTangoArchiving.Schemas is used to declare new schemas.

The property will contain a list of valid schemas, each of them will be specified using its own PyTangoArchiving.$SCHEMA property.

Example
.......

A typical declaration will be:

.. code::

  PyTangoArchiving.Schemas
  
    hdb
    tdb
    
  PyTangoArchiving.hdb
  
    {default values, not declared}
    
  PyTangoArchiving.tdb
  
    schema=tdb
    check=start > now-reader.RetentionPeriod
    reader=PyTangoArchiving.Reader('tdb')
    
Parameters
----------
    
reader
  will contain the default object to be used as data reader. 
  The methods is_attribute_archived and get_attribute(s)_values will be 
  used by default if implemented.

method
  method to be called as equivalent to get_attribute_values

user/host/password
  database access parameters

check
  a python formula to be evaluated, if True the schema will be enabled for the given arguments.
  Allowed parameters are: reader, start, end, attribute, now.

schema/dbname
  database to be queried (not needed if equivalent to schema name)


    
