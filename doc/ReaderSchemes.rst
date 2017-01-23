Declaring Schemas for PyTangoArchiving.Reader
=============================================

The Tango free property PyTangoArchiving.Schemas is used to declare new schemas.

The property will contain a list of valid schemas, each of them will be specified using its own PyTangoArchiving.$SCHEMA property.

Example
-------

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
  It should implement is_attribute_archived and get_attribute(s)_values methods.

method
  method to be called as equivalent to get_attribute_values

user/host/password
  database access parameters

check
  a python formula to be evaluated, if True the schema will be enabled for the given arguments.
  Allowed parameters are: reader, start, end, attribute, now.

schema/dbname
  database to be queried (not needed if equivalent to schema name)


    
