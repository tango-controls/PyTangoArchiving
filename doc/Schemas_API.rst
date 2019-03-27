========================
PyTangoArchiving.Schemas
========================

Declaring Schemas in the database
=================================

Just one schema, using DbConfig
-------------------------------

You should setup your PyTangoArchiving.DbConfig property

In Jive::

  Edit => Create Free Property => PyTangoArchiving

  Tabs => Property => New Property => DbConfig = user:password@host/db_name

It should be enough, to test it do::

  import PyTangoArchiving as pta
  rd = pta.Reader()
  print(rd.get_attributes())

Your archived attributes should appear in the list, then just read the values from
the API or the GUI::

  rd.get_attribute_values('your/tango/device/attribute',-600)

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


Accessing schemas from ipython
==============================

Accessing Schemas singletone
----------------------------

code ::

  import PyTangoArchiving as pta
  schemas = pta.Schemas.load()

Accessing schemas from Reader
-----------------------------

code ::

  import PyTangoArchiving as pta
  rd = pta.Reader()
  devs = 'test/acc/ps-clic-01','test/acc/ps-clic-02'
  attrs = [a for a in rd.get_attributes() for d in devs if a.startswith(d+'/')]

  import fandango as fn
  fn.kmap(rd.is_attribute_archived,attrs)
  
    [('test/acc/ps-clic-01/current', ('hdbpp',)),
     ('test/acc/ps-clic-01/polarity', ('hdbpp',)),
     ('test/acc/ps-clic-01/state', ('hdbpp',)),
     ('test/acc/ps-clic-01/voltage', ('hdbpp',)),
     ('test/acc/ps-clic-02/current', ('hdbpp',)),
     ('test/acc/ps-clic-02/polarity', ('hdbpp',)),
     ('test/acc/ps-clic-02/state', ('hdbpp',)),
     ('test/acc/ps-clic-02/voltage', ('hdbpp',))
    ]

  s0 = fn.now()-90*86400

  vals = rd.get_attributes_values(attrs,s0)
  [(k,len(v)) for k,v in vals.items()]
  
    [('test/acc/ps-clic-02/voltage', 46610),
     ('test/acc/ps-clic-02/state', 87),
     ('test/acc/ps-clic-01/state', 754),
     ('test/acc/ps-clic-01/polarity', 14105),
     ('test/acc/ps-clic-02/current', 48849),
     ('test/acc/ps-clic-01/current', 49299),
     ('test/acc/ps-clic-02/polarity', 14136),
     ('test/acc/ps-clic-01/voltage', 45451)
    ]
