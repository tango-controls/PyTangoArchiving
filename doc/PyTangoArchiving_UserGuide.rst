========================
PYTANGOARCHIVING RECIPES
========================

by Sergi Rubio

.. contents::

Description
===========

PyTangoArchiving is the python API for Tango Archiving:  https://sourceforge.net/projects/tango-cs/files/tools/ArchivingRoot-15.2.1.zip

This package allows to:

* Integrate Hdb and Snap archiving with other python/PyTango tools.
* Start/Stop Archiving devices in the appropiated order.
* Increase the capabilities of configuration and diagnostic.
* Import/Export .csv and .xml files between the archiving and the database.

Installing PyTangoArchiving
===========================

Repository:
 
  https://github.com/tango-controls/PyTangoArchiving
 
Dependencies
------------
 
* Tango Java Archiving:  https://sourceforge.net/projects/tango-cs/files/tools/ArchivingRoot-15.2.1.zip
* PyTango: https://pypi.python.org/pypi/PyTango
* python-mysql: https://pypi.python.org/pypi/MySQL-python
* Taurus (optional): https://pypi.python.org/pypi/Taurus
* fandango: https://github.com/tango-controls/fandango

Setup
-----
 
Follow Tango Java Archiving installation document to setup Java Archivers and Extractors. 
Some of the most common installation issues are solved in several topics in Tango forums (search for Tdb/Hdb/Snap Archivers)
Install PyTango and MySQL-python using their own setup.py scripts.
fandango, and PyTangoArchiving parent folders must be added to your PYTHONPATH environment variable.
Although Java Extractors may be used, it is recommended to configure direct MySQL access for PyTangoArchiving

Configuring MySQL
-----------------

Although not needed, I recommend you to create a new MySQL user for data querying::

  mysql -u hdbmanager -p hdb

  GRANT USAGE ON hdb.* TO 'browser'@'localhost' IDENTIFIED BY '**********';
  GRANT USAGE ON hdb.* TO 'browser'@'%' IDENTIFIED BY '**********';
  GRANT SELECT ON hdb.* TO 'browser'@'localhost';
  GRANT SELECT ON hdb.* TO 'browser'@'%';

  mysql -u tdbmanager -p tdb

  GRANT USAGE ON tdb.* TO 'browser'@'localhost' IDENTIFIED BY '**********';
  GRANT USAGE ON tdb.* TO 'browser'@'%' IDENTIFIED BY '**********';
  GRANT SELECT ON tdb.* TO 'browser'@'localhost';
  GRANT SELECT ON tdb.* TO 'browser'@'%';

Check in a python shell that your able to access the database::

  import PyTangoArchiving

  PyTangoArchiving.Reader(db='hdb',config='user:password@hostname')
  
Then configure the Hdb/Tdb Extractor class properties to use this user/password for querying::

  import PyTango

  PyTango.Database().put_class_property('HdbExtractor',{'DbConfig':'user:password@hostname'})

  PyTango.Database().put_class_property('TdbExtractor',{'DbConfig':'user:password@hostname'})

You can test now access from a Reader (see recipes below) object or from a taurustrend/ArchivingBrowser UI (Taurus required)::

  python PyTangoArchiving/widget/ArchivingBrowser.py 
 
sub modules
===========

:api: getting servers/devices/instances implied in the archiving system and allowing DistributedArchiving
:archiving: configuration and reading of historic data
:snap: configuration and reading of snapshot data, see ArchivingSnapshots
:xml: conversion between xml and csv files
:scripts: configuration scripts
:reader: providing the useful Reader and ReaderProcess objects to retrieve archived data

General usage
=============

In all these examples you can use hdb or tdb just replacing one by the other

Get archived values for an attribute
------------------------------------

The reader object provides a fast access to archived values::

  import PyTangoArchiving
  rd = PyTangoArchiving.Reader('hdb')
  rd.get_attribute_values('expchan/eh_emet02_ctrl/3/value','2013-03-20 10:00','2013-03-20 11:00')
  Out[11]:
  [(1363770788.0, 5.79643e-14),
  (1363770848.0, 5.72968e-14),
  (1363770908.0, 5.7621e-14),
  (1363770968.0, 6.46782e-14),
  ...

Start/Stop/Check attributes
---------------------------

You must create an Archiving api object and pass to it the list of attributes with its archiving config::

  import PyTangoArchiving
  hdb = PyTangoArchiving.ArchivingAPI('hdb')
  attrs = ['['expchan/eh_emet03_ctrl/3/value','expchan/eh_emet03_ctrl/4/value']

  #Archive every 15 seconds if change> +/-1.0, else every 300 seconds 
  modes = {'MODE_A': [15000.0, 1.0, 1.0], 'MODE_P': [300000.0]} 

  #If you omit the modes argument then archiving will be every 60s
  hdb.start_archiving(attrs,modes) 

  hdb.load_last_values(attrs)
  {'expchan/eh_emet02_ctrl/3/value': [[datetime.datetime(2013, 3, 20, 11, 38, 9),
    7.27081e-14]],
  'expchan/eh_emet02_ctrl/4/value': [[datetime.datetime(2013, 3, 20, 11, 39),
    -3.78655e-08]]
  }

  hdb.stop_archiving(attrs)
  
Loading a .CSV file into Archiving
----------------------------------

The .csv file must have a shape like this one (any row starting with '#' is ignored)::

  Host	Device	Attribute	Type	ArchivingMode	Periode >15	MinRange	MaxRange
                              
  #This header lines are mandatory!!!							
  @LABEL	Unique ID						
  @AUTHOR	Who?						
  @DATE	When?						
  @DESCRIPTION	What?						
                              
  #host	domain/family/member	attribute 	HDB/TDB/STOP	periodic/absolute/relative			
                              
  cdi0404	LI/DI/BPM-ACQ-01	@DEFAULT		periodic	300		
                          ADCChannelAPeak	HDB	absolute	15	1	1
                                      TDB	absolute	5	1	1
                          ADCChannelBPeak	HDB	absolute	15	1	1
                                      TDB	absolute	5	1	1
                          ADCChannelCPeak	HDB	absolute	15	1	1
                                      TDB	absolute	5	1	1
                          ADCChannelDPeak	HDB	absolute	15	1	1
                                      TDB	absolute	5	1	1

The command to insert it is::

  import PyTangoArchiving
  PyTangoArchiving.LoadArchivingConfiguration('/beamlines/bl24/controls/archiving/BL24_EM_fbecheri_20130319.csv','hdb',launch=True)

There are some arguments to modify Loading behavior.

launch::

  if not explicitly True then archiving is not triggered, it just verifies that format of the file is Ok and attributes are available

force::

  if False the loading will stop at first error, if True then it tries all attributes even if some failed

overwrite::

  if False attributes already archived will be skipped.

Checking the status of the archiving
------------------------------------

.. code:: python

  hdb = PyTangoArchiving.ArchivingAPI('hdb')
  hdb.load_last_values()
  filter_ = "/" #Put here whatever you want to filter the attribute names
  lates = [a for a in hdb if filter_ in a and hdb[a].archiver and hdb[a].modes.get('MODE_P') and hdb[a].last_date<(time.time()-(3600+1e-3*hdb[a].modes['MODE_P'][0]))]

  #Get the list of attributes that cannot be read from the control system (ask system responsibles)
  unav = [a for a in lates if not fandango.device.check_attribute(a,timeout=6*3600)]
  #Get the list of attributes that are not being archived
  lates = sorted(l for l in lates if l not in unav)
  #Get the list of archivers not running properly
  bad_archs = [a for a,v in hdb.check_archivers().items() if not v]

  #Restarting the archivers/attributes that failed
  bads = [l for l in lates if hdb[l] not in bad_archs]
  astor = fandango.Astor()
  astor.load_from_devs_list(bad_archs)
  astor.restart_servers()
  hdb.restart_archiving(bads)
  Restart of the whole archiving system
  admin@archiving:> archiving_service.py stop-all
  ...
  admin@archiving:> archiving_service.py start-all
  ...
  admin@archiving:> archiving_service.py status

  #see archiving_service.py help for other usages
  
Start/Stop of an small (<10) list of attributes
-----------------------------------------------

.. code:: python 

  #Stopping ...
  api.stop_archiving(['bo/va/dac/input','bo/va/dac/settings'])

  #Starting with periodic=60s ; relative=15s if +/-1% change
  api.start_archiving(['bo/va/dac/input','bo/va/dac/settings'],{'MODE_P':[60000],'MODE_R':[15000,1,1]})

  #Restarting and keeping actual configuration

  attr_name = 'bo/va/dac/input'
  api.start_archiving([attr_name],api.attributes[attr_name].extractModeString())
  Checking if a list of attributes is archived
  hdb = PyTangoArchiving.api('hdb')

  sorted([(a,hdb.load_last_values(a)) for a in hdb if a.startswith('bl04')])

  Out[17]: 
  [('bl/va/elotech-01/output_1',
    [[datetime.datetime(2010, 7, 2, 15, 53), 6.0]]),
  ('bl/va/elotech-01/output_2',
    [[datetime.datetime(2010, 7, 2, 15, 53, 11), 0.0]]),
  ('bl/va/elotech-01/output_3',
    [[datetime.datetime(2010, 7, 2, 15, 53, 23), 14.0]]),
  ('bl/va/elotech-01/output_4',
    [[datetime.datetime(2010, 7, 2, 15, 52, 40), 20.0]]),
  ...
  
Getting information about attributes archived
---------------------------------------------

.. code:: python

  import PyTangoArchiving
  api = PyTangoArchiving.ArchivingAPI('hdb')
  len(api.attributes) #All the attributes in history
  len([a for a in api.attributes.values() if a.archiving_mode]) #Attributes configured

Getting the configuration of attribute(s)
-----------------------------------------

.. code:: python

  #Getting as string
  modes = api.attributes['rs/da/bpm-07/CompensateTune'].archiving_mode 

  #Getting it as a dict
  api.attributes['sr/da/bpm-07/CompensateTune'].extractModeString()

  #OR
  PyTangoArchiving.utils.modes_to_dict(modes)
  
Getting the list of attributes not updated in the last hour
-----------------------------------------------------------

.. code:: python

  failed = sorted(api.get_attribute_failed(3600).keys())
  Getting values for an attribute
  import PyTangoArchiving,time

  reader = PyTangoArchiving.Reader() #An HDB Reader object using HdbExtractors
  #OR
  reader = PyTangoArchiving.Reader(db='hdb',config='pim:pam@pum') #An HDB reader accessing to MySQL

  attr = 'bo04/va/ipct-05/state'
  dates = time.time()-5*24*3600,time.time() #5days
  values = reader.get_attribute_values(attr,*dates) #it returns a list of (epoch,value) tuples
  Exporting values from a list of attributes as a text (csv / ascii) file
  from PyTangoArchiving import Reader
  rd = Reader(db='hdb') #If HdbExtractor.DbConfig property is set one argument is enough
  attrs = [
          'bl11-ncd/vc/eps-plc-01/pt100_1',
          'bl11-ncd/vc/eps-plc-01/pt100_2',
          ]

  #If you ignore text argument you will get lists of values, if text=True then you get a tabulated file.
  ascii_values = rd.get_attributes_values(attrs,
                        start_date='2010-10-22',stop_date='2010-10-23',
                        correlate=True,text=True)

  print ascii_values

  #Save it as .csv if you want ...
  open('myfile.csv','w').write(ascii_values)
  
Filtering State changes for a device
------------------------------------
  
.. code:: python
  
  import PyTangoArchiving as pta
  rd = pta.Reader('hdb','...:...@...')
  vals = rd.get_attribute_values('bo02/va/ipct-02/state','2010-05-01 00:00:00','2010-07-13 00:00:00')
  bads = []
  for i,v in enumerate(vals[1:]):
      if v[1]!=vals[i-1][1]:
          bads.append((v[0],vals[i-1][1],v[1]))
  report = [(time.ctime(v[0]),str(PyTango.DevState.values[int(v[1])] if v[1] is not None else 'None'),str(PyTango.DevState.values[int(v[2])] if v[2] is not None else 'None')) for v in bads]

  report = 
  [('Sat May  1 00:07:03 2010', 'UNKNOWN', 'ON'),
  ...
  
Getting a table with last values for all attributes of a same device
--------------------------------------------------------------------

.. code:: python

  HOURS = 1
  DEVICE = 'BO/VA/IPCT-05'
  ATTRS = [A FOR A IN READER.GET_ATTRIBUTES() IF A.LOWER().STARTSWITH(DEVICE)]
  VARS = DICT([(ATTR,READER.GET_ATTRIBUTE_VALUES(ATTR,TIME.TIME()-HOURS*3600)) FOR ATTR IN ATTRS])
  TABLE = [[TIME.CTIME(T0)]+
          [([V FOR T,V IN VAR IF T<=T0] OR [NONE])[-1] FOR ATTR,VAR IN SORTED(VARS.ITEMS())] 
          FOR T0,V0 IN VARS.VALUES()[0]]
  PRINT('\N'.JOIN(
        ['\T'.JOIN(['DATE','TIME']+[K.LOWER().REPLACE(DEVICE,'') FOR K IN SORTED(VARS.KEYS())])]+
        ['\T'.JOIN([STR(S) FOR S IN T]) FOR T IN TABLE]))
      
Using CSV files
===============

Loading an HDB/TDB configuration file
-------------------------------------

Create dedicated archivers first

If you want to use this option it will require some RAM resources in the host machine (64MbRAM/250Attributes) and installing the ALBA-Archiving bliss package.

.. code:: python

  from PyTangoArchiving.files import DedicateArchiversFromConfiguration
  DedicateArchiversFromConfiguration('LX_I_Archiving.csv','hdb',launch=True)
  TDB Archiving works different as it shouldn't be working on diskless machines, using instead a centralized host for all archiver devices.

  DedicateArchiversFromConfiguration('LX_I_Archiving.csv','tdb',centralized='archiving01',launch=True)
  
Loading the .csv files
----------------------

All the needed code to do it is:

.. code:: python

  import PyTangoArchiving

  #With launch=False this function will do a full check of the attributes and print the results
  PyTangoArchiving.LoadArchivingConfiguration('/data/Archiving//LX_I_Archiving_.csv','hdb',launch=False)

  #With launch=True configuration will be recorded and archiving started
  PyTangoArchiving.LoadArchivingConfiguration('/data/Archiving//LX_I_Archiving_.csv','hdb',launch=True)

  #To force archiving of all not-failed attributes
  PyTangoArchiving.LoadArchivingConfiguration('/data/Archiving//LX_I_Archiving_.csv','hdb',launch=True,force=True)

  #Starting archiving in TDB mode (kept 5 days only)
  PyTangoArchiving.LoadArchivingConfiguration('/data/Archiving//LX_I_Archiving_.csv','tdb',launch=True,force=True)
  
You must take in account the following conditions:

* Names of attributes must match the NAME, not the LABEL! (that's a common mistake)
* Devices providing the attributes must be running when you setup archiving.
* Regular expressions are NOT ALLOWED (I know previous releases allowed it, but never worked really well)

filtering a list of CSV configurations / attributes to load
-----------------------------------------------------------

You can use GetConfigFiles and filters/exclude to select a predefined list of attributes

.. code:: python

  import PyTangoArchiving as pta

  filters = {'name':".*"}
  exclude = {'name':"(s.*bpm.*)|(s10.*rf.*)|(s14.*rf.*)"}

  #TDB
  confs = pta.GetConfigFiles(mask='.*(RF|VC).*')
  for target in confs:
      pta.LoadArchivingConfiguration(target,launch=True,force=True,overwrite=True,dedicated=False,schema='tdb',filters=filters,exclude=exclude)

  #HDB
  confs = pta.GetConfigFiles(mask='.*BO.*(RF|VC).*')
  for target in confs:
      pta.LoadArchivingConfiguration(target,launch=True,force=True,overwrite=True,dedicated=True,schema='hdb',filters=filters,exclude=exclude)

Comparing a CSV file with the actual configuration
--------------------------------------------------

.. code:: python

import PyTangoArchiving
api = PyTangoArchiving.ArchivingAPI('hdb')
config = PyTangoArchiving.ParseCSV('Archiving_RF_.csv')

for attr,conf in config.items():
    if attr not in api.attributes or not api.attributes[attr].archiving_mode:
        print '%s not archived!' % attr
    elif PyTangoArchiving.utils.modes_to_string(api.check_modes(conf['modes']))!=api.attributes[attr].archiving_mode:
        print '%s: %s != %s' %(attr,PyTangoArchiving.utils.modes_to_string(api.check_modes(conf['modes'])),api.attributes[attr].archiving_mode)

Checking and restarting a known system from a .csv
--------------------------------------------------

.. code:: python

  import PyTangoArchiving.files as ptaf
  borf = '/data/Archiving/BO_20100603_v2.csv'
  config = ptaf.ParseCSV(borf)
  import PyTangoArchiving.utils as ptau
  hdb = PyTangoArchiving.ArchivingAPI('hdb')

  missing = [
  'bo/ra/fim-01/remotealarm',
  'bo/ra/fim-01/rfdet1',
  'bo/ra/fim-01/rfdet2',
  'bo/ra/fim-01/arcdet5',
  'bo/ra/fim-01/rfdet3',
  'bo/ra/fim-01/arcdet3',
  'bo/ra/fim-01/arcdet2',
  'bo/ra/fim-01/vacuum']

  ptau.check_attribute('bo/ra/fim-01/remotealarm')
  missing = 'bo/ra/fim-01/arcdet4|bo/ra/fim-01/remotealarm|bo/ra/fim-01/rfdet1|bo/ra/fim-01/rfdet2|bo/ra/fim-01/arcdet5|bo/ra/fim-01/rfdet3|bo/ra/fim-01/arcdet3|bo/ra/fim-01/arcdet2|bo/ra/fim-01/vacuum'

  ptaf.LoadArchivingConfiguration(borf,filters={'name':missing},launch=True)
  ptaf.LoadArchivingConfiguration(borf,filters={'name':'bo/ra/eps-plc.*'},stop=True,force=True)
  ptaf.LoadArchivingConfiguration(borf,filters={'name':'bo/ra/eps-plc.*'},launch=True,force=True)

  rfplc = ptaf.ParseCSV(borf,filters={'name':'bo/ra/eps-.*'})
  stats = ptaf.CheckArchivingConfiguration(borf,period=300)
