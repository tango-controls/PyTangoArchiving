#!/usr/bin/env python

import sys,fandango
import PyTangoArchiving as pta

schemas = ('hdb','tdb')
params = [a for a in sys.argv[1:] if a.startswith('-')]
args = [a for a in sys.argv[1:] if a not in params]
filenames = [a for a in args if a not in schemas]
schemas = ([a for a in args if a in schemas] or schemas)

for filename,schema in fandango.product(filenames,schemas):

  if '-check' in str(params):
      csv = pta.files.ParseCSV(filename,schema)
      api = pta.api(schema)
      archived = api.get_archived_attributes()
      print '%s contains %d attributes, %d should be added to archiving'%(filename,len(csv),len([a for a in csv if a not in archived and pta.utils.check_attribute(a) is not None]))

  if '-dedicate' in str(params):
      pta.files.DedicateArchiversFromConfiguration(filename, schema, launch=True, restart=True)
  
  if '-test' in str(params):
      pta.files.LoadArchivingConfiguration(filename, schema,launch=False,force=False)

  if '-load' in str(params):
      pta.files.LoadArchivingConfiguration(filename, schema,launch=True,force=True,overwrite=False)

  if '-overwrite' in str(params):
      pta.files.LoadArchivingConfiguration(filename, schema,launch=True,force=True,overwrite=True)
