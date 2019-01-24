#!/usr/bin/python

ArchivingHealth={
	'NOT_READY':-1,
	'ALL_OK':10,
	'NO_ATTRIBUTES_TO_CONTROL':11,
	'ALL_KO':20,
	'PARTIAL_KO':21,
	'ALL_UNDETERMINED':22
	}

import os
import PyTango

subject = '"'+os.environ['TANGO_HOST']+': HdbArchivingWatcher reported some errors'+'"'
receivers = ['srubio@cells.es','rranz@cells.es']
watcher = PyTango.DeviceProxy('archiving/hdbarchivingwatcher/1')

health = watcher.read_attribute('ArchivingHealth').value
if health != ArchivingHealth['ALL_OK']:
	report = watcher.read_attribute('FormattedReport').value[0]
	os.system(' '.join(['echo','"'+report+'"','| mail -s ',subject]+receivers))
else:
	print 'Lo archiving va bene ...'
