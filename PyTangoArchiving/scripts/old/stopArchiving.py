from PyTango import *
import time
import traceback
import inspect
import sys

dp = DeviceProxy('archiving/archivingmanager/1')
nservs = None
ndevs = None
sstart = None

def stopArchiving(attributes,db='Hdb'):
	#print 'a'
	args = attributes
	try:
		#print 'b'
		isArch = dp.command_inout('IsArchived'+db,attributes)
		for i,v in enumerate(isArch):
			if v:
				r = dp.command_inout('ArchivingStop'+db,attributes[i:i+1]) #It Worked!
				if r is not None:
					print r
			else:
				print 'Attributes ',attributes[i:i+1],' are not being archived!!!'
		pass
	except PyTango.DevFailed,e:
		PyTango.Except.print_exception(e)
	except Exception,e:
            	exstring = traceback.format_exc()
            	print 'Exception occurred and catched: ', exstring
		print "Exception '",str(e),"' in ",inspect.currentframe().f_code.co_name	
		print 'Last exception was: \n'+str(e)+'\n'

if len(sys.argv)==2 and sys.argv[1]=='ALL':
	try:	
		print 'Stopping ALL ...'
		watcher = DeviceProxy('archiving/hdbarchivingwatcher/1')
		watcher.set_timeout_millis(300000);
		print 'Getting list of HDB attributes ...'
		attribslist = watcher.command_inout('GetAllArchivingAttributes');
		if len(attribslist):
                        for s in attribslist:
                                attr = s.split(':')[0]
                                print 'Stopping HDB archiving ... ',attr
                                dp.command_inout('ArchivingStopHdb',[attr]);
		
		watcher = DeviceProxy('archiving/tdbarchivingwatcher/1')
		watcher.set_timeout_millis(300000);
		print 'Getting list of TDB attributes ...'
		attribslist = watcher.command_inout('GetAllArchivingAttributes');
		if len(attribslist):
			for s in attribslist:
				attr = s.split(':')[0]
				print 'Stopping TDB archiving ... ',attr
				dp.command_inout('ArchivingStopTdb',[attr]);
		
	except PyTango.DevFailed,e:
		PyTango.Except.print_exception(e)
		
	except Exception,e:
            	exstring = traceback.format_exc()
            	print 'Exception occurred and catched: ', exstring
		print "Exception '",str(e),"' in ",inspect.currentframe().f_code.co_name	
		print 'Last exception was: \n'+str(e)+'\n'	
else:
	if len(sys.argv) >= 3:
		nservs = int(sys.argv[1])
		ndevs = int(sys.argv[2])
		print 'nservs=',nservs,';ndevs=',ndevs
	#THIS WERE THE ARGUMENTS THAT WORKED ON TG_DEVTEST: 0,1,sim/pysignalsimulator/01-01/A1,MODE_P,10000.
	
	if nservs is None:
		nservs = int(raw_input('How many servers do you want to stop %02d?'))
	if sstart is None:
		sstart = int(raw_input('Starting with?'))
	if ndevs is None:
		ndevs = int(raw_input('How many devices do you want to stop for each server?'))
	
	for m in range(sstart,1+nservs):
		attributes=[]
		for n in range(1,1+ndevs):
			member = '%02d'%m+'-'+'%02d'%n
			device='sim/pysignalsimulator/'+member
			#attributes.append(device+'/A1')
			attributes=[device+'/A1']
			print 'Stoping HdbArchiver for attributes: ',attributes
			stopArchiving(attributes)
			
	for m in range(sstart,1+nservs):
		attributes=[]
		for n in range(1,1+ndevs):
			member = '%02d'%m+'-'+'%02d'%n
			device='sim/pysignalsimulator/'+member
			#attributes.append(device+'/A1')
			attributes=[device+'/A1']
			print 'Stoping TdbArchiver for attributes: ',attributes
			stopArchiving(attributes,'Tdb')
	
		#time.sleep(3)

