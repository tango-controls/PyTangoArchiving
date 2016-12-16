from PyTango import *
import time
import traceback
import inspect
import sys

dp = DeviceProxy('archiving/archivingmanager/1')
dp.set_timeout_millis(90000);
nservs = None
ndevs = None
sstart = None
period = None

if len(sys.argv) >= 3:
	nservs = int(sys.argv[1])
	ndevs = int(sys.argv[2])
	print 'nservs=',nservs,';ndevs=',ndevs
	
if len(sys.argv) >= 4:
	period = int(sys.argv[3])


#THIS WERE THE ARGUMENTS THAT WORKED ON TG_DEVTEST: 0,1,sim/pysignalsimulator/01-01/A1,MODE_P,10000.

def dev2archiver(config,attributes,mode):
	#print 'a'
	args = config+attributes+mode
	try:
		#print 'b'
		isArch = dp.command_inout('IsArchivedHdb',attributes)
		for i,v in enumerate(isArch):
			if v:
				print 'Attributes ',attributes,' are actually being archived ... Stopping ...'
				dp.command_inout('ArchivingStopHdb',attributes[i:i+1])
		time.sleep(1.)
		r = dp.command_inout('ArchivingStartHdb',args) #It Worked!
		#print 'c'
		if r is not None:
			print r
		#print 'd'
		pass
	except PyTango.DevFailed,e:
		PyTango.Except.print_exception(e)
	except Exception,e:
            	exstring = traceback.format_exc()
            	print 'Exception occurred and catched: ', exstring
		print "Exception '",str(e),"' in ",inspect.currentframe().f_code.co_name	
		print 'Last exception was: \n'+str(e)+'\n'

if nservs is None:
	nservs = int(raw_input('How many servers do you want to register %02d?'))
if sstart is None:
	sstart = int(raw_input('Starting with?'))
if ndevs is None:
	ndevs = int(raw_input('How many devices do you want to register from each server?'))
if period is None:
	period = int(raw_input('Archiving Period?'))	

for m in range(sstart,sstart+nservs):
	attributes=[]
	# config = [archivedTogether,numOfAttributes]
	config = ['0','1']
	mode = ['MODE_P',str(period)]
	for n in range(1,1+ndevs):
		member = '%02d'%m+'-'+'%02d'%n
		device='sim/pysignalsimulator/'+member
		#attributes.append(device+'/A1')
		attributes=[device+'/A1']
		print 'Starting HdbArchive with mode ',mode,' for attributes: ',attributes
		dev2archiver(config,attributes,mode)
		time.sleep(20.)

