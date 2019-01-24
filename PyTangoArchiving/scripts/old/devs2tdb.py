from PyTango import *
import time
import traceback
import inspect
import sys

dp = DeviceProxy('archiving/archivingmanager/1')
dp.set_timeout_millis(60000);
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


#THIS WERE THE ARGUMENTS THAT WORKED ON iPython:
#archiver = DeviceProxy('archiving/archivingmanager/1')
#archiver.set_timeout_millis(30000)
#archiver.command_inout('ArchivingStartTdb',['0','1','sim/pysignalsimulator/01-01/A1','MODE_P','1000','TDB_SPEC',str(int(10*60e3)),str(int(3600e3*24*3)) ])

def dev2tdb(config,attributes,mode):
	#print 'a'
	export=str(int(10*60e3))
	keeping=str(int(3600e3*24*3))
	args = config+attributes+mode+['TDB_SPEC',export,keeping]
	try:
		#print 'b'
		isArch = dp.command_inout('IsArchivedTdb',attributes)
		for i,v in enumerate(isArch):
			if v:
				print 'Attributes ',attributes,' are actually being archived ... Stopping ...'
				dp.command_inout('ArchivingStopTdb',attributes[i:i+1])
		r = dp.command_inout('ArchivingStartTdb',args) #It Worked!
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
	config = ['0','1']
	mode = ['MODE_P',str(period)]
	for n in range(1,1+ndevs):
		member = '%02d'%m+'-'+'%02d'%n
		device='sim/pysignalsimulator/'+member
		#attributes.append(device+'/A1')
		attributes=[device+'/A1']
		print 'Starting TdbArchiver with mode ',mode,' for attributes: ',attributes
		dev2tdb(config,attributes,mode)
		time.sleep(1)

