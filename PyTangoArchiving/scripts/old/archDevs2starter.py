
#THIS IS A JYTHON SCRIPT!
#ADD TangORB.jar TO CLASSPATH AND LAUNCH >jython devs2starter.py

from java.lang import System
System.setProperty('TANGO_HOST','alba02.cells.es:10000')

from fr.esrf.TangoApi import *

db = ApiUtil.get_db_obj()

#Check the number of available Startup Levels and adapt it
#db.get_class_property_list('Starter','*')
dd=db.get_class_property('Starter','NbStartupLevels')
nbactual=0
if not dd.is_empty():
	nbactual=dd.extractLong()
if nbactual<200:
	db.put_class_property('Starter',[DbDatum('NbStartupLevels',200)])


def add2starter(host,server,instance,level,export=False):
	admin = '/'.join(['dserver',server,instance])
	
	di=db.import_device(admin)
	if di.exported is False:
		de=DbDevExportInfo(admin,di.ior,host,di.version)
		ApiUtil.get_db_obj().export_device(de)
		ApiUtil.get_db_obj().unexport_device(admin)
	
	di = db.get_server_info('/'.join([server,instance]))
	di.host,di.controlled,di.startup_level = host,1,level
	print 'Setting ','/'.join([server,instance]),' with host=',di.host,'; controlled=',di.controlled,'; startup_level=',di.startup_level
	db.put_server_info(di)
	
host = 'palantir01'
"""
for i in range(10):
	add2starter(host,'PySignalSimulator','%02d'%(i+31),5)
	
for i in range(10):
	add2starter(host,'PySignalSimulator','%02d'%(i+41),6)
	
for i in range(10):
	add2starter(host,'PySignalSimulator','%02d'%(i+51),7)
"""


#Archiving Devices
for i in range(1,5):
	add2starter(host,'HdbArchiver','%02d'%i,100+i)
	add2starter(host,'TdbArchiver','%02d'%i,100+i)


add2starter(host,'SnapArchiver',str(1),100)
add2starter(host,'HdbExtractor',str(1),100)
add2starter(host,'TdbExtractor',str(1),100)
add2starter(host,'SnapExtractor',str(1),100)
add2starter(host,'ArchivingManager',str(1),200)
add2starter(host,'SnapManager',str(1),200)
add2starter(host,'HdbArchivingWatcher',str(1),200)
add2starter(host,'TdbArchivingWatcher',str(1),200)

