from PyTango import *
db = Database()

archHost = 'alba02.cells.es'
rateDS = 5
nDS = 20

def addArchDev(server,instance,member):
	sep = '/'; domain = 'archiving'; family = server.lower();
	di = DbDevInfo()
	di.name,di._class,di.server = \
		sep.join([domain,family,member]), \
		server,sep.join([server,instance])
	db.add_device(di)
	
createServers = True
if createServers:
	print 'Creating Archiving device Servers: \n', \
		'ArchivingManager/1, SnapManager/1, HdbArchiver/01-',nDS,', TdbArchiver/01-',nDS, \
		'SnapArchiver/1, HdbExtractor/1, TdbExtractor/1, SnapExtractor/1, ',\
		'HdbArchivingWatcher/1, TdbArchivingWatcher/1'
	addArchDev('ArchivingManager','1','1')
	addArchDev('SnapManager','1','1')
	for m in range(1,nDS+1):
		try: db.delete_server('HdbArchiver/'+'%02d'%m)
		except:	pass
		try: db.delete_server('TdbArchiver/'+'%02d'%m)
		except:	pass
		for n in range(1,rateDS+1):
			addArchDev('HdbArchiver','%02d'%m,'%02d'%m+'-'+'%02d'%n)
			addArchDev('TdbArchiver','%02d'%m,'%02d'%m+'-'+'%02d'%n)
			pass
	addArchDev('SnapArchiver','1','1')
	addArchDev('HdbExtractor','1','1')
	addArchDev('TdbExtractor','1','1')
	addArchDev('SnapExtractor','1','1')
	addArchDev('HdbArchivingWatcher','1','1')
	addArchDev('TdbArchivingWatcher','1','1')
	pass

print 'Setting HdbArchiver class properties'
dclass = 'HdbArchiver'
db.put_class_property(dclass,{'DbHost':[archHost]})
db.put_class_property(dclass,{'DbName':['hdb']})
#db.put_class_property(dclass,{'Facility':['false']})

print 'Setting TdbArchiver device properties'
for m in range(1,nDS+1):
	for n in range(1,rateDS+1):
		dname = 'archiving/tdbarchiver/'+'%02d'%m+'-'+'%02d'%n
		db.put_device_property(dname,{'DsPath':['/tmp/archiving/tdb']})
		db.put_device_property(dname,{'DbPath':['/tmp/archiving/tdb']})
		db.put_device_property(dname,{'DiaryPath':['/tmp/archiving/tdb']})

print 'Setting TdbArchiver class properties'
dclass = 'TdbArchiver'
db.put_class_property(dclass,{'DbHost':[archHost]})
db.put_class_property(dclass,{'DbName':['tdb']})
#db.put_class_property(dclass,{'Facility':['false']})

print 'Setting snaparchiver device properties'
dname = 'archiving/snaparchiver/1'
db.put_device_property(dname,{'beansFileName':['beansMySQL.xml']})
#db.put_device_property(dname,{'dbHost':['localhost']})
#db.put_device_property(dname,{'dbName':['snap']})
#db.put_device_property(dname,{'dbPassword':['archiver']})
#db.put_device_property(dname,{'dbUser':['archiver']})
#db.put_class_property(dname,{'facility':['false']})

print 'Setting snaparchiver class properties'
dclass = 'SnapArchiver'
db.put_class_property(dclass,{'DbHost':[archHost]})
db.put_class_property(dclass,{'DbName':['snap']})
db.put_class_property(dclass,{'dbSchema':['snap']})

print 'Setting hdbextractor device properties'
dclass = 'HdbExtractor'
db.put_class_property(dclass,{'dbHost':[archHost]})
#db.put_device_property(dname,{'dbPassword':['extractor']})
#db.put_device_property(dname,{'dbUser':['extractor']})

print 'Setting tdbextractor device properties'
dclass = 'TdbExtractor'
db.put_class_property(dclass,{'dbHost':[archHost]})
#db.put_device_property(dname,{'dbPassword':['extractor']})
#db.put_device_property(dname,{'dbUser':['extractor']})

print 'Setting snapextractor device properties'
dclass = 'SnapExtractor'
db.put_class_property(dclass,{'dbHost':[archHost]})
#db.put_device_property(dname,{'dbPassword':['extractor']})
#db.put_device_property(dname,{'dbUser':['extractor']})

print 'Setting hdbarchivingwatcher device properties'
dname = 'archiving/hdbarchivingwatcher/1'
db.put_device_property(dname,{'doArchiverDiagnosis':['True']})
db.put_device_property(dname,{'doStartOnInitDevice':['True']})
db.put_device_property(dname,{'macroPeriod':['7200']})
db.put_device_property(dname,{'hdbPwd':['browser']})
db.put_device_property(dname,{'hdbUser':['browser']})

print 'Setting hdbarchivingwatcher class properties'
dclass = 'HdbArchivingWatcher'
db.put_class_property(dclass,{'DbHost':[archHost]})
db.put_class_property(dclass,{'DbName':['hdb']})
#db.put_class_property(dclass,{'Facility':['false']})

print 'Setting tdbarchivingwatcher device properties'
dname = 'archiving/tdbarchivingwatcher/1'
db.put_device_property(dname,{'doArchiverDiagnosis':['True']})
db.put_device_property(dname,{'doStartOnInitDevice':['True']})
db.put_device_property(dname,{'macroPeriod':['7200']})
db.put_device_property(dname,{'tdbPwd':['browser']})
db.put_device_property(dname,{'tdbUser':['browser']})

print 'Setting tdbarchivingwatcher class properties'
dclass = 'TdbArchivingWatcher'
db.put_class_property(dclass,{'DbHost':[archHost]})
db.put_class_property(dclass,{'DbName':['tdb']})
#db.put_class_property(dclass,{'Facility':['false']})

#db.put_DbServInfo(server,device,controlled,startup_level)


