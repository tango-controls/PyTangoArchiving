import sys
import fandango as fn
import PyTangoArchiving as pta
from fandango.db import FriendlyDB
from fandango import time2str, str2time

def get_table_description(api,table):
    if fn.isString(api): api = pta.api(api)
    return api.Query('show create table %s'%table)[-1][-1]

"""

In [1]: import PyTangoArchiving as pta

In [2]: api = pyta.api('hdbpp')
---------------------------------------------------------------------------
NameError                                 Traceback (most recent call last)
<ipython-input-2-b81ce98a59f8> in <module>()
----> 1 api = pyta.api('hdbpp')

NameError: name 'pyta' is not defined

In [3]: table = 'att_array_devfloat_rw'

In [4]: hdbpp = pta.api('hdbpp')
HDBpp(): Loading from Schemas

In [5]: partitions = pta.dbs.get_partitions_from_query(hdbpp,'select * from %s'%table)

In [6]: partitions
Out[6]: 'afw20171201,afw20180101,afw20180201,afw20180301,afw20180401,afw20180501,afw20180601,afw20180701,afw20180801,afw20180901,afw20181001,afw20181101,afw20181201,afw20190101,afw20190201,afw20190301'

In [7]: partitions = 'afw20180301,afw20180401,afw20180501,afw20180601,afw20180701,afw20180801,afw20180901,afw20181001,afw20181101,afw20181201,afw20190101,afw20190201,afw20190301'

In [8]: hdbpp.Query('alter table %s optimize partition %s' % (table,partitions))
Out[8]: [('hdbpp.att_array_devfloat_rw', 'optimize', 'status', 'OK')]

"""

def get_attributes_row_counts(db,attrs='*',start=-86400,stop=-1,limit=0):
    """
    DUPLICATED BY HDBPP.get_attribute_rows !!!
    
    It will return matching $attrs that recorded more than $limit values in 
    the $start-$stop period::
    
      countsrf = get_attributes_row_counts('hdbrf',start=-3*86400,limit=20000)
      
    """
    db = pta.api(db) if fn.isString(db) else db
    start = start if fn.isString(start) else fn.time2str(start) 
    stop = stop if fn.isString(stop) else fn.time2str(stop)
    
    if fn.isString(attrs):
        attrs = [a for a in db.get_attributes() if fn.clmatch(attrs,a)]
        
    r = {}
    for a in attrs:
        i,t,b = db.get_attr_id_type_table(a)
        l = db.Query("select count(*) from %s where att_conf_id = %d"
            " and data_time between '%s' and '%s'"  % (b,i,start,stop))
        c = l[0][0] if len(l) else 0
        if c >= limit:
            r[a] = c
    return r

def get_table_partitions(api,table,description=''):
    """
    DUPLICATED BY pta.dbs.get_partitions_from_query !!!
    """
    if fn.isString(api): api = pta.api(api)
    if not description: description = get_table_description(api,table)
    rows = [l for l in description.split('\n') if 'partition' in l.lower()]
    f = rows[0].split()[-1]
    data = (f,[])
    for i,l in enumerate(rows[1:]):
        try:
            l,n = l.split(),i and rows[i].split() or [0]*6
            data[-1].append((l[1],n[5],l[5]))
        except:
            print(fn.except2str())
            print(i,l,n)
    return(data)

def create_new_partitions(api,table,preffix,key,npartitions):
    """
    This script will create N new partitions, one for each month
    for the given table and key
    """
    
    

"""
cd $FOLDER
FILENAME=$SCHEMA.full.$(date +%F).dmp

echo "$(fandango time2str) Dump to $FOLDER/$FILENAME..."
mysqldump --single-transaction --force --compact --no-create-db --skip-lock-tables --quick -u manager -p $SCHEMA > $FILENAME
echo "$(fandango time2str) Compressing $FOLDER/$FILENAME"
tar zcvf $FILENAME.tgz $FILENAME
echo "$(fandango time2str) Removing $FOLDER/$FILENAME"
rm $FILENAME

"""


def main(*args,**opts):
    schema = args[0]
    api = pta.api(schema)
    tables = [a for a in api.getTables() if fn.clmatch('att_(scalar|array)_',a)]
    descriptions = dict((t,get_table_description(api,t)) for t in tables)
    partitioned = [t for t,v in descriptions.items() if 'partition' in str(v).lower()]
    print('%s: partitioned tables: %d/%d' % (schema,len(partitioned),len(tables)))
    
if __name__ == '__main__' :
    if sys.argv[1:] and (sys.argv[1]=='help' or sys.argv[1] in locals()):
        print(fn.call(locals_=locals()))
    else:
        args,opts = fn.linos.sysargs_to_dict(split=True)
        main(*args,**opts)
    
