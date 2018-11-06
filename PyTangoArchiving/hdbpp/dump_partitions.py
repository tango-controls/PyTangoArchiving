
import fandango as fn
import PyTangoArchiving as pta

def get_table_description(api,table):
    if fn.isString(api): api = pta.api(api)
    return api.Query('show create table %s'%table)[-1][-1]

def get_table_partitions(api,table,description=''):
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

def dump_table_partition(schema,filename,tables='',where=''):
    cmd = "mysqldump --single-transaction --force --compact --no-create-db --skip-lock-tables --quick -u manager -p"
    cmd += " " + schema
    if where:
        cmd += ' --where=" %s"' % where
    if tables:
        cmd += ' --tables '+(' '.join(tables) if fn.isSequence(tables) else tables)
    print(cmd)
    

def main(args,opts):
    schema = args[0]
    api = pta.api(schema)
    tables = [a for a in api.getTables() if fn.clmatch('att_(scalar|array)_',a)]
    descriptions = dict((t,get_table_description(api,t)) for t in tables)
    partitioned = [t for t,v in descriptions.items() if 'partition' in str(v).lower()]
    print('%s: partitioned tables: %d/%d' % (schema,len(partitioned),len(tables)))

def test(args):
    print('testing')
    m = globals().get(args[0])
    print(str(m(*args[1:])))
    
if __name__ == '__main__' :
    args,opts = fn.linos.sysargs_to_dict(split=True)
    print(args,opts)
    if 'test' in opts:
        test(opts.get('test'))
    else:
        main(args,opts)
    
