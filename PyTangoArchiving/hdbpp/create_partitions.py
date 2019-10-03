import sys
import fandango as fd
import PyTangoArchiving


from argparse import ArgumentParser
parser = ArgumentParser()
parser.add_argument('--tables', metavar='N', type=str, nargs='+',
                    help='optional, tables to partition')
parser.add_argument('--start', type=str, 
                    help='MANDATORY, first partition to create')
parser.add_argument('--api', type=str, help='if provided, it will generate '
                    'partitions from the existing point')
parser.add_argument('--nparts', type=int, help='partitions to add at the end,'
                    ' if not set, nothing is done except if --add_last 1')
parser.add_argument('--int_time', type=bool, 
                    help='partition using INT_TIME column')
parser.add_argument('--add_last', type=bool, 
                    help='add a prefix_last partition at the end')

args = parser.parse_args()
if not args.start and not args.add_last: 
    raise Exception('--start or --add_last argument required (e.g. 2017-08-01)')

if args.api:
    api = PyTangoArchiving.api(args.api)
else:
    api = None

tables = PyTangoArchiving.hdbpp.query.partition_prefixes
### BUT, NOT ALL TABLES ARE IN THIS LIST!
# I'm partitioning only the big ones, and ignoring the others
# boolean, encoded, enum, long64 uchar ulong64, ulong, ushort
# b, e, n, l64, ul6, ul, us, uc

start_date = args.start or fd.time2str().split()[0]
npartitions = args.nparts or 0
counter = 0

def inc_months(date,count):
    y,m,d = map(int,date.split('-'))
    m = m+count
    r = m%12
    if r:
        y += int(m/12)
        m = m%12
    else:
        y += int(m/12)-1
        m = 12
    return '%04d-%02d-%02d'%(y,m,d)

intcol = 'int_time'
    
if args.int_time:
    newc = ("alter table %s add column int_time INT "
        "generated always as (TO_SECONDS(data_time)-62167222800) PERSISTENT;")

    newi = ("drop index att_conf_id_data_time on %s;")
    newi += ("\ncreate index i%s on %s(att_conf_id, int_time);")
    head = "ALTER TABLE %s "
    comm = "PARTITION BY RANGE(int_time) ("
    line = "PARTITION %s%s VALUES LESS THAN (TO_SECONDS('%s')-62167222800)"
else:
    head = "ALTER TABLE %s "
    comm = "PARTITION BY RANGE(TO_DAYS(data_time)) ("
    line = "PARTITION %s%s VALUES LESS THAN (TO_DAYS('%s'))"

lines = []

tlist = [t for t in tables if not args.tables or t in args.tables]

for t in tlist:
    p = tables[t]
    if args.int_time and (not api or not intcol in api.getTableCols(t)):
        lines.append(newc%t)
        lines.append(newi%(t,p,t))
    eparts = [] if not api else api.getTablePartitions(t)
    lines.append(head%t)
    if not any(eparts):
        lines.append(comm)
    elif p+'_last' in eparts:
        lines.append('REORGANIZE PARTITION %s INTO (' % (p+'_last'))
    else:
        lines.append('ADD PARTITION (')
            
    for i in range(0,npartitions):
        date = inc_months(start_date,i)
        end = inc_months(date,1)
        pp = p+date.replace('-','')
        l = line%(p,date.replace('-',''),end)
        if i<(npartitions-1):
            l+=','
        if pp not in eparts:
            lines.append(l)

    if args.add_last and p+'_last' not in eparts or 'REORGANIZE' in str(lines):
        if not lines[-1][-1] in ('(',','):
            lines[-1] += ','
        lines.append('PARTITION %s_last VALUES LESS THAN (MAXVALUE)'%p)
            
    lines.append(');\n\n')
    
print('\n'.join(lines))
    
    
