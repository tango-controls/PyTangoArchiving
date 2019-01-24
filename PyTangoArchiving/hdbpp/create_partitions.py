import sys
import fandango as fd

assert sys.argv[1:], 'Start date required (e.g. 2017-08-01)'

tables = {

    'att_array_devdouble_ro':'adr',
    'att_array_devlong_ro':'alr',
    'att_array_devshort_ro':'ahr',
    'att_array_devstring_ro':'asr',
    'att_array_devstate_ro':'atr',

    'att_scalar_devdouble_ro':'sdr',
    'att_scalar_devdouble_rw':'sdw',
    
    'att_scalar_devfloat_ro':'sfr',
    'att_scalar_devlong_ro':'slr',
    'att_scalar_devlong_rw':'slw',
    'att_scalar_devstate_ro':'str',
    'att_scalar_devstring_ro':'ssr',
    'att_scalar_devshort_ro':'shr',
    'att_scalar_devshort_rw':'shw',
    
    }
    
start_date = sys.argv[1] # '2017-08-01'
npartitions = 20
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
    

head = "ALTER TABLE %s PARTITION BY RANGE(TO_DAYS(data_time)) ("
line = "PARTITION %s%s VALUES LESS THAN (TO_DAYS('%s'))"
lines = []

for t,p in tables.items():
    lines.append(head%t)
    for i in range(0,npartitions):
        date = inc_months(start_date,i)
        end = inc_months(date,1)
        l = line%(p,date.replace('-',''),end)
        if i<(npartitions-1): l+=','
        lines.append(l)
    lines.append(');\n\n')
    
print('\n'.join(lines))
    
    
