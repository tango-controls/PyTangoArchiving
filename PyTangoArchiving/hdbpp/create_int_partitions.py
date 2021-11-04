import sys, traceback
import fandango as fn
import PyTangoArchiving as pta
import PyTangoArchiving.hdbpp.maintenance as ptam

__doc__ = """
Usage:

create_int_index.py db_name host user passwd n_months

"""

try:
    db_name = sys.argv[1]    
    host = sys.argv[2]
    user = sys.argv[3]
    passwd = sys.argv[4]
    max_parts = int(sys.argv[5])
    stop_date = fn.time2str(fn.now()+max_parts*365.25*86400/12.)
    stop_date = stop_date.rsplit('-',1)[0]+'-01'
except:
    print(__doc__)
    sys.exit() 

api = pta.HDBpp(host=host,db_name=db_name,user=user,passwd=passwd)
if fn.check_device(api.manager) or any(api.check_device(e) for e in api.get_archivers()):
    print('Archiving devices must be stop first!')
    sys.exit()

tables = dict((t,api.getTableCreator(t)) for t in api.get_data_tables())

if not all("datetime(3)" in str(tables[t]).lower() for t in tables):
    ptam.alter_data_time(api)
    
ptam.create_db_partitions(api, max_parts=max_parts, stop_date=stop_date,
                          do_it = True, int_time = True)

#for t in tables:
    #if t in pta.hdbpp.query.partition_prefixes:
        #if "int_time" not in str(tables[t]).lower():
            #r.append(ptam.add_int_time_column(api,t,do_it=False))
            #r.append(ptam.add_idx_index(api,t,do_it=False))
    
#for t in tables:
    #if t in pta.hdbpp.query.partition_prefixes:
        #try:
            #r.append(ptam.create_new_partitions(api,t,nmonths=24,
                    #start_date=start_date,add_last=True,do_it=False))
        #except:
            #traceback.print_exc()
            #print('%s failed' % t)
    
#f = open(output_file,'w')
#f.write('\n'.join(l+('' if ';' in l else ';') for l in r if l))
#f.close()
#print('%s written'%output_file)

#tables = {
#### BUT, NOT ALL TABLES ARE IN THIS LIST!
## I'm partitioning only the big ones, and ignoring the others
## boolean, encoded, enum, long64 uchar ulong64, ulong, ushort
## b, e, n, l64, ul6, ul, us, uc

    #'att_array_devdouble_ro':'adr',
    #'att_array_devfloat_ro':'afr',
    #'att_array_devlong_ro':'alr',
    #'att_array_devlong_rw':'alw',    
    #'att_array_devshort_ro':'ahr',
    #'att_array_devboolean_ro':'abr',    
    #'att_array_devboolean_rw':'abw',        
    #'att_array_devstring_ro':'asr',
    #'att_array_devstate_ro':'atr',

    #'att_scalar_devdouble_ro':'sdr',
    #'att_scalar_devdouble_rw':'sdw',
    
    #'att_scalar_devfloat_ro':'sfr',
    #'att_scalar_devlong_ro':'slr',
    #'att_scalar_devlong_rw':'slw',
    #'att_scalar_devshort_ro':'shr',
    #'att_scalar_devshort_rw':'shw',    
    #'att_scalar_devboolean_ro':'sbr',
    #'att_scalar_devboolean_rw':'sbw',

    #'att_scalar_devstate_ro':'str',
    #'att_scalar_devstring_ro':'ssr',
    #'att_scalar_devushort_ro':'sur',
    #'att_scalar_devuchar_ro':'scr',
    
    #'att_array_devfloat_rw':'afw',
    #'att_scalar_devstring_ro':'ssw',
    #}

#start_date = sys.argv[1] # '2017-08-01'
#npartitions = 20
#counter = 0

#def inc_months(date,count):
    #y,m,d = map(int,date.split('-'))
    #m = m+count
    #r = m%12
    #if r:
        #y += int(m/12)
        #m = m%12
    #else:
        #y += int(m/12)-1
        #m = 12
    #return '%04d-%02d-%02d'%(y,m,d)

#newc = ("alter table %s add column int_time INT "
    #"generated always as (TO_SECONDS(data_time)-62167222800) PERSISTENT;")

#newi = ("drop index att_conf_id_data_time on %s;")
#newi += ("\ncreate index i%s on %s(att_conf_id, int_time);")
#head = "ALTER TABLE %s PARTITION BY RANGE(int_time) ("
#line = "PARTITION %s%s VALUES LESS THAN (TO_SECONDS('%s')-62167222800)"
#lines = []

#for t,p in tables.items():
    #lines.append(newc%t)
    #lines.append(newi%(t,p,t))
    #lines.append(head%t)
    #for i in range(0,npartitions):
        #date = inc_months(start_date,i)
        #end = inc_months(date,1)
        #l = line%(p,date.replace('-',''),end)
        #if i<(npartitions-1): l+=','
        #lines.append(l)
    #lines.append(');\n\n')
    
#print('\n'.join(lines))
