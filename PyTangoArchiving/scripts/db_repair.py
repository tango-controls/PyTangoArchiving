#!/usr/bin/python

import MySQLdb,sys,time,os
try:
    import fandango
except:
    sys.path.append('/homelocal/sicilia/lib/python/site-packages/')
    import fandango

# Author: osanchez, srubio
# -------------------------------
# mysqlcheck --repair --all-databses doesn't works so well otherwise the SQL command REPAIR TABLE... works 
# properly.
# -------------------------------

__doc__="""db_repair.py """
#For a proper definition would be better to have long name before the short name
__options__ = [
    ('h','help','Show Usage'),
    ('h','host=','DB Host'),
    ('n','no-prompt','Dont ask for confirmation'),
    ('f','force','Check all tables'),    
    ('d:','days=','Number of days to check; 0 for raw check'),
    ('','','user'),
    ('','','password')]
shorts,ops,longs,args,usage = [],[],[],[],''
for s,l,d in __options__:
    if not s and not l: 
        args.append(d)
    else: 
        usage+='\n\t%s%s%s : %s' %(s,l and '/' or '',l,d)
        if s: (shorts if not s.endswith(':') else ops).append(s)
        if l: longs.append(l)
__usage__ = 'Usage:\n'+'\tdb_repair.py '+'[%s]'%''.join(shorts)+' '+' '.join('[-%s X]'%o[:-1] for o in ops)+' ' +' '.join('[--%s%s]'%(l,'X' if l.endswith('=') else '') for l in longs)+' '+' '.join(args)+'\n'+usage+'\n'
def usage(): return __doc__+'\n\n'+__usage__

def do_repair(user,passwd,condition="engine is null",database="information_schema",force=False,days=0,db_host='localhost') :
    sql = "select CONCAT(table_schema, '.', table_name) from tables where %s" % condition
    db_con = MySQLdb.connect(db_host, port=3306, user=user,passwd=passwd,db=database)
    cursor = db_con.cursor()
    cursor.execute(sql)
    rset = cursor.fetchall()
    print '%d tables match condition'%len(rset)
    now = time.time()
    days = days or 60
    tlimit = fandango.time2str(now-days*24*3600);
    now = fandango.time2str(now);
    for item in rset :
        try:
            if fandango.isSequence(item): 
                item = item[0]
            if force: 
                raise Exception,'force=True, all tables to be checked'
            elif 'att_' in item: 
                q = "select count(*) from %s where time between '%s' and '%s' order by time"%(item,tlimit,now)
                cursor.execute(q)
                count = cursor.fetchone()[0]
                q = "select * from %s where time between '%s' and '%s' order by time"%(item,tlimit,now)
                print q
                cursor.execute(q)  # desc limit 1'%item);
                l = len(cursor.fetchall())
                if abs(count-l)>5: 
                  raise Exception('%d!=%d'%(count,l))
            else: 
              raise Exception,'%s is a config table'%item
        except Exception,e:
            print e
            print 'Repairing %s ...' % item
            cursor.execute('repair table %s' % item)
            print '[OK]\n'
        time.sleep(.001)
    cursor.close()
    db_con.close()

def main():
    import getopt
    t0,t1,t2 = sys.argv[1:],''.join(s for s,l,d in __options__ if s),[l for s,l,d in __options__ if l]
    print t0,t1,t2
    opts,args = getopt.getopt(t0,t1,t2)
    opts = dict((k.strip('-'),v) for k,v in opts+[('',args)])
    t0 = time.time()
    if any(o in opts for o in ('h','?','help')):
        print usage()
        sys.exit()
    else:
        if not opts['']:
            user = raw_input('Enter user:')
            passwd = raw_input('Enter password:')
        else:
            user,passwd = opts[''][:2]
        db_host = opts.get('host','localhost')
        os.system('mysqladmin -h %s -u %s -p%s flush-hosts'%(db_host,user,passwd))
        condition = 'engine is null OR table_schema like "hdb%%" OR table_schema like "tdb%%"'
        if 'no-prompt' not in opts:
            new_condition = raw_input('Query condition (%s):'%condition)
            condition = new_condition.strip() or condition
        do_repair(user,passwd,condition,force='force' in opts or 'f' in opts,days=int(opts.get('days',0)),db_host=db_host)
        print 'database repair finished in %d seconds'%(time.time()-t0)
    pass

if (__name__ == '__main__') :
    main()
