########################################################################
## Time conversion methods from Fandango
########################################################################

import time, datetime, re, traceback

END_OF_TIME = 1024*1024*1024*2-1 #Jan 19 04:14:07 2038

TIME_UNITS = { 'ns': 1e-9, 'us': 1e-6, 'ms': 1e-3, '': 1, 's': 1, 'm': 60, 
    'h': 3600, 'd': 86.4e3, 'w': 604.8e3, 'M': 30*86.4e3, 'y': 31.536e6 }
TIME_UNITS.update((k.upper(),v) for k,v in list(TIME_UNITS.items()) if k!='m')

#@todo: RAW_TIME should be capable to parse durations as of ISO 8601
RAW_TIME = ('^(?:P)?([+-]?[0-9]+[.]?(?:[0-9]+)?)(?: )?(%s)$'
            % ('|').join(TIME_UNITS)) # e.g. 3600.5 s

MYSQL_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
ISO_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

global DEFAULT_TIME_FORMAT
DEFAULT_TIME_FORMAT = MYSQL_TIME_FORMAT

ALT_TIME_FORMATS = [ ('%s%s%s' % (
    date.replace('-',dash),separator if hour else '',hour)) 
        for date in ('%Y-%m-%d','%y-%m-%d','%d-%m-%Y',
                        '%d-%m-%y','%m-%d-%Y','%m-%d-%y')
        for dash in ('-','/')
        for separator in (' ','T')
        for hour in ('%H:%M','%H:%M:%S','%H','')]
        
def set_default_time_format(dtf, test = True):
    """
    Usages:
    
        fandango.set_default_time_format('%Y-%m-%d %H:%M:%S')
        
        or
        
        fandango.set_default_time_format(fandango.ISO_TIME_FORMAT)
        
    """
    if test:
        str2time(time2str(cad = dtf), cad = dtf)
    global DEFAULT_TIME_FORMAT
    DEFAULT_TIME_FORMAT = dtf

def now():
    return time.time()

def time2tuple(epoch=None, utc=False):
    if epoch is None: epoch = now()
    elif epoch<0: epoch = now()-epoch
    if utc:
        return time.gmtime(epoch)
    else:
        return time.localtime(epoch)
    
def tuple2time(tup):
    return time.mktime(tup)

def date2time(date,us=True):
    """
    This method would accept both timetuple and timedelta
    in order to deal with times coming from different
    api's with a single method
    """
    try:
      t = tuple2time(date.timetuple())
      us = us and getattr(date,'microsecond',0)
      if us: t+=us*1e-6
      return t
    except Exception as e:
      try:
        return date.total_seconds()
      except:
        raise e

def date2str(date, cad = '', us=False):
    #return time.ctime(date2time(date))
    global DEFAULT_TIME_FORMAT
    cad = cad or DEFAULT_TIME_FORMAT
    t = time.strftime(cad, time2tuple(date2time(date)))
    us = us and getattr(date,'microsecond',0)
    if us: t+='.%06d'%us
    return t

def time2date(epoch=None):
    if epoch is None: epoch = now()
    elif epoch<0: epoch = now()-epoch
    return datetime.datetime.fromtimestamp(epoch)

def utcdiff(t=None):
    return now() - date2time(datetime.datetime.utcnow())  

def time2str(epoch=None, cad='', us=False, bt=True,
             utc=False, iso=False):
    """
    cad: introduce your own custom format (see below)
    use DEFAULT_TIME_FORMAT to set a default one
    us=False; True to introduce ms precission
    bt=True; negative epochs are considered relative from now
    utc=False; if True it converts to UTC
    iso=False; if True, 'T' will be used to separate date and time
    
    cad accepts the following formats:
    
    %a 	Locale’s abbreviated weekday name. 	 
    %A 	Locale’s full weekday name. 	 
    %b 	Locale’s abbreviated month name. 	 
    %B 	Locale’s full month name. 	 
    %c 	Locale’s appropriate date and time representation. 	 
    %d 	Day of the month as a decimal number [01,31]. 	 
    %H 	Hour (24-hour clock) as a decimal number [00,23]. 	 
    %I 	Hour (12-hour clock) as a decimal number [01,12]. 	 
    %j 	Day of the year as a decimal number [001,366]. 	 
    %m 	Month as a decimal number [01,12]. 	 
    %M 	Minute as a decimal number [00,59]. 	 
    %p 	Locale’s equivalent of either AM or PM. 	(1)
    %S 	Second as a decimal number [00,61]. 	(2)
    %U 	Week number of the year (Sunday as the first day of the week) as a decimal number [00,53]. 
    All days in a new year preceding the first Sunday are considered to be in week 0. 	(3)
    %w 	Weekday as a decimal number [0(Sunday),6]. 	 
    %W 	Week number of the year (Monday as the first day of the week) as a decimal number [00,53]. 
    All days in a new year preceding the first Monday are considered to be in week 0. 	(3)
    %x 	Locale’s appropriate date representation. 	 
    %X 	Locale’s appropriate time representation. 	 
    %y 	Year without century as a decimal number [00,99]. 	 
    %Y 	Year with century as a decimal number. 	 
    %Z 	Time zone name (no characters if no time zone exists). 	 
    %% 	A literal '%' character.    
    """
    if epoch is None: epoch = now()
    elif bt and epoch<0: epoch = now()+epoch
    global DEFAULT_TIME_FORMAT 
    if cad:
        cad = 'T'.join(cad.split(' ',1)) if iso else cad
    else:
        cad = ISO_TIME_FORMAT if iso else DEFAULT_TIME_FORMAT

    t = time.strftime(cad,time2tuple(epoch,utc=utc))
    us = us and epoch%1
    if us: t+='.%06d'%(1e6*us)
    return t
  
epoch2str = time2str
 
def str2time(seq='', cad='', throw=True, relative=False):
    """ 
    :param seq: Date must be in ((Y-m-d|d/m/Y) (H:M[:S]?)) format or -N [d/m/y/s/h]
    
    See RAW_TIME and TIME_UNITS to see the units used for pattern matching.
    
    The conversion itself is done by time.strptime method.
    
    :param cad: You can pass a custom time format
    :param relative: negative times will be converted to now()-time
    :param throw: if False, None is returned instead of exception
    """
    try: 
        if seq in (None,''): 
            return time.time()
        if 'NOW-' in seq:
            seq,relative = seq.replace('NOW',''),True
        elif seq=='NOW':
            return now()
        
        t, seq = None, str(seq).strip()
        if not cad:
            m = re.match(RAW_TIME,seq) 
            if m:
                #Converting from a time(unit) format
                value,unit = m.groups()
                t = float(value)*TIME_UNITS[unit]
                return t # must return here
                
        #Converting from a date format
        ms = re.match('.*(\.[0-9]+)$',seq) #Splitting the decimal part
        if ms: 
            ms,seq = float(ms.groups()[0]),seq.replace(ms.groups()[0],'')

        if t is None:
            #tf=None will try default system format
            global DEFAULT_TIME_FORMAT
            time_fmts = ([cad] if cad else 
                         [DEFAULT_TIME_FORMAT,None] + ALT_TIME_FORMATS)
            for tf in time_fmts:
                try:
                    tf = (tf,) if tf else () 
                    t = time.strptime(seq,*tf)
                    break
                except: 
                    pass
                
        v = time.mktime(t)+(ms or 0)
        if relative and v<0:
            v = fn.now()-v
        return v
    except: 
        if throw:
            raise Exception('PARAMS_ERROR','unknown time format: %s' % seq)
        else:
            return None
        

str2epoch = str2time

def time2gmt(epoch=None):
    if epoch is None: epoch = now()
    return tuple2time(time.gmtime(epoch))
    
def timezone():
    t = now()
    return old_div(int(t-time2gmt(t)),3600)

#Auxiliary methods:
def ctime2time(time_struct):
    try:
      return (float(time_struct.tv_sec)+1e-6*float(time_struct.tv_usec))
    except:
      return -1
    
def mysql2time(mysql_time,us=True):
    try:
      return date2time(mysql_time,us=us)
      #t = time.mktime(mysql_time.timetuple())
    except:
      return -1
