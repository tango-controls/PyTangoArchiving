import fandango as fn
from fandango import isCallable, isString, str2type, clsub

def call(args=None,locals_=None):
    """
    Calls a method from local scope parsing a pipe-like argument list
    """
    if args is None:
        import sys
        args = sys.argv[1:]
    f,args = args[0],args[1:]
    
    print(f,args)
    
    if not isCallable(f):
        locals_ = locals_ or globals()
        if f=='help':
            if args and args[0] in locals_:
                n,o = args[0],locals_[args[0]]
                if hasattr(o,'func_code'):
                    n = n+str(o.func_code.co_varnames)
                return '%s:\n%s' % (n,o.__doc__)
            else:
                m = [k for k,v in locals_.items() if isCallable(v)]
                return ('\n'.join(sorted(m,key=str.lower)))
        f = locals_.get(f,None) 
    if all(isString(a) for a in args):
        args = map(str2type,args)
    return f(*args)    

def parse_dump(filename):
    lines = open(filename).readlines()
    nname = 'int.'+filename
    final = open(nname,'w')
    for l in lines:
        l = l.strip('\r\n')
        if fn.re.search('ID|id_|substitute',l):
            #l = l.replace('smallint(','int('
                          #).replace('mediumint(','int('
                          #).replace('int(6)','int(9)'
                          #).replace('int(5)','int(9)'
            l = clsub('(smallint|mediumint)\([0-9]\)','int(9)',l)
        final.write(l+'\n')
    final.close()
    print(nname+': done')

if __name__ == '__main__':
    call()
