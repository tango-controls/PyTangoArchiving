#Tests for PyTangoArchiving.utils

import sys
import fandango
import PyTangoArchiving.utils as ptau

VERBOSE = '-v' in sys.argv

@fandango.CatchedArgs(verbose=VERBOSE)
def test_parse_property(
    kwargs = {'name':'',
        'value':'tango://alba03.cells.es:10000/building/ct/alarms-scw/state'
            ';strategy=ALWAYS;ttl=0'},
    result = {'name': 
            'tango://alba03.cells.es:10000/building/ct/alarms-scw/state',
                'strategy': 'ALWAYS',
                'ttl': '0'} ):
        
    return ptau.parse_property(**kwargs) == result

#test_parse_property = fandango.Catched(test_parse_property, verbose=False)

if __name__ == '__main__':
    
    print('\nTesting PyTangoArchiving.utils\n')
    r = True
    for f,ff in locals().items():
        if fandango.isCallable(ff):
            v = ff()
            print('\t%s:\t%s' % (f,v))
            r = r and v
    print('\n')
    sys.exit(int(not r))
    
    
