
from enum import Enum

class Aggregator(Enum):
    """
    Enum to describe aggregation method to use.
    Note that this aggregation functions should
    be supported at the backend level.
    """
    COUNT = 1
    COUNT_ERRORS = 2
    COUNT_NAN = 3
    FIRST = 4
    LAST = 5
    MIN = 6
    MAX = 7
    AVG = 8
    STD_DEV = 9


class AbstractReader(object):
    """
    Subclass this class to create a PyTangoArchiving Reader for your specific DB

    e.g. TimeDBReader(AbstractReader)
    """

    def __init__(self, config='',**kwargs):
        '''
        Config can be an string like user:passwd@host
        or a json-like dictionary "{'user':'...','password':'...','database':}"
        '''
        try:
            self.db = YourDb(**(config or kwargs))
        except:
            raise Exception('WrongDatabaseConfig')
        return

    def get_connection(self):
        """
        Return the connection object to avoid a client
        to open one for custom queries.
        The returned object will be implementation specific.
        """
        return self.db

    def get_attributes(self, active=False, pattern=''):
        """
        Queries the database for the current list of archived attributes.
        arguments:
            active: True: only attributes currently archived
                    False: all attributes, even the one not archiving anymore
            pattern: '' :filter for attributes to retrieve
        """
        return list()

    def is_attribute_archived(self, attribute, active=False):
        """
        Returns if an attribute has values in DB.

        arguments:
            attribute: fqdn for the attribute.
            active: if true, only check for active attributes,
                    otherwise check all.
        """
        return True

    def get_last_attribute_value(self, attribute):
        """
        Returns last value inserted in DB for an attribute

        arguments:
            attribute: fqdn for the attribute.
        returns:
            (epoch, r_value, w_value, quality, error_desc)
        """

        return self.get_last_attributes_values([attribute])[attribute]

    def get_last_attributes_values(self, attributes, columns = 'time, r_value'):
        """
        Returns last values inserted in DB for a list of attributes

        arguments:
            attribute: fqdn for the attribute.
            columns: requested columns separated by commas
        returns:
            {'att1':(epoch, r_value, w_value, quality, error_desc),
             'att2':(epoch, r_value, w_value, quality, error_desc),
             ...
            }
        """

        return {attributes[0]: (time.time(), 0., 0., 0, "")}

    def get_attribute_values(self, attribute,
            start_date, stop_date=None,
            decimate=None,
            **params):
        """
        Returns attribute values between start and stop dates.

        arguments:
            attribute: fqdn for the attribute.
            start_date: datetime, beginning of the period to query.
            stop_date: datetime, end of the period to query.
                       if None, now() is used.
            decimate: aggregation function to use in the form:
                      {'timedelta0':(MIN, MAX, …)
                      , 'timedelta1':(AVG, COUNT, …)
                      , …}
                      if None, returns raw data.
        returns:
            [(epoch0, r_value, w_value, quality, error_desc),
            (epoch1, r_value, w_value, quality, error_desc),
            ... ]
        """
        return self.get_attributes_values([attribute], start_date, stop_date, decimate, False, params)[attribute]

    def get_attributes_values(self, attributes,
            start_date, stop_date=None,
            decimate=None,
            correlate = False,
            columns = 'time, r_value',
            **params):
        """
        Returns attributes values between start and stop dates
        , using decimation or not, correlating the values or not.

        arguments:
            attributes: a list of the attributes' fqdn
            start_date: datetime, beginning of the period to query.
            stop_date: datetime, end of the period to query.
                       if None, now() is used.
            decimate: aggregation function to use in the form:
                      {'timedelta0':(MIN, MAX, …)
                      , 'timedelta1':(AVG, COUNT, …)
                      , …}
                      if None, returns raw data.
            correlate: if True, data is generated so that
                       there is available data for each timestamp of
                       each attribute.
            columns: columns separated by commas
                    time, r_value, w_value, quality, error_desc                       

        returns:
            {'attr0':[(epoch0, r_value, w_value, quality, error_desc),
            (epoch1, r_value, w_value, quality, error_desc),
            ... ],
            'attr1':[(…),(…)]}
        """
        return {'attr0': [(time.time(), 0., 0., 0, '')]
                , 'attr1': [(time.time(), 0., 0., 0, '')]}

###############################################################################

__usage__ = """
Usage:

:> reader : print this help

:> reader [options] list [pattern] : 
    returns matching attributes from database

:> reader [options] <attribute> : 
    print last value for attribute

:> reader [options] <attribute> <start> <stop> : 
    returns values for attribute between given dates
    
Options (at least some is needed):
    --prompt
    --config=user:password@host:port/database
    --database=
    --host=
    --port=
    --user=
    --password=
    
"""

def main(apiclass=AbstractReader,timeformatter=None):
    import sys
    
    args = [a for a in sys.argv[1:] if not a.startswith('-')]
    opts = dict([a.strip('-').split('=') for a in sys.argv[1:] 
                 if a not in args and '=' in a])
    if '--prompt' in sys.argv:
        opts['host'] = input('host:')
        opts['database'] = input('database:')
        opts['user'] = input('user:')
        opts['password'] = input('password:')
        opts['port'] = input('port(3306):') or '3306'
    
    if not args or not opts:
        print(__usage__)
        sys.exit(0)    
    
    reader = apiclass(**opts)
    if args[0] == 'list':
        pattern = (args[1:] or [''])[0]
        print('\n'.join(reader.get_attributes(pattern=pattern)))
    else:
        if args[1:]:
            data = reader.get_attribute_values(args[0],args[1],args[2],
                                            decimate=True)
            for d in data:
                l = '\t'.join(map(str,d))
                if timeformatter:
                    print('%s\t%s' % (timeformatter(d[0]),l))
                else:
                    print(l)
        else:
            print(reader.get_last_attribute_value(args[0]))
            
if __name__ == '__main__':
    main()
