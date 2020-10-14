


import time
  
class AbstractReader(object):
    """
    Subclass this class to create a PyTangoArchiving Reader for your specific DB
    
    e.g. TimeDBReader(AbstractReader)
    """

    def __init__(self,config='',...):
        '''
        config must be an string like user:passwd@host 
        or a json-like dictionary "{'user':'...','passwd':'...'}"
        '''
        self.db = YourDb(config)
        return

    def get_database(self,epoch=-1):
        """
        This method should provide the current connection object to DB
        
        
        """
        return self.db

    def get_attributes(self,active=False,regexp=''):
        """ 
        Queries the database for the current list of archived attributes.
        arguments:
            active: True/False: attributes currently archived
            regexp: '' :filter for attributes to retrieve
        """
        return list()

    def is_attribute_archived(self,attribute):
        """
        Returns if an attribute has values in DB.
        If active=True, only returns for value currently adding new values
        """
        return True

    def load_last_values(self,attribute):
        """
        Returns last value inserted in DB for an attribute
        
        (epoch, r_value, w_value, quality)
        """
        return (time.time(), 0., 0., 0)

    def get_attribute_values(self,attribute,start_date,stop_date=None,
        decimate=False):
        """
        Returns attribute values between dates in the format:
            [(epoch0, r_value, w_value, quality), 
            (epoch1, r_value, w_value, quality),
            (epoch2, Exception, None, ATTR_INVALID),
            ... ]
        decimate may be False, True or an aggregation method
        w_value and quality are optional, while r_value is mandatory
        """
        return [(time.time(), 0., 0., 0)]

    def get_attributes_values(self,attribute,start_date,stop_date=None,
        decimate=False, correlate=False):
        """
        Returns attribute values between dates in the format:
            [(epoch0, (r_value,w_value,quality)), 
            (epoch1, (r_value,w_value,quality)),
            (epoch2, (Exception, None, ATTR_INVALID)),
            ... ]
        decimate may be False, True, an aggregation method or just an interval in seconds

        if correlate is True, attributes with no value in the interval will be correlated
        """
        return {'attr0':[(time.time(), 0., 0., 0)], 'attr1':[(time.time(), 0., 0., 0)]}
