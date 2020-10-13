.. code::

  class Reader(Object,SingletonMap):

    def __init__(self,config='',...):
        '''
        config must be an string like user:passwd@host
        '''

    def get_database(self,epoch=-1):
        """
        This method should provide the current connection object to DB
        """

    def get_attributes(self,active=False,regexp=''):
        """ 
        Queries the database for the current list of archived attributes.
        arguments:
            active: True/False: attributes currently archived
            regexp: '' :filter for attributes to retrieve
        """

    def is_attribute_archived(self,attribute):
        """
        Returns if an attribute has values in DB.
        If active=True, only returns for value currently adding new values
        """

    def load_last_values(self,attribute):
        """
        Returns last values inserted in DB for an attribute
        """

    def get_attribute_values(self,attribute,start_date,stop_date=None,
        decimate=False):
        """
        Returns attribute values between dates in the format:
            [(epoch0, (r_value,w_value,quality)), 
            (epoch1, (r_value,w_value,quality)),
            (epoch2, (Exception, None, ATTR_INVALID)),
            ... ]
        decimate may be False, True or an aggregation method
        """

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

