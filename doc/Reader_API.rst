Reader API
==========

.. contents::

Reader object description
-------------------------

@TODO

Reader API
----------

@TODO

Previous docs
-------------

This file should merge information previously available at

* doc/brief/ReaderAndDecimation.rst
* doc/ReaderSchemas.rst

Reader test cases
-----------------

* Reader should be capable to get data from multiple databases
 * When DB schemas overlap for an attribute, it should be able to apply fallback schemas
* When aliases files are used  (TC_3 => Thermocouples[3]), this should be applied or not for each schema
 * Arrays may be present in models and aliases (like TC_3 => Thermocouples[3]); this should work for all schemas
* Relative timestamps (moving windows) to be suported

Reader and trends
-----------------

The interaction between Taurus and PyTangoArchiving is done at Reader level or using PyTangoArchiving.widget.trend.getArchivedTrendValues method:

::

    def getArchivedTrendValues(trend_set,model,start_date=0,stop_date=None,
            log='INFO',use_db=True,db_config='',decimate=True,
            multiprocess=USE_MULTIPROCESS,insert=False,forced=False):

* An STARTUP_DELAY global variable can be set to avoid DB queries during GUI initialization
* This method parses tango host, attribute and model from the model string; being attribute just dev/attr and model using the full URI with fqdn host.
* Then obtains the ArchivedTrendLogger singleton (per widget) that will record logs and caches (last intervals requested).
 * lasts = (start,stop,history length, last query time)
* On the parent trend widget, checks buffer (numpy arrays) existence and the current bounds

* No query is allowed if interval between queries is less than MIN_REFRESH_PERIOD (even if bounds differ?)
 * logget.setLastArgs is used to update the time of the last query (before it is even tried, to avoid repetitive retrying)

* if forced argument is used, then start,stop = trend.bounds
* elif start,stop are defined ... they are extracted to be inserted within the buffer
* else, start,stop are obtained searching for gaps in the existing buffers
 * ZONE = where to insert in the buffer: begin, middle, end
 * area = % of buffer to override (a tuple of 2 values!?))

* and then, it comes the update by bunches, if start-stop > MAX_QUERY_TIME (10 days by default!)
  * it basically restricts the query N to MAX_QUERY_LENGTH( 1e5) and resets lasts.history = 0
  
* afterwards, stored args are rounded to minutes

* if update wasn't forced (default is False) it will be rejected if:
 * the interval is considered too small if range < MIN_WINDOW (60s) or area is below 10% in the middle
 * the current range dont differ from lasts (rounded to 1 minut)
 
If all previous conditions are met, then data retrieval starts:

* 
