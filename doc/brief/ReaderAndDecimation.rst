---------------------------------
Decimation On Acquisition of Data
---------------------------------

.. contents::


Levels 
------

 - On clients, decimation is configured in PyTangoArchiving.widgets.dialogs.QReloadWidget, loaded by ArchivedTrendLogger

 - This method is later called by PyTangoArchiving.widgets.trend.getArchivedTrendValues and passed to the Reader object
 
 



Decimation is used on loading data from database sources and before exporting to CSV files or plots.

Methods used:

- Reader.decimation
- fandango.arrays.filter_array

'''Reader.get_attributes_from_db''' takes the data using a direct query to MySQL and then extracts the 
data to a python list of (time,value) tuples.

Decimation modes on Taurus Clients
----------------------------------

The TaurusTrend widget is calling just PyTangoArchiving.trend.getArchivedTrendValues with no arguments::

    ./taurus/qt/qtgui/plot/taurustrend.py:356:  getArchivedTrendValues(self, model, insert=True)
    
This method uses its default arguments and the settings coming from ArchivedTrendLogger dialog.

.. code::

  # All methods linked here are AGGREGATORS, to extract the meaningful value from a chunk
  # All of them will be applied together with a given period (window)

  DECIMATION_MODES = [
    #('Hide Nones',fn.arrays.notnone),
    ('Pick One',fn.arrays.pickfirst), # <<< DEFAULT
    ('Minimize Noise',fn.arrays.mindiff),
    ('Maximize Peaks',fn.arrays.maxdiff),
    ('Average Values',fn.arrays.average),
    ('In Client', False),
    ('RAW',None),        
    ]
    
Other option used is : "RemoveNones" (True by default)

This values are returned by:

 - ArchivedTrendLogger.getDecimation()
 - ArchivedTrendLogger.getNonesCheck()
 - ArchivedTrendLogger.getPeriod() #window frame to apply decimation
 
 If no aggregator method is chosen, then the fallbacks are:
 
  - fn.arrays.notnone if RemoveNones is checked
  - None if RAW extraction is chosen
  - if decimate=True argument in getArchivedTrendValues (default):
    - reader.data_has_changed
    
.. code::

  def dateHasChanged(prev,curr=None):
    v = all(curr) and (not prev or not any(prev[:2]) 
                        or any(abs(x-y)>MIN_WINDOW 
                                for x,y in zip(curr,prev)))
    return v
    
Also a parameter N = MAX_QUERY_LENGTH is passed to Reader!! (MAX_QUERY_LENGTH = 65536*1024)

This parameter is passed only if start-stop >= MAX_QUERY_TIME = 3600*24*1000

Then::

  history = reader.get_attribute_values(model,start_date,stop_date,
                N=N,asHistoryBuffer=False,decimate=decimation) or []
                
Later on, when inserting into the buffer, methods to filter numpy arrays are also used::

    def updateTrendBuffers(...):
        ...
               minstep = abs(t[-1] - t[0]) / 1081.
                logger.warning('In updateTrendBuffers('
                        '(%s - %s)[%d] \n\t+ (%s - %s)[%d],'
                        'fromHistoryBuffer=%s, minstep=%s, overlap=%s)' % (
                            len(self._xBuffer) and time2str(self._xBuffer[0]),
                            len(self._xBuffer) and time2str(self._xBuffer[-1]),
                            len(self._xBuffer) or 0,
                            time2str(t[0]),
                            time2str(t[-1]),
                            len(t) or 0,
                            fromHistoryBuffer,minstep,overlap))
                
                t0 = time.time()
                #No backtracking, normal insertion
                t_index = utils.sort_array(t,decimate=True,as_index=True,
                                            minstep=minstep)
                t,y = t.take(t_index,0),y.take(t_index,0)                                

                if overlap: 
                    #History and current buffer overlap!; resorting data
                    t = numpy.concatenate((t,self._xBuffer.toArray()))
                    y = numpy.concatenate((y,self._yBuffer.toArray()))
                    t_index = utils.sort_array(t,decimate=False,as_index=True)
                    t,y = t.take(t_index,0),y.take(t_index,0)
                    newsize = int(max((parent.DEFAULT_MAX_BUFFER_SIZE,
                                       1.5*len(t))))
                    resetTrendBuffer(self._xBuffer,newsize,t)
                    resetTrendBuffer(self._yBuffer,newsize,y)
                else: 
                    self._xBuffer.extendLeft(t)
                    self._yBuffer.extendLeft(y)
                    
            
    

Decimation in Reader.get_attributes_from_db
-------------------------------------------

decimate arg is False by default, even if True any lists of less than 128 elements will not be decimated

before any other decimation is done, all repeated values are removed using data_has_changed filter(a,b)

then, if decimate differs from data_has_changed, an additional reader.decimation method is executed,
specified by 2 arguments:

- decimate, callable to be passed to fandango.filter_array
- window, string representing a time value ( 1s, 30m , 1m , 0.2 )

Note that SPECTRUM data will NOT be decimated by reader.decimation

For any scalar, if decimation is wanted it will also filter any None,NaN value appearing in the data.

- The default window will be (stop-start)/1080.
- The minimum window will be 1. or (stop-start)/108000.
- Decimation will be applied only if len(history) > (stop-start)/window

WINDOWS SMALLER THAN 1. ARE NOT ALLOWED

The values returned are stored in Reader.cache dictionary and returned to the client

Decimation in TaurusTrends
--------------------------

The default method for decimation in taurus trends is fandango.arrays.maxdiff ; it is passed to 
the Reader object as argument.

Then, an additional decimation is done when the loaded buffer overlaps with existing data.

Decimation in updateTrendBuffers (numpy)
........................................

Once the loaded data is merged with the existing trend buffer, several methods are called:

- PyTangoArchiving.utils.sort_array : sorts a numpy array ensuring that timestamps are unique ordered

- PyTangoArchiving.utils.get_array_steps : obtains the difference between consecutive positions in 
a numpy Array column; the minimum step is set to (tlast-tfirst)/1080.

- numpy.compress : used to remove all entries with timestamp steps equal to 0


Decimation in archiving2csv
---------------------------

In archiving2csv the fandango.filter_array method is called passing the correlation parameters, in order to match
the values of the different columns with the assigned timestamps


