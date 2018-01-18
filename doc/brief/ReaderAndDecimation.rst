----------
Decimation
----------

Decimation is used on loading data from database sources and before exporting to CSV files or plots.

Methods used:

- Reader.decimation
- fandango.arrays.filter_array

'''Reader.get_attributes_from_db''' takes the data using a direct query to MySQL and then extracts the 
data to a python list of (time,value) tuples.

Decimation in Reader.get_attributes_from_db
-------------------------------------------

decimate arg is False by default, even if True any lists of less than 128 elements will not be decimated

before any other decimation is done, all repeated values are removed using data_has_changed filter(a,b)

then, if decimate differs from data_has_changed, an additional reader.decimation method is executed,
specified by 2 arguments:

- decimate, callable to be passed to fandango.filter_array
- window, string 

if it's valid, for any decimation method it will be filtered any None,NaN value appearing in the data

Note that SPECTRUM data will NOT be decimated by reader.decimation

The values returned are stored in Reader.cache dictionary and returned to the client

Decimation in archiving2csv
---------------------------

In archiving2csv the fandango.filter_array method is called passing the correlation parameters, in order to match
the values of the different columns with the assigned timestamps


