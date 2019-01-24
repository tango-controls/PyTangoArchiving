   
PyTangoArchiving allows to:

* Integrate Hdb and Snap archiving with other python/PyTango tools.
* Start/Stop Archiving devices in the appropiated order.
* Increase the capabilities of configuration and diagnostic.
* Import/Export .csv and .xml files between the archiving and the database.

PyTangoArchiving requires a MySQL python binding installed, either python-mysql or mysqlclient

    https://pypi.org/project/mysqlclient/#description
    https://github.com/PyMySQL/mysqlclient-python
    https://mysqlclient.readthedocs.io

With Python3 only mysqlclient will be supported, the steps to install it (on Debian) are:

    sudo aptitude remove python-mysqldb
    sudo aptitude install python-pip
    sudo aptitude install libmariadbclient-dev
    sudo pip install mysqlclient

Other PyTangoArchiving dependences are PyTango, Fandango and Soleil's Archiving or HDB++.

All of them can be obtained from www.tango-controls.org

Examples and usage of PyTangoArchiving can be found here:

* https://github.com/sergirubio/PyTangoArchiving/blob/master/doc/PyTangoArchiving_UserGuide.rst
* http://tango-controls.readthedocs.io/en/latest/tutorials-and-howtos/how-tos/how-to-pytangoarchiving.html

The old tags and branches are available in sourceforge: 

   https://svn.code.sf.net/p/tango-cs/code/archiving/tool/PyTangoArchiving

-------------------------------------------------------------------------------


