Download PyTangoArchiving from sourceforge:

   svn co https://svn.code.sf.net/p/tango-cs/code/archiving/tool/PyTangoArchiving/trunk
   
This package allows to:
* Integrate Hdb and Snap archiving with other python/PyTango tools.
* Start/Stop Archiving devices in the appropiated order.
* Increase the capabilities of configuration and diagnostic.
* Import/Export .csv and .xml files between the archiving and the database.


PyTangoArchiving dependences are PyTango, Fandango and Soleil's Archiving

All of them can be obtained from www.tango-controls.org

Examples and usage of PyTangoArchiving can be found here:

http://plone.tango-controls.org/Members/srubio/pytangoarchiving

-------------------------------------------------------------------------------

Archiving Maintenance from Crontab:

00 */6 * * * archiving_report_update
00 00 * * * cleanTdbFiles --no-prompt
00 01 * * * TdbCleaner.sh > /dev/null
0 00 * 7 * db_repair.py --no-prompt manager manager
23 00 * * 1 archiver_health_check.py hdb tdb email=...@... restart


