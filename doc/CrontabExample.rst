Archiving Maintenance from Crontab::

  00 */6 * * * archiving_report_update
  00 00 * * * cleanTdbFiles --no-prompt
  00 01 * * * TdbCleaner.sh > /dev/null
  0 00 * 7 * db_repair.py --no-prompt manager manager
  23 00 * * 1 archiver_health_check.py hdb tdb email=...@... restart

