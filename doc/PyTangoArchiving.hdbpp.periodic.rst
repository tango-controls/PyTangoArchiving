PyTangoArchiving.hdbpp.periodic.HDBppPeriodic
=============================================

Adding attributes to periodic archiving

The periodic archiving has been added to HDB++ using the PyHDBppPeriodicArchiver device

https://git.cells.es/controls/pyhdbppperiodicarchiver


Just calling a single method of the api, attributes will be added to the least loaded archiver::

        api = PyTangoArchiving.api('hdbrf')

        attrs = fn.find_attributes('tango://tangodb:10000/wr/rf/*thomson*/*')

        api.add_periodic_attributes(attrs, periods=3000)



If the load is already high, the API can be used to create a new archiver::

        loads = api.get_periodic_archivers_attributes()

        if all(len(v)>100 for v in loads.values()):

          api.add_periodic_archiver('PyHdbppPeriodicArchiver/xx','archiving/hdbrf/per-x')



For a full list of periodic archiving methods, see::

        get_periodic_archivers

        is_periodic_archived

        get_next_periodic_archiver

                attrexp can be used to get archivers already archiving attributes

        get_periodic_attribute_period

        get_periodic_archiver_periods

        get_periodic_attributes

        get_periodic_archivers_attributes

        add_periodic_attribute

        add_periodic_archiver

        add_periodic_attributes

                attributes must be a list, periods a number, list or dict


        get_periodic_attribute_archiver





