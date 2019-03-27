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
