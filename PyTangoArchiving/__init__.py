#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################################
## This file is part of Tango Control System:  http://www.tango-controls.org/
##
## $Author: Sergi Rubio Manrique, srubio@cells.es
## copyleft :    ALBA Synchrotron Controls Section, www.cells.es
##
## Tango Control System is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as published
## by the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## Tango Control System is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""
MODULE FOR TANGO ARCHIVING MANAGEMENT
by Sergi Rubio Manrique, srubio@cells.es
ALBA Synchrotron Control Group
16 June 2007

@mainpage 

<h1 id="PyTangoArchivingModule">PyTangoArchiving Module</h1>
<p>This package allowed to:</p>
<ul><li>Integrate Hdb and Snap archiving with other python/PyTango tools.
</li></ul><ul><li>Start/Stop Archiving devices in the appropiated order.
</li></ul><ul><li>Increase the capabilities of configuration and diagnostic.
</li></ul><ul><li>Import/Export .csv and .xml files between the archiving and 
    the database.

"""

RELEASE = (6,4,0)

ARCHIVING_TYPES = ['hdb','tdb','snap']
ARCHIVING_CLASSES =     ['HdbArchiver','TdbArchiver','SnapArchiver',
                    'HdbExtractor','TdbExtractor','SnapExtractor',
                    'ArchivingManager','SnapManager',
                    #'HdbArchivingWatcher','TdbArchivingWatcher',
                    ]
MAX_SERVERS_FOR_CLASS=5
MIN_ARCHIVING_PERIOD=10

import fandango as fn

import utils
import dbs
import common
#import reader ; should be loaded the last
import archiving
import files

from common import CommonAPI
from common import getSingletonAPI as getCommonAPI
from schemas import Schemas
from archiving import ArchivingAPI,ArchivedAttribute
from utils import check_attribute
from check import check_archiving_schema
from files import GetConfigFiles,LoadArchivingConfiguration,\
    CheckArchivingConfiguration,ParseCSV,StopArchivingConfiguration

__all__=['reader','archiving','utils','files','common']

try:
    import snap
    from snap import SnapDB,SnapAPI
    __all__.extend(('snap',))#'SnapDB','SnapAPI'))
except:
    print('Unable to import snap')

try:
   import hdbpp
   from hdbpp import HDBpp, multi
   from hdbpp.multi import start_archiving_for_attributes, \
       get_last_values_for_attributes
  
except:
   print('Unable to import hdbpp') 

api = Schemas.getApi #ArchivingAPI

#Order matters!, it should be the last import
import reader
from reader import Reader,getArchivedTrendValues

"""
Some interesting queries;

select ID,full_name from adt;
select ID,archiver,start_date,stop_date from amt;
select ID,full_name,data_type,writable from adt;
select full_name,archiver,start_date,stop_date from adt,amt 
    where adt.ID=amt.ID order by full_name;
select adt.ID,full_name,archiver,start_date,stop_date from adt,amt 
    where adt.ID=amt.ID order by full_name,start_date;

: db=MySQLdb.connect(db='hdb',host='alba02',passwd='browser',user='browser')

In [135]: q.cursor()

Methods from hdbextractor:
GetHost
GetInfo
GetMaxTime
GetMembers
GetConnectionState
GetCurrentArchivedAtt
GetAttDataBetweenDates: It fails for DevState attributes!!!
GetAttDataLastN #(attribute,n): returns last n values
GetAttDataCount
GetAttCountFilterType
GetArchivingMode
GetAttCountAll
GetAttCountFilterFormat
GetAttDefinitionData : It allows to get all the fields from the adt table, 
    including the Data type
GetAttId
GetAttNameAll

from HdbArchiver
commands: 
    StateDetailed
attributes:
    image_charge
    scalar_charge
    spectrum_charge


Methods from ArchivingManager:
5 - ArchivingStartHdb

    * Description: Start an historic archiving of several attributes, with mode.
       
    * Argin:
      DEVVAR_STRINGARRAY : Archiving arguments...

              o The first part :
                    + argin[0] = the load balancing type of the archiving
                      "1", if all the attribute are archived together in the 
                      same TdbArchiver device,
                      "0" otherwise.
                    + argin[1] = the number of attributes to archive
                    + argin[2] to argin [2 + argin[1] - 1] = the name of each 
                    attribute 
              o The second part (the Mode part) :
                Let us note "index" the last index used (for example, at this 
                point, index = 2]).
                    + If the Mode is composed of a Periodical Mode
                      argin[index+ 1] = MODE_P
                      argin[index+ 2] = the period of the periodic mode in (ms)
                      index = index + 2
                    + If the Mode is composed of an Absolute Mode
                      argin[index+ 1] = MODE_A
                      argin[index+ 2] = the frequency of the absolute mode
                      in (ms)
                      argin[index+ 3] = the delta value max when decreasing
                      argin[index+ 4] = the delta value max when increasing
                      index = index + 4
                    + If the Mode is composed of a Relative Mode
                      argin[index+ 1] = MODE_R
                      argin[index+ 2] = the frequency of the relative mode
                      in (ms)
                      argin[index+ 3] = the decreasing variation associated to 
                      this mode
                      argin[index+ 4] = the increasing variation associated to 
                      this mode
                      index = index + 4
                    + If the Mode is composed of an Threshold Mode
                      argin[index+ 1] = MODE_T
                      argin[index+ 2] = the frequency of the threshold mode 
                      in (ms)
                      argin[index+ 3] = the smallest value (min) when decreasing
                      argin[index+ 4] = the biggest value (max) when increasing
                      index = index + 4
                    + If the Mode is composed of a On Calculation Mode
                      argin[index+ 1] = MODE_C
                      argin[index+ 2] = the frequency of the on calculation mode 
                      in (ms)
                      argin[index+ 3] = the number of values taken into account
                      argin[index+ 4] = the type associated to this mode
                      argin[index+ 5] = Not used at the moment
                      index = index + 5
                    + If the Mode is composed of an On Difference Mode
                      argin[index+ 1] = MODE_D
                      argin[index+ 2] = the frequency of this mode (in ms)
                      index = index + 2
                    + If the Mode is composed of an External Mode
                      argin[index+ 1] = MODE_E
                      index = index + 1

15 - GetStatusHdb

    * Description: For each attribute of the given list, get the status of 
    the device in charge of its historical archiving
       
    * Argin:
      DEVVAR_STRINGARRAY : The attribute list.
       
    * Argout:
      DEVVAR_STRINGARRAY : The list of status.
       
    * Command allowed for:
          o Tango::ON
          o Tango::RUNNING
          o Tango::INIT
          o Tango::ALARM
          o Tango::FAULT
      
13 - GetArchivingModeHdb

    * Description: Return the historical archiving mode applied to an attribute.
       
    * Argin:
      DEV_STRING : The attribute name.
       
    * Argout:
      DEVVAR_STRINGARRAY : The applied mode...

              o Let us note "index" the last index used (for example, at 
              this point, index = 0]). If the Mode is composed 
              of a Periodical Mode
                argout[index] = MODE_P
                argout[index + 1] = the period of the periodic mode in (ms)
                index = index + 2
              o If the Mode is composed of an Absolute Mode
                argout[index] = MODE_A
                argout[index+ 1] = the frequency of the absolute mode in (ms)
                argout[index+ 2] = the delta value max when decreasing
                argout[index+ 3] = the delta value max when increasing
                index = index + 4
              o If the Mode is composed of a Relative Mode
                argout[index] = MODE_R
                argout[index+ 1] = the frequency of the relative mode in (ms)
                argout[index+ 2] = the decreasing variation associated to 
                this mode
                argout[index+ 3] = the increasing variation associated to 
                this mode
                index = index + 4
              o If the Mode is composed of an Threshold Mode
                argout[index] = MODE_T
                argout[index+ 1] = the frequency of the threshold mode in (ms)
                argout[index+ 2] = the smallest value (min) when decreasing
                argout[index+ 3] = the biggest value (max) when increasing
                index = index + 4
              o If the Mode is composed of a On Calculation Mode
                argout[index] = MODE_C
                argout[index+ 1] = the frequency of the on calculation mode 
                in (ms)
                argout[index+ 2] = the number of values taken into account
                argout[index+ 3] = the type associated to this mode
                argout[index+ 4] = Not used at the moment
                index = index + 5
              o If the Mode is composed of an On Difference Mode
                argout[index] = MODE_D
                argout[index+ 1] = the frequency of this mode (in ms)
                index = index + 2
              o If the Mode is composed of an External Mode
                argout[index] = MODE_E
                index = index + 1
"""

