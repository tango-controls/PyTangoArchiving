Commands available from the PyTango_uitls.archiving.snap.SnapAPI Class

::

  CreateContext(author,name,﻿date,reason,description,['attributes'],FORCE=False)
      Checks if already exists a context with the same attributes. If doesn't exists or FORCE==True creates a new one and returns the ID, else returns None.

  GetContextsWithAttrs(AttributeList)
       returns a list of matching IDs (int number)
   GetContextsByName('*ContextName*')
      returns a list of matching IDs (int number)

  GetContextByID(ContextID)
      returns a dict with keys (name,reason,description,date and attr_list)

   SaveSnapshot(ContextID,comment='')
  SetSnapshotComment(ContextID,date,comment)
   GetSnapshotsByContext(ContextID)
      returns a sorted list of (date,comment) tuples
  GetValuesFromSnapshot(ContextID,date)
      returns a dict {'attribute':value}
  SetValuesFromSnapshot(ContextID,date)



Example for a GUI that has 2 attributes (d/f/m/a1,d/f/m/a2):

::

  GUI_Name = 'LTB-GUI'
  attr_list = ['d/f/m/a1','d/f/m/a2']

  #TO CREATE A CONTEXT, OR LOAD AN EXISTING ONE FROM DB:
  snap = PyTango_utils.archiving.snap.SnapAPI()
  context_id = snap.GetContextsByName(GUI_Name)
  if not context_id:
      context_id = CreateContext('srubio',GUI_Name,time.ctime(),'LTB Gui','attrs of LTB',attr_list)

  #TO LOAD LAST STORED VALUES FOR ATTRIBUTES
  last_snap,last_comment = snap.GetSnapshotsByContext(context_id)[-1]

  attribute_values = snap.GetValuesFromSnapShot(context_id,last_snap)
  for attribute,value in attribute_values.items():
      print attribute,'=',value,'; recorded at ',time.ctime(last_snap),'(',last_comment,')'

  if I_want_to_force_attribute_writing:
      #Maybe you prefer to do this action after checking recorded values
      SetValuesFromSnapshot(ContextID,date)
