=====================
Snapshoting API Usage
=====================

The Snapshot Archiving System provides a reliable system to Save/Restore attribute values linked to descriptive information.
It provides a common tool for all GUI's to save their settings and setpoints, allowing the users to introduce textual comment for each stored set of values.
The PyTangoArchiving SnapAPI is build partially on top of Soleil's JAVA Api and Servers, providing new objects and methods to simplify its usage.
The creation of an SnapAPI object requires a registered user and password in the MySQL host.

::

  from PyTangoArchiving import snap
  snapapi = snap.SnapAPI('snapuser@snaphost','password')

Context Creation
----------------

SnapContext objects are easily created and inserted in the database::

  new_ctx = snapapi.create_context('Author','Name','Reason','Description',['test/sim/user/writable','test/sim/user/prova','test/sim/sergi/array'])

  SnapContext(359,Name,Author,Reason,Attributes[3],Snapshots[0])

Read contexts information
-------------------------

Several methods allow to reload contexts information from the database and retrieve the SnapContext object::

  snapapi.get_contexts()
    {1: SnapContext(1,Prova1,User1,Tests,Attributes[1],Snapshots[6]),

  ...
  359: SnapContext(359,Name,Author,Reason,Attributes[3],Snapshots[0])}

  last_ctx = snapapi.get_context(359) #It can be returned by using the context ID
  SnapContext(359,Name,Author,Reason,Attributes[3],Snapshots[0])

  last_ctx = snapapi.get_context(max(snapapi.contexts)) 
  SnapContext(359,Name,Author,Reason,Attributes[3],Snapshots[0])

  snapapi.get_contexts('Test*')
  {347: SnapContext(347,TestContext,user1@inst.es,test,Attributes[0],Snapshots[0]),

  348: SnapContext(348,TestContext2,user1@inst.es,test,Attributes[4],Snapshots[1])}

  snapapi.get_context('TestContext2')
  SnapContext(348,TestContext2,user1@inst.es,test,Attributes[4],Snapshots[1])

Get/Take attributes and snapshots
---------------------------------

SnapContext objects allow to check attributes/snapshots information and create new snapshots::

  new_ctx.get_attributes()
  {1095: {'full_name': 'Test/Sim/user/Array',
         'data_format': PyTango._PyTango.AttrDataFormat.SPECTRUM,
         ... }

   1097: {'full_name': 'Test/Sim/user/Prova',
       ... },
   ... }

   new_ctx.take_snapshot('this is just a test')

   new_ctx.get_snapshots()
   {48: [datetime.datetime(2009, 3, 5, 11, 38, 28), 'this is just a test']}

Snapshot objects
----------------

The Snapshot objects contain timestamp and comment, but also are dictionaries containing {attribute:values} pairs::

  first_snapshot = new_ctx.get_snapshot_by_date(0)

  last_snapshot = new_ctx.get_snapshot_by_date(-1)

  snapshot = new_ctx.get_snapshot(48)

  Snapshot(48,1236249508.0,this is just a test,{test/sim/user/writable,test/sim/user/prova,test/sim/sergi/array})

  print time.ctime(snapshot.time),snapshot.comment
  Thu Mar  5 11:38:28 2009 this is just a test

  snapshot['test/sim/user/writable'] #Snapshot is a dictionary containing attribute values

  (3.0, 2.0) #(read_value,write_value)
