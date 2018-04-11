========================
PyTangoArchiving.Schemas
========================

Declaring Schemas in the database
---------------------------------

Accessing schemas from ipython
------------------------------

Accessing schemas from Reader
-----------------------------

.. :

  import PyTangoArchiving as pta
  rd = pta.Reader()
  devs = 'test/acc/ps-clic-01','test/acc/ps-clic-02'
  attrs = [a for a in rd.get_attributes() for d in devs if a.startswith(d+'/')]

  import fandango as fn
  fn.kmap(rd.is_attribute_archived,attrs)
  
    [('test/acc/ps-clic-01/current', ('hdbpp',)),
     ('test/acc/ps-clic-01/polarity', ('hdbpp',)),
     ('test/acc/ps-clic-01/state', ('hdbpp',)),
     ('test/acc/ps-clic-01/voltage', ('hdbpp',)),
     ('test/acc/ps-clic-02/current', ('hdbpp',)),
     ('test/acc/ps-clic-02/polarity', ('hdbpp',)),
     ('test/acc/ps-clic-02/state', ('hdbpp',)),
     ('test/acc/ps-clic-02/voltage', ('hdbpp',))
    ]

  s0 = fn.now()-90*86400

  vals = rd.get_attributes_values(attrs,s0)
  [(k,len(v)) for k,v in vals.items()]
  
    [('test/acc/ps-clic-02/voltage', 46610),
     ('test/acc/ps-clic-02/state', 87),
     ('test/acc/ps-clic-01/state', 754),
     ('test/acc/ps-clic-01/polarity', 14105),
     ('test/acc/ps-clic-02/current', 48849),
     ('test/acc/ps-clic-01/current', 49299),
     ('test/acc/ps-clic-02/polarity', 14136),
     ('test/acc/ps-clic-01/voltage', 45451)
    ]
