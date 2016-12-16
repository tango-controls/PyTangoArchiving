import PyTangoArchiving,traceback,time,re,fandango

class EvalReader(PyTangoArchiving.Reader):
  def get_attribute_values(self,attribute,start_date,stop_date=None,asHistoryBuffer=False,N=0):
    print 'get_attribute_values(%s)'%attribute
    is_eval = expandEvalAttribute(attribute)
    if stop_date is None: stop_date=time.time()
    if is_eval:
      getId = lambda s: s.strip('{}').replace('/','_').replace('-','_')
      attribute = attribute.replace('eval://','')
      attributes = is_eval
      for a in attributes:
       attribute = attribute.replace(a,' %s '%getId(a))
      resolution = max((1,(stop_date-start_date)/(10*1080)))
      vals = dict((k,fandango.arrays.filter_array(v,window=resolution)) for k,v in self.get_attributes_values([a.strip('{}') for a in attributes],start_date,stop_date).items())
      cvals = self.correlate_values(vals,resolution=resolution,rule=(lambda t1,t2,tt:t2))
      nvals = []
      for i,t in enumerate(cvals.values()[0]):
       try:
        vars = dict((getId(k),v[i][1]) for k,v in cvals.items())
        if None in vars.values():
         v= None
        else:
         v = eval(attribute,vars)
       except:
        traceback.print_exc()
        v = None
       nvals.append((t[0],v))
      return nvals
    else:
     return PyTangoArchiving.Reader.get_attribute_values(self,attribute,start_date,stop_date)
     
     
 #eval://{sr/di/dcct/averagecurrent}*{fe04/vc/vgct-01/state}+2