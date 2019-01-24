#!/usr/bin/env python

#############################################################################
##
## This file is part of Taurus, a Tango User Interface Library
## 
## http://www.tango-controls.org/static/taurus/latest/doc/html/index.html
##
## Copyright 2011 CELLS / ALBA Synchrotron, Bellaterra, Spain
## 
## Taurus is free software: you can redistribute it and/or modify
## it under the terms of the GNU Lesser General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
## 
## Taurus is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU Lesser General Public License for more details.
## 
## You should have received a copy of the GNU Lesser General Public License
## along with Taurus.  If not, see <http://www.gnu.org/licenses/>.
##
#############################################################################

import time,sys,os,re,fandango,traceback
from fandango.functional import *
import PyTangoArchiving
from fandango.qt import Qt, QOptionDialog

__all__= ['show_history']

TABS = []

"""
@TODO

This dialog hungs on exit and is very hard to debug

Do not use it outside of TaurusFinder

It should be refactored in next release

"""

tformat = '%Y-%m-%d %H:%M:%S'    
str2epoch = lambda s: time.mktime(time.strptime(s,tformat))
epoch2str = lambda f: time.strftime(tformat,time.localtime(f))

def state_formatter(value):
    try:
      n = int(float(value))
      state = fandango.tango.DevState.values[n]
      return '%s (%s)'%(value,state)
    except:
      return str(value)
    
def get_value_formatter(attribute):
    formatter = str
    try:
      #ai = fandango.tango.get_attribute_config(attribute)
      d,a = attribute.rsplit('/',1)
      ai = fandango.get_device(d).get_attribute_config(a)
      #print(attribute,':',ai)
      if ai.data_type is fandango.tango.CmdArgType.DevState:
        formatter = state_formatter
      #else:
        #print(repr(ai.datatype))
    except:
      traceback.print_exc()
    return formatter


class ShowHistoryDialog(fandango.Object):
  
  DefaultStart = 0
  DefaultEnd = 0
  
  @classmethod
  def get_default_dates(klass):
    return klass.DefaultStart or now()-3600,klass.DefaultEnd or now()
  
  @classmethod
  def set_default_dates(klass,start,end):
    klass.DefaultStart = start
    klass.DefaultEnd = end

  @staticmethod
  def setup_table(attribute,start,stop,values):
  
      print 'drawing table from %d values: %s ...' %( len(values),values[:2])
      twi = Qt.QWidget()
      twi.setLayout(Qt.QVBoxLayout())
      tab = Qt.QTableWidget()
      tab.setWindowTitle('%s: %s to %s' % (attribute,start,stop))       
      twi.setWindowTitle('%s: %s to %s' % (attribute,start,stop))       
      tab.setRowCount(len(values))
      tab.setColumnCount(3)
      tab.setHorizontalHeaderLabels(['TIME','EPOCH','VALUE'])
      
      formatter = get_value_formatter(attribute)
      
      for i,tup in enumerate(values):
          date,value = tup[:2]
          qdate = Qt.QTableWidgetItem(epoch2str(date))
          qdate.setTextAlignment(Qt.Qt.AlignRight)
          tab.setItem(i,0,qdate)
          qtime = Qt.QTableWidgetItem(str(date))
          qtime.setTextAlignment(Qt.Qt.AlignRight)
          tab.setItem(i,1,qtime)
          qvalue = Qt.QTableWidgetItem(formatter(value))
          qvalue.setTextAlignment(Qt.Qt.AlignRight)
          tab.setItem(i,2,qvalue)
          
      twi.layout().addWidget(tab)
      tab.resizeColumnsToContents()
      tab.horizontalHeader().setStretchLastSection(True)
      return twi

  @classmethod
  def show_new_dialog(klass,attribute,schema='*',parent=None,dates=[]):
      try:
          if not Qt.QApplication.instance(): Qt.QApplication([])
      except:
          pass
      print 'in Vacca.widgets.show_history(%s)'
      
      print 'getting archiving readers ...'
      from PyTangoArchiving import Reader
      rd = Reader(schema.lower())
      hdb = Reader('hdb')
      tdb = Reader('tdb')
      attribute = str(attribute).lower()
      
      try:
        dates = dates or klass.get_default_dates()
        if not all(map(fandango.isString,dates)):
          dates = map(epoch2str,dates)
      except:
          traceback.print_exc()
          dates = epoch2str(),epoch2str()

      schemas = rd.is_attribute_archived(attribute,preferent=False)
      if schemas:
          print '%s is being archived' % attribute
          di = Qt.QDialog(parent)
          wi = di #QtGui.QWidget(di)
          wi.setLayout(Qt.QGridLayout())
          begin = Qt.QLineEdit()
          begin.setText(dates[0])
          end = Qt.QLineEdit()
          end.setText(dates[1])
          tfilter = Qt.QLineEdit()
          vfilter = Qt.QCheckBox()
          wi.setWindowTitle('Show %s Archiving'%attribute)
          wil = wi.layout()
          wi.layout().addWidget(Qt.QLabel(attribute),0,0,1,2)
          wi.layout().addWidget(Qt.QLabel('Preferred Schema'),1,0,1,1)
          qschema = Qt.QComboBox()
          qschema.insertItems(0,['*']+list(schemas))
          wil.addWidget(qschema,1,1,1,1)
          #qb = Qt.QPushButton("Save as preferred schema")
          #wil.addWidget(qb,wil.rowCount(),0,1,2)
          #qi.connect(qb,Qt.SIGNAL('pushed()'),save_schema)
          wil.addWidget(Qt.QLabel('Enter Begin and End dates in %s format'%tformat),2,0,1,2)
          wil.addWidget(Qt.QLabel('Begin:'),3,0,1,1)
          wil.addWidget(begin,3,1,1,1)
          wil.addWidget(Qt.QLabel('End:'),4,0,1,1)
          wil.addWidget(end,4,1,1,1)
          wil.addWidget(Qt.QLabel('Time Filter:'),5,0,1,1)
          wil.addWidget(tfilter,5,1,1,1)
          wil.addWidget(Qt.QLabel('Value Filter:'),6,0,1,1)
          wil.addWidget(vfilter,6,1,1,1)
          buttons = Qt.QDialogButtonBox(wi)
          buttons.addButton(Qt.QPushButton("Export"),Qt.QDialogButtonBox.AcceptRole)

          bt = Qt.QPushButton("Apply")
          buttons.addButton(bt,Qt.QDialogButtonBox.ApplyRole)
          def set_schema(r=rd,a=attribute,qs=qschema):
              print 'setting schema ...'
              schema = str(qs.currentText()).strip()
              r.set_preferred_schema(a,schema)
              
          buttons.connect(bt,Qt.SIGNAL('clicked()'),set_schema)

          buttons.addButton(Qt.QPushButton("Close"),Qt.QDialogButtonBox.RejectRole)

          #  Qt.QDialogButtonBox.Apply\
          #    |Qt.QDialogButtonBox.Open|Qt.QDialogButtonBox.Close)
          wi.connect(buttons,Qt.SIGNAL('accepted()'),wi.accept)
          wi.connect(buttons,Qt.SIGNAL('rejected()'),wi.reject)
          wi.layout().addWidget(buttons,7,0,1,2)
          
          def check_values():
              di.exec_()
              print 'checking schema ...'
              schema = str(qschema.currentText()).strip()
              if schema != '*':
                    rd.set_preferred_schema(attribute,schema)              
              if di.result():
                  print 'checking result ...'
                  try:
                      start,stop = str(begin.text()),str(end.text())
                      try: tf = int(str(tfilter.text()))
                      except: tf = 0
                      vf = vfilter.isChecked()
                      if not all(re.match('[0-9]+-[0-9]+-[0-9]+ [0-9]+:[0-9]+:[0-9]+',str(s).strip()) for s in (start,stop)):
                          print 'dates are wrong ...'
                          Qt.QMessageBox.warning(None,'Show archiving', 'Dates seem not in %s format'%(tformat), Qt.QMessageBox.Ok)
                          return check_values()
                      else:
                          print 'getting values ...'
                          print 'using %s reader' % rd.schema
                          values = rd.get_attribute_values(attribute,str2epoch(start),str2epoch(stop))
                          if not len(values) and schema=='*' and hdb.is_attribute_archived(attribute,active=True):
                              print 'tdb failed, retrying with hdb'
                              values = hdb.get_attribute_values(attribute,str2epoch(start),str2epoch(stop))
                          if vf:
                              values = fandango.arrays.decimate_array(values)
                          if tf:
                              print 'Filtering %d values (1/%dT)'%(len(values),tf)
                              values = fandango.arrays.filter_array(values,window=tf) #,begin=start,end=stop)

                          twi = klass.setup_table(attribute,start,stop,values)
                          klass.set_default_dates(str2epoch(start),str2epoch(stop))
                          
                          button = Qt.QPushButton('Save to file')
                          button2 = Qt.QPushButton('Send to email')
                          
                          def save_to_file(var=attribute,data=values,parent=twi,edit=True):
                              try:
                                  options = {'sep':'\\t','arrsep': '\\ ','linesep':'\\n'}
                                  import codecs
                                  dd = QOptionDialog(model=options,title='CSV Options')
                                  dd.exec_()
                                  for k,v in options.items():
                                    options[k] = codecs.escape_decode(str(v))[0]
                                  print options
                                
                                  filename = Qt.QFileDialog.getSaveFileName(parent,
                                      'File to save',
                                      '/data/'+PyTangoArchiving.files.get_data_filename(var,data,'csv','human'),
                                      'CSV files (*.csv)')
                                  
                                  PyTangoArchiving.files.save_data_file(var,data,filename,format='csv',**options)
                                  if edit:
                                    try: os.system('gedit %s &'%filename)
                                    except: pass
                                  return filename
                              except Exception,e:
                                  Qt.QMessageBox.warning(None, "Warning" , "Unable to save %s\n:%s"%(filename,e))
                                  
                          def send_by_email(var=attribute,data=values,parent=twi):
                              #subject,text,receivers,sender='',attachments=None,trace=False):
                              try:
                                receivers,ok = Qt.QInputDialog.getText(None,'Send by email','to:')
                                if ok:
                                  filename = str(save_to_file(var,data,parent,edit=False))
                                  fandango.linos.sendmail(filename,attribute,receivers=str(receivers),attachments=[filename])
                              except Exception,e:
                                Qt.QMessageBox.warning(None, "Warning" , "Unable to send %s\n:%s"%(filename,e))
                              
                          #button.setTextAlignment(Qt.Qt.AlignCenter)
                          twi.connect(button, Qt.SIGNAL("pressed ()"), save_to_file)
                          twi.layout().addWidget(button)
                          twi.connect(button2, Qt.SIGNAL("pressed ()"), send_by_email)
                          twi.layout().addWidget(button2)                        
                          twi.show()

                          TABS.append(twi)
                          twi.connect(twi,Qt.SIGNAL('close()'),lambda o=twi: TABS.remove(o))
                          print 'show_history done ...'
                          return twi
                  except Exception,e:
                      print traceback.format_exc()
                      Qt.QMessageBox.warning(None, "Warning" , "Unable to retrieve the values (%s), sorry"%e)
              else:
                  print 'dialog closed'
                  return None
          print 'asking for dates ...'
          return check_values()
      else:
          Qt.QMessageBox.warning(None,'Show archiving', 'Attribute %s is not being archived'%attribute, Qt.QMessageBox.Ok)
          
def show_history(*args,**kwargs):
  """ This method is a wrapper for HistoryDialog.show_new_dialog """
  print('%s: PyTangoArchiving.widget.history.show_history(...)'%time2str())
  return ShowHistoryDialog.show_new_dialog(*args,**kwargs)

def __test__(args):
  w = show_history(*args)
  
if __name__ == '__main__':
  import sys,fandango.qt
  qapp = fandango.qt.getApplication()
  w = __test__(sys.argv[1:])
  sys.exit(qapp.exec_())
