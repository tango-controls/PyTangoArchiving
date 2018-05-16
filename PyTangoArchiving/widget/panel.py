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

## author: srubio@cells.es, 2015

import PyTango
import re,sys,os,time,traceback,threading
import fandango,fandango as F
import fandango.qt as fqt
from fandango.qt import Qt
import fandango.functional as fun
from threading import Thread
import PyTangoArchiving as pta
from PyTangoArchiving.widget.history import show_history
from PyTangoArchiving.common import modes_to_string

def showTestDevice(device=None):
    import os
    os.system('tg_devtest %s&'%(device))

def showDeviceInfo(device,parent=None):
    device = device or str(self.current_item._model.rsplit('/',1)[0])
    info = F.tango.get_device_info(device).items()
    Qt.QMessageBox.warning(parent, "%s Info"%device , '\n'.join('%s : %s'%i for i in info))
    
def showArchivingModes(model,parent=None,schemas=('HDB','TDB')):
    print('onShowArchivingModes(%s)'%model)
    #DIALOG MUST BE ALWAYS SHOWN, EVEN FOR NON-ARCHIVED ATTRIBUTES!
    #w = Qt.QWidget() 
    w = Qt.QDialog()
    w.setModal(False)
    w.setLayout(Qt.QVBoxLayout())
    w.setWindowTitle(model)
    ws = {}
    for s in schemas:
     ws[s] = QArchivingMode(w)
     ws[s].setSchema(s)
     ws[s].setModel(model)
     w.layout().addWidget(ws[s])
    w.exec_()
    #w.show()
    return w

class QArchivingMode(fqt.Dropable(Qt.QFrame)):
  __help__ = """
  <strong>Archiving Types</strong>&nbsp;
  <ul><li><strong>HDB (historic)</strong>: This mode of archiving will keep the information stored 
  forever, the archiving periodicity is limited to not faster than 10 seconds.</li>
  <li><strong>TDB (temporary)</strong>: The information is stored in round-buffer and will be kept 
  only for few days, this database will allow a periodicity not faster than 1 second.</li></ul>
  <p>In general the attributes should be archived in HDB or&nbsp; TDB, not in both. But this option is 
  possible in case some attributes would be needed with more precission during operation/injection.
  It is strongly adviced to <strong>not use historic database for debugging</strong> of device servers, 
  and avoid redundancy.</p>
  &nbsp;
  <p><strong>periodic, absolute, relative modes: </strong></p>
  <p>Each archiving mode is defined by the period at which the value of the attribute is checked. 
  If the conditions of the archiving mode are matched the attribute value will be archived.</p>  
  <ul><li>a <strong>periodic</strong> mode means archiving at a fixed period; periodic mode is always 
  enabled by default at 300 seconds<br /></li>
  <li><strong>absolute</strong> and <strong>relative</strong>
  modes specify an upper and lower bound of change between one measure and the next one:</li>
  <ul><li><strong>absolute</strong> mode will trigger archiving if:&nbsp; new_measure <strong>not in</strong> 
  [old_measure-LB,old_measure+UB]</li>
  <li><strong>relative</strong> (<strong>%</strong>) mode will trigger archiving if:&nbsp; new_measure 
  <strong>not in</strong> 
  [old_measure*(1-LBe-2),old_measure*(1+UBe-2)]</li></ul>
  <p>&nbsp;</p>
  """
  
  VALID_SCHEMAS = 'hdb','tdb','snap','h++','cass'
  _logger = None
  _worker = None
  _threads = []
 
  def __init__(self,parent=None,multirow=True):
    Qt.QFrame.__init__(self,parent)
    self.setToolTip(self.__help__)
    self.setLayout(Qt.QGridLayout())
    self.api = None
    self.reader = None
    self.schema = Qt.QLabel('SCHEMA:')
    self.attribute = Qt.QLabel('at/tri/bu/te')
    self.start = Qt.QPushButton('Start')
    self.stop = Qt.QPushButton('Stop')
    self.export = Qt.QPushButton('Export')
    self.archiver = fqt.MenuContexted(Qt.QLabel)('archiver(ID)')
    multirow = 3 if multirow else 0
    self.setTable(multirow)
    length = 6
    self.layout().addWidget(self.schema,0,0,1,1)
    self.layout().addWidget(self.attribute,0,1,1,int(length/2))
    self.layout().addWidget(self.table,1,0,multirow or 1,length)
    self.layout().addWidget(self.archiver,multirow+2,0,1,2)
    self.layout().addWidget(self.export,multirow+2,length-3,1,1)
    self.layout().addWidget(self.start,multirow+2,length-2,1,1)
    self.layout().addWidget(self.stop,multirow+2,length-1,1,1)
    self.setFrameStyle(self.Panel)
    self.checkDropSupport()
    self.setDropEventCallback(self.setModel)
    self.setModes()
    self.connect(self.start,Qt.SIGNAL('pressed()'),self.applyModes)
    self.connect(self.stop,Qt.SIGNAL('pressed()'),self.resetModes)
    self.connect(self.export,Qt.SIGNAL('pressed()'),
               lambda s=self:show_history(s.getModel(),{'hdb':'hdb'}.get(s.getSchema(),'*'),parent=s))
    self.connect(self,Qt.SIGNAL('archive'),self.startArchiving)
    self.connect(self,Qt.SIGNAL('update'),self.setModes)
    #self.setLineWidth(1)
    
  def logger(self):
    if not QArchivingMode._logger:
      QArchivingMode._logger = QLoggerDialog(parent=self,remote=True)
    return QArchivingMode._logger
   
  def threads(self):
    return QArchivingMode._threads
   
  def setTable(self,multirow=None):
    if multirow is not None:
      self.multirow = multirow
    self.table = Qt.QTableWidget()
    self.header = self.table.horizontalHeader()
    try:
        self.table.verticalHeader().setResizeMode(self.header.Stretch)
        self.header.setResizeMode(self.header.Stretch)
    except:
        pass
    if self.multirow:
      self.table.setColumnCount(3)
      self.table.setRowCount(3)
      self.table.setHorizontalHeaderLabels(['Period','Range-','Range+'])
      self.table.setVerticalHeaderLabels(['Periodic','Relative','Absolute'])
    else:
      self.table.setMaximumHeight(60)
      self.table.setColumnCount(6)
      self.table.setRowCount(1)
      self.table.verticalHeader().hide()
      self.table.setHorizontalHeaderLabels(['Periodic','Abs/Rel','Polling','Range-','Range+','Select'])
      self.qb = Qt.QCheckBox()
      self.table.setCellWidget(0,5,self.qb)
      
  def setSchema(self,schema):
    schema = schema.lower()
    print('setSchema(%s)'%schema)
    self.reader = pta.Reader(schema)
    self.schema.setText('<b>%s</b>'%schema.upper())
    
  def getSchema(self):
    sch = fun.rtf2plain(str(self.schema.text())).lower()
    if sch not in self.VALID_SCHEMAS:
      sch = 'hdb'
      self.setSchema(sch)
    return sch
   
  def getReader(self):
    if not self.reader:
      self.reader = pta.Reader()
    return self.reader
    
  def setModel(self,model):
    model = self.getReader().get_attribute_alias(model)
    model = re.sub('\[([0-9]+)\]','',model.lower())
    print('QArchivingMode.setModel(%s@%s)'%(model,self.getSchema()))
    self.attribute.setText(model)
    Qt.QApplication.instance().setOverrideCursor(Qt.QCursor(Qt.Qt.WaitCursor))
    try:
      if self.reader and self.reader.is_attribute_archived(model):
        self.setModes(self.reader.get_attribute_modes(model,force=True))
      else:
        self.setModes()
    except: traceback.print_exc()
    Qt.QApplication.instance().restoreOverrideCursor()
      
  def getModel(self):
    return str(self.attribute.text())
   
  def setModes(self,modes={},expected={}):
    """ modes is a dictionary like {'MODE_P':[period]} """
    print(type(self).__name__,'setModes',self.getModel(),modes)
    if modes_to_string(expected) == modes_to_string(modes):
      self.logger().hide()    
    for i,m in enumerate(('MODE_P','MODE_R','MODE_A')):
      for j in range(3):
        t = self.table.item(i,j)
        if not t: 
          t = Qt.QTableWidgetItem('')
          self.table.setItem(i,j,t)
        if m in modes and j<len(modes[m]):
          t.setText(str(modes[m][j]))
          #print((i,j,modes[m][j]))
        else:
          t.setText('')
    if 'archiver' in modes:
      try:
        db = self.reader.get_database()
        attr_id = modes.get('ID')
        date = db.get_table_updates(db.get_table_name(attr_id)).values()[0]
        if date<time.time()-600: date = '<b>%s</b>' % fun.time2str(date)
        else: date = fun.time2str(date)
        dev = modes['archiver']
        self.archiver.setText('%s(%s,%s)'%(dev,attr_id,date))
        self.archiver.setContextCallbacks({
          'Test Device':fun.partial(showTestDevice,device=dev),
          'Show Info':fun.partial(showDeviceInfo,device=dev),
          'Show Logs':self.logger().show,
          'Refresh':fun.partial(self.setModel,model=self.getModel()),
          })
      except:
        traceback.print_exc()
    else:
       self.archiver.setText('...')
       self.archiver.setContextCallbacks({})
    Qt.QApplication.instance().restoreOverrideCursor()
    return
          
  def getItem(self,i,j):
    t = self.table.item(i,j)
    try:
      return float(str(t.text()).strip() or '0')
    except:
      return 0
          
  def getModes(self):
    """ return table values like {'MODE_P':[period]} """
    modes = {}
    values = [[self.getItem(i,j) for j in range(3)] for i in range(3)]
    self.logger().info(str('%s(%s).getModes(%s), checking ...'%(type(self).__name__,self.getModel(),values)))
    if values[0][0]:
      modes['MODE_P'] = [values[0][0]]
    if values[1][0]:
      modes['MODE_R'] = values[1]
    if values[2][0]:
      modes['MODE_A'] = values[2]
    if modes:
      Qt.QApplication.instance().setOverrideCursor(Qt.QCursor(Qt.Qt.WaitCursor))
      modes = pta.ArchivingAPI.check_modes(self.getSchema(),modes)
      Qt.QApplication.instance().restoreOverrideCursor()
    return modes
  
  def applyModes(self):
    self.logger().show()
    #Qt.QApplication.instance().setOverrideCursor(Qt.QCursor(Qt.Qt.WaitCursor))
    try:
      attr = self.getModel()
      v = F.check_attribute(attr,brief=True)
      if isinstance(v,(type(None),Exception)): 
        Qt.QMessageBox.warning(self,"Warning","%s is not readable nor archivable"%attr)
        self.logger().hide()
        return
      if fun.isSequence(v) or fun.isString(v):
        Qt.QMessageBox.warning(self,"Warning","%s array type is not supported"%attr)
        self.logger().hide()
        return
      modes = self.getModes() or {'MODE_P':[60000]}
      schema = self.getSchema()
      print('%s: applyModes(%s)'%(fun.time2str(),modes))
      msg = 'Modes to be applied:\n'
      for m,v in modes.items():
       msg += '\t%s.%s: %s\n'%(schema,m,v)
      qm = Qt.QMessageBox(Qt.QMessageBox.Warning,'Confirmation',msg,Qt.QMessageBox.Ok|Qt.QMessageBox.Cancel)
      r = qm.exec_()
      if r == Qt.QMessageBox.Ok:
        if not self.api: 
          self.api = pta.api(self.getSchema().lower(),logger=self.logger())
        self.api.log = self.logger()
        #self.emit(Qt.SIGNAL('archive'),attr,modes)
        Qt.QApplication.instance().setOverrideCursor(Qt.QCursor(Qt.Qt.WaitCursor))
        thr = threading.Thread(target=self.startArchiving,args=(attr,modes))
        QLoggerDialog._threads = filter(Thread.is_alive,self.threads())+[thr]
        thr.start()
      else:
        self.logger().hide()
    except: 
      self.logger().error(traceback.print_exc())
    print('%s: applyModes(...): running!'%(fun.time2str()))
    #Qt.QApplication.instance().restoreOverrideCursor()
    
  def resetModes(self):
    self.logger().show()
    try:
      attr = self.getModel()
      schema = self.getSchema()
      print('%s: resetModes(%s)'%(fun.time2str(),attr))
      qm = Qt.QMessageBox(Qt.QMessageBox.Warning,'Confirmation',
           '%s archiving will be stopped'%attr,
           Qt.QMessageBox.Ok|Qt.QMessageBox.Cancel)
      r = qm.exec_()
      if r == Qt.QMessageBox.Ok:
        if not self.api: 
          self.api = pta.api(self.getSchema().lower(),logger=self.logger())
        self.api.log = self.logger()
        #self.emit(Qt.SIGNAL('archive'),attr,modes)
        Qt.QApplication.instance().setOverrideCursor(Qt.QCursor(Qt.Qt.WaitCursor))
        thr = threading.Thread(target=self.startArchiving,args=(attr,{}))
        QLoggerDialog._threads = filter(Thread.is_alive,self.threads())+[thr]
        thr.start()
      else:
        self.logger().hide()
    except: 
      traceback.print_exc()
    print('%s: resetModes(...): running!'%(fun.time2str()))
    
  def startArchiving(self,attr,modes):
    try:
      print('%s: startArchiving(%s)'%(fun.time2str(),attr))
      #self.api.load_attribute_modes([attr])
      if modes:
        self.api.start_archiving(attr,modes)
      else:
        self.api.stop_archiving(attr)
      news = self.reader.get_attribute_modes(attr,force=True)
      self.emit(Qt.SIGNAL('update'),news,modes)
    except:
      self.logger().error(traceback.format_exc())
    print('%s: startArchiving(%s): done!'%(fun.time2str(),attr))
    
  @staticmethod
  def _test(schema = None, model = None, multirow = None):
    print('_test',schema,model,multirow)
    if multirow is None:
      w = QArchivingMode()
    else:
      w = QArchivingMode(multirow=F.str2bool(multirow))
    if schema:
      w.setSchema(schema)
    if model:
      w.setModel(model)
    w.show()
    return w    
    
class QLoggerDialog(fqt.QTextBuffer):
 
  def __init__(self,title='Logs',parent=None,filters=['*','!DEBUG'],remote=True): #['*','!DEBUG']):
    fqt.QTextBuffer.__init__(self,title=title,parent=parent)
    self.logger = None
    self.log_objs = {}
    self.last_args = {}
    self.last_msg = ''
    self.loglevel = 'INFO'
    self.filters = filters
    self.remote = remote
    self.connect(self,Qt.SIGNAL('logging'),self.log)
    print('QLoggerDialog(%s)'%title)
    
  def dialog(self):
    return self #self._dialog

  def log(self,severity,msg,remote=None):
    """
    The remote flag should allow operations done outside of QMainThread to be logged
    """
    remote = remote if remote is not None else self.remote
    if remote:
      self.emit(Qt.SIGNAL('logging'),severity,msg,False)
      return
    if msg == self.last_msg: 
        msg = '+1'
    else: 
        self.last_msg = msg
        if self.logger:
            try:
                if severity not in self.log_objs: self.log_objs[severity] = \
                    getattr(self.logger,severity,(lambda m,s=severity:'%s:%s: %s'%(s.upper(),F.time2str(),m)))
                self.log_objs[severity](msg)
            except: pass
    if self.dialog():
        if msg!='+1': 
            msg = '%s:%s: %s'%(severity.upper(),F.time2str(),msg)
        if self.filters:
            msg = (F.filtersmart(msg,self.filters) or [''])[0]
        if msg:
            self.dialog().append(msg)
              
  def setLogLevel(self,level): self.loglevel = level
  def getLogLevel(self): return self.loglevel
  def trace(self,msg): self.log('trace',msg)
  def debug(self,msg): self.log('debug',msg)
  def info(self,msg): self.log('info',msg)
  def warning(self,msg): self.log('warning',msg)
  def alarm(self,msg): self.log('alarm',msg)
  def error(self,msg): self.log('error',msg)    
   
if __name__ == '__main__':
  import sys
  app = fqt.getApplication()
  m = Qt.QWidget()
  m.setLayout(Qt.QVBoxLayout())
  w = QArchivingMode._test(*sys.argv[1:])
  m.layout().addWidget(Qt.QLabel(str(sys.argv)))
  m.layout().addWidget(w)
  m.show()
  app.exec_()
