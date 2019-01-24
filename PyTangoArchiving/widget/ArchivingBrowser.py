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

__doc__ = """ABBA, Archiving Best Browser for Alba"""

import re,sys,os,time,traceback,threading
import PyTango
import fandango
import fandango.functional as fun
from fandango.log import tracer

import taurus
from fandango.qt import Qt,Qwt5
from taurus.qt.qtgui.plot import TaurusTrend,TaurusPlot
from taurus.qt.qtgui.panel import TaurusDevicePanel
from taurus.qt.qtgui.panel import TaurusValue
from taurus.qt.qtcore.util.emitter import SingletonWorker

try:
    # taurus 4
    from taurus.core.tango.util import tangoFormatter
    from taurus.qt.qtgui.display import TaurusLabel
    TaurusLabel.FORMAT=tangoFormatter  
except:
    try:
        # Tau / Taurus < 3.4
        from taurus.qt.qtgui.display import TaurusValueLabel as TaurusLabel
    except:
        # Taurus > 3.4
        from taurus.qt.qtgui.display import TaurusLabel

try:
    from PyTangoArchiving.widget.tree import TaurusModelChooser
except:
    traceback.print_exc()
    TaurusModelChooser = None

def get_distinct_domains(l):
    return sorted(set(str(s).upper().split('/')[0] for s in l))

def launch(script,args=[]):
    import os
    f = '%s %s &'%(script,' '.join(args))
    print 'launch(%s)'%f
    os.system(f)

from fandango.qt import QGridTable, QDictToolBar

###############################################################################
###############################################################################

class MyScrollArea(Qt.QScrollArea):
    def setChildrenPanel(self,child):
        self._childrenPanel = child
    def childrenPanel(self):
        return getattr(self,'_childrenPanel',None)
    def resizeEvent(self,event):
        Qt.QScrollArea.resizeEvent(self,event)
        if self.childrenPanel():
            w,h = self.width()-15,self.childrenPanel().height()
            #print 'AttributesPanel.ScrollArea.resize(%s,%s)'%(w,h)
            self.childrenPanel().resize(w,h)

PARENT_KLASS = QGridTable #Qt.QFrame #Qt.QWidget
class AttributesPanel(PARENT_KLASS):
    
    _domains = ['ALL EPS']+['LI','LT']+['LT%02d'%i for i in range(1,3)]+['SR%02d'%i for i in range(1,17)]
    _fes = [f for f in get_distinct_domains(fandango.get_database().get_device_exported('fe*')) if fun.matchCl('fe[0-9]',f)]
    
    LABELS = 'Label/Value Device Attribute Alias Archiving Check'.split()
    SIZES = [500, 150, 90, 90, 120, 40]
    STRETCH = [8, 4, 4, 4, 2, 1]
    
    def __init__(self,parent=None,devices=None):
        #print '~'*80
        tracer('In AttributesPanel()')
        PARENT_KLASS.__init__(self,parent)
        self.setSizePolicy(Qt.QSizePolicy(Qt.QSizePolicy.Ignored,Qt.QSizePolicy.Ignored))
        self.worker = SingletonWorker(parent=self,cursor=True,sleep=50.,start=True)
        #self.worker.log.setLogLevel(self.worker.log.Debug)
        self.filters=('','','') #server/device/attribute
        self.devices=devices or []
        self.setValues(None)
        self.models = []
        
        self.current_item = None
        #self.connect(self, Qt.SIGNAL('customContextMenuRequested(const QPoint&)'), self.onContextMenu)
        self.popMenu = Qt.QMenu(self)
        
        self.actions = {
            'TestDevice': self.popMenu.addAction(Qt.QIcon(),
                "Test Device",self.onTestDevice),
            'ShowDeviceInfo': self.popMenu.addAction(Qt.QIcon(),
                "Show Device Info",self.onShowInfo),
            #'ShowDevicePanel': self.popMenu.addAction(Qt.QIcon(),"Show Info",self.onShowPanel),
            'ShowArchivingInfo': self.popMenu.addAction(Qt.QIcon(),
                "Show Archiving Info",self.onShowArchivingModes),
            'AddToTrend': self.popMenu.addAction(Qt.QIcon(),
                "Add attribute to Trend", self.addAttributeToTrend),
            'AddSelected': self.popMenu.addAction(Qt.QIcon(),
                "Add selected attributes to Trend", self.addSelectedToTrend),
            'CheckAll': self.popMenu.addAction(Qt.QIcon(),
                "Select all attributes", self.checkAll),
            'UncheckAll': self.popMenu.addAction(Qt.QIcon(),
                "Deselect all attributes", self.uncheckAll),            
            
            #'Test Device': self.popMenu.addAction(Qt.QIcon(),"Test Device",self.onTestDevice)
            }
        #if hasattr(self,'setFrameStyle'):
            #self.setFrameStyle(self.Box)
        try:
            import PyTangoArchiving
            self.reader = PyTangoArchiving.Reader('*')
            #self.hreader = self.reader.configs['hdb']
            #self.treader = self.reader.configs.get('tdb',self.hreader)
        except:
            traceback.print_exc()
            
    def __del__(self):
        print 'AttributesPanel.__del__'
        QGridTable.__del__(self)
        
    def setItem(self,x,y,item,spanx=1,spany=1,align=None,model=None):
        align = align or Qt.Qt.AlignLeft
        try:
            if model:
                item._model = model
        except: pass
        self.layout().addWidget(item,x,y,spany,spanx,Qt.Qt.AlignCenter)
        if item not in self._widgets: self._widgets.append(item)            
            
    def mousePressEvent(self, event):
        point = event.pos()
        widget = Qt.QApplication.instance().widgetAt(self.mapToGlobal(point))
        if hasattr(widget,'_model'):
            print('onMouseEvent(%s)'%(getattr(widget,'text',lambda:widget)()))
            self.current_item = widget
            if event.button()==Qt.Qt.RightButton:
                self.onContextMenu(point)
        getattr(super(type(self),self),'mousePressEvent',lambda e:None)(event)
        
    def onContextMenu(self, point):
        print('onContextMenu()')
        try:
            self.actions['TestDevice'].setEnabled('/' in self.current_item._model)
            self.actions['ShowDeviceInfo'].setEnabled('/' in self.current_item._model)
            self.actions['ShowArchivingInfo'].setEnabled('/' in self.current_item._model)
            self.actions['AddToTrend'].setEnabled(hasattr(self,'trend'))
            self.actions['AddSelected'].setEnabled(hasattr(self,'trend'))
            self.popMenu.exec_(self.mapToGlobal(point))
        except:
            traceback.print_exc()
            
    def getCurrentModel(self):
         return '/'.join(str(self.current_item._model).split('/')[-4:])
        
    def getCurrentDevice(self):
         return str(self.current_item._model.rsplit('/',1)[0])
            
    def onTestDevice(self,device=None):
        from PyTangoArchiving.widget.panel import showTestDevice
        showTestDevice(device or self.getCurrentDevice())
        
    def onShowInfo(self,device=None):
        from PyTangoArchiving.widget.panel import showDeviceInfo
        showDeviceInfo(device=device or self.getCurrentDevice(),parent=self)
        
    def onShowArchivingModes(self,model=None):
        try:
          from PyTangoArchiving.widget.panel import showArchivingModes
          model = model or self.getCurrentModel()
          showArchivingModes(model,parent=self)
        except:
          Qt.QMessageBox.warning(self,"ups!",traceback.format_exc())
          
    def addAttributeToTrend(self,model=None):
        try:
          model = model or self.getCurrentModel()
          self.trend.addModels([model])
        except:
          Qt.QMessageBox.warning(self,"ups!",traceback.format_exc())        
          
    def addSelectedToTrend(self):
        try:
            y = self.columnCount()-1
            models = []
            for x in range(self.rowCount()):
                item = self.itemAt(x,y).widget()
                m = getattr(item,'_model','')
                if m and item.isChecked():
                    models.append(m)
            if len(models) > 20:
                Qt.QMessageBox.warning(self,"warning",
                    "To avoid performance issues, dynamic scale will be disabled")
                self.trend.setXDynScale(False)
            self.trend.addModels(models)
        except:
            Qt.QMessageBox.warning(self,"ups!",traceback.format_exc())
            
    def checkAll(self):
        y = self.columnCount()-1
        for x in range(self.rowCount()):
            self.itemAt(x,y).widget().setChecked(True)
            
    def uncheckAll(self):
        y = self.columnCount()-1
        for x in range(self.rowCount()):
            self.itemAt(x,y).widget().setChecked(False)            
    
    def setValues(self,values,filters=None):
        """ filters will be a tuple containing several regular expressions to match """
        #print('In AttributesPanel.setValues([%s])'%len(values or []))
        if values is None:
           self.generateTable([])
        elif True: #filters is None:
            self.generateTable(values)
        #print 'In AttributesPanel.setValues(...): done'
        return
            
    def generateTable(self,values):
        
        #thermocouples = thermocouples if thermocouples is not None else self.thermocouples
        self.setRowCount(len(values))
        self.setColumnCount(5)
        #self.vheaders = []
        self.offset = 0
        self.widgetbuffer = []
        
        for i,tc in enumerate(sorted(values)):
            #print 'setTableRow(%s,%s)'%(i,tc)
            model,device,attribute,alias,archived,ok = tc
            model,device,attribute,alias = map(str.upper,(model,device,attribute,alias))
            #self.vheaders.append(model)
            def ITEM(m,model='',size=0):
                q = fandango.qt.Draggable(Qt.QLabel)(m)
                if size is not 0:
                    q.setMinimumWidth(size) #(.7*950/5.)
                q._model = model
                q._archived = archived
                q.setDragEventCallback(lambda s=q:s._model)
                return q
            ###################################################################
            qf = Qt.QFrame()
            qf.setLayout(Qt.QGridLayout())
            qf.setMinimumWidth(self.SIZES[0])
            qf.setSizePolicy(Qt.QSizePolicy.Expanding,Qt.QSizePolicy.Fixed)
            #Order changed, it is not clear if it has to be done before or after adding TaurusValue selfect
            self.setCellWidget(i+self.offset,0,qf) 
            
            #print('Adding item: %s, %s, %s, %s, %s' % (model,device,attribute,alias,archived))
            if ok:
                tv = TaurusValue() #TaurusValueLabel()
                qf.layout().addWidget(tv,0,0)
                tv.setParent(qf)
            else:
                self.setItem(i+self.offset,0,ITEM(model,model))
                
            devlabel = ITEM(device,model,self.SIZES[1])
            self.setItem(i+self.offset,1,devlabel)
            self.setItem(i+self.offset,2,ITEM(attribute,model,self.SIZES[2]))
            self.setItem(i+self.offset,3,ITEM(alias,model,self.SIZES[3]))

            from PyTangoArchiving.widget.panel import showArchivingModes,show_history
            if archived:
              active = self.reader.is_attribute_archived(model,active=True)
              txt = '/'.join(a.upper() if a in active else a for a in archived)
            else:
              txt = '...'
            q = Qt.QPushButton(txt)
            q.setFixedWidth(self.SIZES[-2])
            q.setToolTip("""%s<br><br><pre>
              'HDB' : Archived and updated, push to export values
              'hdb' : Archiving stopped, push to export values
              '...' : Not archived
              </pre>"""%txt)
            self.connect(q, Qt.SIGNAL("pressed ()"), 
                lambda a=self.reader.get_attribute_alias(model),o=q: 
                 setattr(q,'w',show_history(a))) #showArchivingModes(a,parent=self)))
            self.setItem(i+self.offset,4,q)
            
            qc = Qt.QCheckBox()
            qc.setFixedWidth(self.SIZES[-1])
            self.setItem(i+self.offset,5,qc,1,1,Qt.Qt.AlignCenter,model)

            if ok:
                #print('Setting Model %s'%model)
                #ADDING WIDGETS IN BACKGROUND DIDN'T WORKED, I JUST CAN SET MODELS FROM THE WORKER
                try:
                    if self.worker:
                        self.worker.put([(lambda w=tv,m=model:w.setModel(m))])
                        #print 'worker,put,%s'%str(model)
                    else:
                        tv.setModel(model)
                except: 
                    print traceback.format_exc()
                self.models.append(tv)
                
            #self.widgetbuffer.extend([qf,self.itemAt(i+self.offset,1),self.itemAt(i+self.offset,2),self.itemAt(i+self.offset,3),self.itemAt(i+self.offset,4)])
            fandango.threads.Event().wait(.02)
            
        if len(values):
            def setup(o=self):
                [o.setRowHeight(i,20) for i in range(o.rowCount())]
                #o.setColumnWidth(0,350)
                o.update()
                o.repaint()
                #print o.rowCount()
                o.show()
                
            setup(self)
            
        if self.worker:
            print( '%s.next()' % (self.worker) )
            self.worker.next()
            
        #threading.Event().wait(10.)
        tracer('Out of generateTable()')
            
    def clear(self):
        try:
            #print('In AttributesPanel.clear()')
            for m in self.models: 
                m.setModel(None)
            self.models = []
            self.setValues(None)
            #QGridTable.clear(self)
            def deleteItems(layout):
                if layout is not None:
                    while layout.count():
                        item = layout.takeAt(0)
                        widget = item.widget()
                        if widget is not None:
                            widget.deleteLater()
                        else:
                            deleteItems(item.layout())
            deleteItems(self.layout())
            #l = self.layout()
            #l.deleteLater()
            #self.setLayout(Qt.QGridLayout())
        except:
            traceback.print_exc()

class ArchivingBrowser(Qt.QWidget):
    _persistent_ = None #It prevents the instances to be destroyed if not called explicitly
    MAX_DEVICES = 500
    MAX_ATTRIBUTES = 1500
    
    LABELS = AttributesPanel.LABELS
    SIZES = AttributesPanel.SIZES
    STRETCH = AttributesPanel.STRETCH
    
    def __init__(self,parent=None,domains=None,regexp='*pnv-*',USE_SCROLL=True,USE_TREND=False):
        print('%s: ArchivingBrowser()' % fun.time2str())
        Qt.QWidget.__init__(self,parent)
        self.setupUi(USE_SCROLL=USE_SCROLL, USE_TREND=USE_TREND, SHOW_OPTIONS=False)
        self.load_all_devices()
        try:
            import PyTangoArchiving
            self.reader = PyTangoArchiving.Reader('*') 
            self.archattrs = sorted(set(self.reader.get_attributes()))
            self.archdevs = list(set(a.rsplit('/',1)[0] for a in self.archattrs))
        except:
            traceback.print_exc()
            
        self.extras = []
        #self.domains = domains if domains else ['MAX','ANY','LI/LT','BO/BT']+['SR%02d'%i for i in range(1,17)]+['FE%02d'%i for i in (1,2,4,9,11,13,22,24,29,34)]
        #self.combo.addItems((['Choose...']+self.domains) if len(self.domains)>1 else self.domains)
        self.connectSignals()
        print('%s: ArchivingBrowser(): done' % fun.time2str())
        
    def load_all_devices(self,filters='*'):
        import fandango
        self.tango = fandango.get_database()
        self.alias_devs = fandango.defaultdict_fromkey(
                lambda k,s=self: str(s.tango.get_device_alias(k)))
        self.archattrs = []
        self.archdevs = []
        #print('In load_all_devices(%s)...'%str(filters))
        devs = fandango.tango.get_all_devices()
        if filters!='*': 
            devs = [d for d in devs if fandango.matchCl(
                        filters.replace(' ','*'),d,extend=True)]
        self.all_devices = devs
        self.all_domains = sorted(set(a.split('/')[0] for a in devs))
        self.all_families = sorted(set(a.split('/')[1] for a in devs))

        members = []
        for a in devs:
            try:
                members.append(a.split('/')[2])
            except:
                # Wrong names in DB? yes, they are
                pass #print '%s is an invalid name!'%a
        members = sorted(set(members))
        
        self.all_members = sorted(set(e for m in members 
                for e in re.split('[-_0-9]',m) 
                if not fandango.matchCl('^[0-9]+([ABCDE][0-9]+)?$',e)))

        #print 'Loading alias list ...'
        self.all_alias = self.tango.get_device_alias_list('*')
        #self.alias_devs =  dict((str(self.tango.get_device_alias(a)).lower(),a) for a in self.all_alias)
        tracer('Loading (%s) finished.'%(filters))
        
    def load_attributes(self,servfilter,devfilter,attrfilter,warn=True,exclude = ('dserver','tango*admin','sys*database','tmp','archiving')):
        tracer('In load_attributes(%s,%s,%s)'%(servfilter,devfilter,attrfilter))
        
        servfilter,devfilter,attrfilter = servfilter.replace(' ','*').strip(),devfilter.replace(' ','*'),attrfilter.replace(' ','*')
        attrfilter = attrfilter or 'state'
        devfilter = devfilter or attrfilter
        archive = self.dbcheck.isChecked()
        all_devs = self.all_devices if not archive else self.archdevs
        all_devs = [d for d in all_devs if not any(d.startswith(e) for e in exclude) or any(d.startswith(e) and fun.matchCl(e,devfilter) for e in exclude)]
        if servfilter.strip('.*'):
            sdevs = map(str.lower,fandango.Astor(servfilter).get_all_devices())
            all_devs = [d for d in all_devs if d in sdevs]
        #print('In load_attributes(%s,%s,%s): Searching through %d %s names'
              #%(servfilter,devfilter,attrfilter,len(all_devs),
                #'server' if servfilter else 'device'))
        if devfilter.strip().strip('.*'):
            devs = [d for d in all_devs if (fandango.searchCl(devfilter,d,extend=True))]
            print('\tFound %d devs, Checking alias ...'%(len(devs)))
            alias,alias_devs = [],[]
            if '&' in devfilter:
                alias = self.all_alias
            else:
                for df in devfilter.split('|'):
                    alias.extend(self.tango.get_device_alias_list('*%s*'%df.strip()))
            if alias: 
                print('\t%d alias found'%len(alias))
                alias_devs.extend(self.alias_devs[a] for a in alias if fun.searchCl(devfilter,a,extend=True))
                print('\t%d alias_devs found'%len(alias_devs))
                #if not self.alias_devs:
                    #self.alias_devs =  dict((str(self.tango.get_device_alias(a)).lower(),a) for a in self.all_alias)
                #devs.extend(d for d,a in self.alias_devs.items() if fandango.searchCl(devfilter,a) and (not servfilter or d in all_devs))
                devs.extend(d for d in alias_devs if not servfilter.strip('.*') or d in all_devs)
        else:
            devs = all_devs
            
        devs = sorted(set(devs))
        self.matching_devs = devs
        print('In load_attributes(%s,%s,%s): %d devices found'%(servfilter,devfilter,attrfilter,len(devs)))

        if False and not len(devs) and not archive:
            #Devices do not actually exist, but may exist in archiving ...
            #Option disabled, was mostly useless
            self.dbcheck.setChecked(True)
            return self.load_attributes(servfilter,devfilter,attrfilter,warn=False)
        
        if len(devs)>self.MAX_DEVICES and warn:
            Qt.QMessageBox.warning(self, "Warning" , "Your search (%s,%s) matches too many devices!!! (%d); please refine your search\n\n%s\n..."%(devfilter,attrfilter,len(devs),'\n'.join(devs[:30])))
            return {}
        elif warn and len(devs)>15:
            r = Qt.QMessageBox.warning(self, "Message" , "Your search (%s,%s) matches %d devices."%(devfilter,attrfilter,len(devs)),Qt.QMessageBox.Ok|Qt.QMessageBox.Cancel)
            if r==Qt.QMessageBox.Cancel:
                return {}
        
        self.matching_attributes = {} #{attribute: (device,alias,attribute,label)}
        failed_devs = []
        for d in sorted(devs):
            try:
                dp = taurus.Device(d)
                if not archive:
                    dp.ping()
                    tcs = [t for t in dp.get_attribute_list()]
                else:
                    tcs = [a.split('/')[-1] for a in self.archattrs if a.startswith(d+'/')]

                matches = [t for t in tcs if fandango.searchCl(attrfilter,t,extend=True)]

                for t in sorted(tcs):
                    if not self.dbcheck.isChecked() or not matches: 
                        label = dp.get_attribute_config(t).label
                    else: 
                        label = t
                        
                    if t in matches or fandango.searchCl(attrfilter,label,extend=True):
                        if self.archivecheck.isChecked() \
                                and not self.reader.is_attribute_archived(d+'/'+t):
                            continue
                        
                        if d in self.alias_devs: 
                            alias = self.alias_devs[d]
                        else:
                            try: alias = str(self.tango.get_alias(d))
                            except: alias = ''
                            
                        self.matching_attributes['%s/%s'%(d,t)] = (d,alias,t,label)
                        
                        if warn and len(self.matching_attributes)>self.MAX_ATTRIBUTES:
                            Qt.QMessageBox.warning(self, "Warning" , 
                                "Your search (%s,%s) matches too many attributes!!! (%d); please refine your search\n\n%s\n..."%(
                                devfilter,attrfilter,len(self.matching_attributes),'\n'.join(sorted(self.matching_attributes.keys())[:30])))
                                
                            return {}
            except:
                print('load_attributes(%s,%s,%s => %s) failed!'%(servfilter,devfilter,attrfilter,d))
                failed_devs.append(d)
                if attrfilter in ('state','','*','**'):
                    self.matching_attributes[d+'/state'] = (d,d,'state',None) #A None label means device-not-readable
                    
        if warn and len(self.matching_attributes)>30:
            r = Qt.QMessageBox.warning(self, "Message" , "(%s) matches %d attributes."%(attrfilter,len(self.matching_attributes)),Qt.QMessageBox.Ok|Qt.QMessageBox.Cancel)
            if r==Qt.QMessageBox.Cancel:
                return {}
        if not len(self.matching_attributes):
            Qt.QMessageBox.warning(self, "Warning", "No matching attribute has been found in %s." % ('Archiving DB' if archive else 'Tango DB (try Archiving option)'))
        if failed_devs:
            print('\t%d failed devs!!!: %s'%(len(failed_devs),failed_devs))
            if warn:
                Qt.QMessageBox.warning(self, "Warning" , 
                    "%d devices were not running:\n"%len(failed_devs) +'\n'.join(failed_devs[:10]+(['...'] if len(failed_devs)>10 else []) ))
        
        tracer('\t%d attributes found'%len(self.matching_attributes))
        return self.matching_attributes
        
    def setupUi(self,USE_SCROLL=False, SHOW_OPTIONS=False, USE_TREND=False):
        self.setWindowTitle('Tango Finder : Search Attributes and Archiving')
        self.setLayout(Qt.QVBoxLayout())
        self.setMinimumWidth(950)#550)
        #self.setMinimumHeight(700)
        
        self.layout().setAlignment(Qt.Qt.AlignTop)
        self.browser = Qt.QFrame()
        self.browser.setLayout(Qt.QVBoxLayout())
        
        self.chooser = Qt.QTabWidget()
        self.chooser.setTabPosition(self.chooser.West if SHOW_OPTIONS else self.chooser.North)
        #self.combo = Qt.QComboBox() # Combo used for domains, currently disabled

        self.searchbar = Qt.QFrame()
        self.searchbar.setLayout(Qt.QGridLayout()) 

        #self.label = Qt.QLabel('Type a part of device name and a part of attribute name, use "*" or " " as wildcards:')
        #self.layout().addWidget(self.label)
        
        self.ServerFilter = Qt.QLineEdit()
        self.ServerFilter.setMaximumWidth(250)
        self.DeviceFilter = fandango.qt.Dropable(Qt.QLineEdit)()
        self.DeviceFilter.setSupportedMimeTypes(fandango.qt.TAURUS_DEV_MIME_TYPE)
        self.AttributeFilter = fandango.qt.Dropable(Qt.QLineEdit)()
        self.AttributeFilter.setSupportedMimeTypes([fandango.qt.TAURUS_ATTR_MIME_TYPE,fandango.qt.TEXT_MIME_TYPE])
        self.update = Qt.QPushButton('Update')
        self.archivecheck = Qt.QCheckBox("Only archived")
        self.archivecheck.setChecked(False)
        self.dbcheck = Qt.QCheckBox("DB cache")
        self.dbcheck.setChecked(False)

        self.searchbar.layout().addWidget(Qt.QLabel(
            'Enter Device and Attribute filters using wildcards '
            '(e.g. li/ct/plc[0-9]+ / ^stat*$ & !status ) and push Update'),0,0,3,13)
        
        [self.searchbar.layout().addWidget(o,x,y,h,w) for o,x,y,h,w in (
            (Qt.QLabel("Device or Alias:"),4,0,1,1),(self.DeviceFilter,4,1,1,4),
            (Qt.QLabel("Attribute:"),4,5,1,1),(self.AttributeFilter,4,6,1,4),
            (self.update,4,10,1,1),(self.archivecheck,4,11,1,1),
            (self.dbcheck,4,12,1,1),
            )]
        
        if SHOW_OPTIONS:
            self.options = Qt.QWidget() #self.searchbar
            self.options.setLayout(Qt.QGridLayout())
            separator = lambda x:Qt.QLabel(' '*x)
            row = 1
            [self.options.layout().addWidget(o,x,y,h,w) for o,x,y,h,w in (
                #separator(120),Qt.QLabel("Options: "),separator(5),
                (Qt.QLabel("Server: "),row,0,1,1),(self.ServerFilter,row,1,1,4),(Qt.QLabel(''),row,2,1,11)
                )]
            #self.panel = generate_table(load_all_thermocouples('SR14')[-1])
            self.optiontab = Qt.QTabWidget()
            self.optiontab.addTab(self.searchbar,'Filters')
            self.optiontab.addTab(self.options,'Options')
            self.optiontab.setMaximumHeight(100)
            self.optiontab.setTabPosition(self.optiontab.North)
            self.browser.layout().addWidget(self.optiontab)
            
        else: 
            self.browser.layout().addWidget(self.searchbar)
            
        self.toppan = Qt.QWidget(self)
        self.toppan.setLayout(Qt.QVBoxLayout())

        if USE_SCROLL:
            print '*'*30 + ' USE_SCROLL=True '+'*'*30
            ## TO USE SCROLL, HEADER HAS BEEN SET AS A SEPARATE WIDGET
            #self.header = QGridTable(self.toppan)
            #self.header.setHorizontalHeaderLabels(self.LABELS)
            #self.header.setColumnWidth(0,350)
            self.headers = []
            self.header = Qt.QWidget(self.toppan)
            self.header.setLayout(Qt.QHBoxLayout())
            for l,s in zip(self.LABELS,self.SIZES):
                ql = Qt.QLabel(l)
                self.headers.append(ql)
                #if s is not None:
                    #ql.setFixedWidth(s)
                #else:
                    #ql.setSizePolicy(Qt.QSizePolicy.MinimumExpanding,Qt.QSizePolicy.Fixed)
                self.header.layout().addWidget(ql)
                
            self.toppan.layout().addWidget(self.header)            
            
            self._scroll = MyScrollArea(self.toppan)#Qt.QScrollArea(self)
            self._background = AttributesPanel(self._scroll) #At least a panel should be kept (never deleted) in background to not crash the worker!
            self.panel = None
            self._scroll.setChildrenPanel(self.panel)
            self._scroll.setWidget(self.panel)
            self._scroll.setMaximumHeight(700)
            self.toppan.layout().addWidget(self._scroll)
            self.attrpanel = self._background
        else:
            self.panel = AttributesPanel(self.toppan)
            self.toppan.layout().addWidget(self.panel)
            self.attrpanel = self.panel
            
        self.toppan.layout().addWidget(Qt.QLabel('Drag any attribute from the first column into the trend or any taurus widget you want:'))
        
        self.browser.layout().addWidget(self.toppan)
        self.chooser.addTab(self.browser,'Search ...')
        
        if USE_TREND:
            self.split = Qt.QSplitter(Qt.Qt.Vertical)
            self.split.setHandleWidth(25)
            self.split.addWidget(self.chooser)
            
            from taurus.qt.qtgui.plot import TaurusTrend
            from PyTangoArchiving.widget.trend import ArchivingTrend,ArchivingTrendWidget
            self.trend = ArchivingTrendWidget() #TaurusArchivingTrend()
            self.trend.setUseArchiving(True)
            self.trend.showLegend(True)
            self.attrpanel.trend = self.trend

            if TaurusModelChooser is not None:
                self.treemodel = TaurusModelChooser(parent=self.chooser)
                self.chooser.addTab(self.treemodel,'Tree')
                self.treemodel.updateModels.connect(self.trend.addModels)
                #self.treemodel.connect(self.treemodel,Qt.SIGNAL('updateModels'),self.trend.addModels)      
            else:
                tracer('TaurusModelChooser not available!')
            
            self.split.addWidget(self.trend)
            self.layout().addWidget(self.split)
        else:
            self.layout().addWidget(self.chooser)
        type(self)._persistent_ = self
        
    def connectSignals(self):
        #self.combo.connect(self.combo, Qt.SIGNAL("currentIndexChanged (const QString&)"), self.comboIndexChanged)
        #self.connect(self.combo, Qt.SIGNAL("currentIndexChanged (const QString&)"), self.comboIndexChanged)
        self.connect(self.update, Qt.SIGNAL("pressed ()"), self.updateSearch)
        #if len(self.domains)==1: self.emit(Qt.SIGNAL("currentIndexChanged (const QString&)"),Qt.QString(self.domains[0]))
        
    def open_new_trend(self):
        from taurus.qt.qtgui.plot import TaurusTrend
        tt = TaurusTrend()
        tt.show()
        self.extras.append(tt)
        tt.setUseArchiving(True)
        tt.showLegend(True)
        return tt
    
    def resizeEvent(self,evt):
        try:
            Qt.QWidget.resizeEvent(self,evt)
            self.adjustColumns()
            #type(self)._persistent_ = None
        except:
            traceback.print_exc()
            
    def adjustColumns(self):
        try:
            if not getattr(self,'panel',None):
                return
            w = int(max((self.panel.width()+20,self.width()*0.9)))
            self.header.setMaximumWidth(w)
            for j in range(self.panel.columnCount()):
                m = 0
                for i in range(self.panel.rowCount()):
                    try:
                        w = self.panel.layout().itemAtPosition(i,j).geometry().width()
                        if w > m: m = w
                    except:
                        m = self.SIZES[j]

                #print(j,self.LABELS[j],self.SIZES[j],m)
                self.headers[j].setFixedWidth(max((m,self.SIZES[j])))
        except:
            traceback.print_exc()
        
    def closeEvent(self,evt):
        Qt.QWidget.closeEvent(self,evt)
        type(self)._persistent_ = None
    
    #def __del__(self):
        #print 'In ValvesChooser.del()'
        ##try: Qt.QWidget.__del__(self)
        ##except: pass
        #type(self)._persistent_ = None
        
    def comboIndexChanged(self,text=None):
        #print 'In comboIndexChanged(...)'
        pass
        
    def splitFilters(self,filters):
        if filters.count(',')>1: filters.replace(',',' ')
        if ',' in filters: filters = filters.split(',')
        elif ';' in filters: filters = filters.split(';')
        elif filters.count('/') in (1,3): filters = filters.rsplit('/',1)
        elif ' ' in filters: filters = filters.rsplit(' ',1)
        else: filters = [filters,'^state$'] #'*']
        return filters
        
    def setModel(self,model):
        model = str(model).strip()
        if model: self.updateSearch(model)
    
    def updateSearch(self,*filters):
        #Text argument applies only to device/attribute filter; not servers
        try:
            #print('In updateSearch(%s[%d])'%(filters,len(filters)))
            if len(filters)>2:
                filters = [' '.join(filters[:-1]),filters[-1]]
            if len(filters)==1: 
                filters = ['']+self.splitFilters(filters[0])
            elif len(filters)==2:
                filters = ['']+list(filters)
            elif len(filters)==3:
                filters = list(filters)
            else:
                filters = (self.ServerFilter,self.DeviceFilter,self.AttributeFilter)
                filters = [str(f.text()).strip() for f in filters]
            
            #Texts are rewritten to show format as it is really used
            self.ServerFilter.setText(filters[0])
            self.DeviceFilter.setText(filters[1])
            self.AttributeFilter.setText(filters[2])
            
            if not any(filters):
                Qt.QMessageBox.warning(self, "Warning" , "you must type a text to search")
                return
            if not any (f.strip('.*') for f in filters): #Empty or too wide filters not allowed
                Qt.QMessageBox.warning(self, "Warning" , "you must reduce your filtering!")
                return
            
            wildcard = '*' if not '.*' in str(filters) else '.*'
            for i,f in enumerate(filters):
                if not (f.startswith('*') or f.startswith('.')):
                    filters[i] = '^state$' if (i==2 and not f) else f #'%s%s%s'%(wildcard,f,wildcard)
                    
            if self.panel and filters==self.panel.filters:
                return
            else:
                old = self.panel
                if self.panel:
                  if hasattr(self,'_scroll'): self._scroll.setWidget(None)
                  self.panel.setParent(None)
                  self.panel = None
                if not self.panel: 
                    self.panel = AttributesPanel(self._scroll,devices=self.all_devices)
                    self.attrpanel = self.panel
                    if hasattr(self,'trend'): 
                        self.attrpanel.trend = self.trend
                else: 
                    self.panel.clear()
                if old: 
                  old.clear()
                  old.deleteLater() #Must be done after creating the new one!!
                table = [] #model,device,attribute,alias,archived,ok
                #ATTRIBUTES ARE FILTERED HERE!! <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                for k,v in self.load_attributes(*filters).items():
                    try: 
                        archived = self.reader.is_attribute_archived(k)
                    except Exception,e: 
                        print('Archiving not available!:\n %s'
                              %traceback.format_exc())
                        archived = []
                    #print(k,v,archived)
                    table.append((k,v[0],v[2],v[1],archived,v[3] is not None))
                    
                self.panel.setValues(sorted(table))
                
                if hasattr(self,'_scroll'): 
                    self._scroll.setWidget(self.panel)
                    #self.panel.setParent(self._scroll) #IT DOESNT WORK
                    self._scroll.setChildrenPanel(self.panel)
                    
            #print('labels/columns: %d,%d' % (len(self.SIZES),self.panel.columnCount()))
            for j in range(self.panel.columnCount()):
                #print(j)
                l, s = self.LABELS[j], self.SIZES[j]
                #print('Resizing %s cells to %s' % (l,s))
                self.panel.layout().setColumnStretch(j,self.STRETCH[j])
                #for i in range(self.panel.rowCount()):
                    #try:
                        #w = self.panel.itemAt(i,j).widget()
                        #if s is not None:
                            #w.setFixedWidth(s)
                        #else:
                            #p = Qt.QSizePolicy(Qt.QSizePolicy.Expanding,Qt.QSizePolicy.Fixed)
                            #w.setSizePolicy(p)
                    #except:
                        #traceback.print_exc()
                        
            self.adjustColumns()
                    
        except Exception,e:
            #traceback.print_exc()
            Qt.QMessageBox.warning(self, "Warning" , "There's something wrong in your search (%s), please simplify the string"%traceback.format_exc())
        return
                
ModelSearchWidget = ArchivingBrowser

def main(args=None):
    """
    --range=YYYY/MM/DD_HH:mm,XXh
    """
    import sys
    
    opts = dict(a.split('=',1) for a in args if a.startswith('-'))
    print(opts)
    args = [a for a in args if not a.startswith('-')]
    print(args)    
    
    #from taurus.qt.qtgui.container import TaurusMainWindow
    tmw = Qt.QMainWindow() #TaurusMainWindow()
    tmw.setWindowTitle('Tango Attribute Search (%s)'%(os.getenv('TANGO_HOST')))
    table = ArchivingBrowser(domains=args,USE_SCROLL=True,USE_TREND=True)
    tmw.setCentralWidget(table)
    
    use_toolbar = True
    if use_toolbar:
        toolbar = QDictToolBar(tmw)
        toolbar.set_toolbar([
            ##('PDFs','icon-all.gif',[
                #('Pdf Q1','icon-all.gif',lambda:launch('%s %s'%('kpdf','TC_Q1.pdf'))),
                #('Pdf Q2','icon-all.gif',lambda:launch('%s %s'%('kpdf','TC_Q2.pdf'))),
                #('Pdf Q3','icon-all.gif',lambda:launch('%s %s'%('kpdf','TC_Q3.pdf'))),
                #('Pdf Q4','icon-all.gif',lambda:launch('%s %s'%('kpdf','TC_Q4.pdf'))),
            ##    ]),
            #('Archiving Viewer','Mambo-icon.ico', lambda:launch('mambo')),
            ('Show New Trend','qwtplot.png',table.open_new_trend),
            ])
        toolbar.add_to_main_window(tmw,where=Qt.Qt.BottomToolBarArea)
    tmw.show()

    if args: 
        table.updateSearch(*args)

    if '--range' in opts:
        tracer('Setting trend range to %s' % opts['--range'])
        table.trend.applyNewDates(opts['--range'].replace('_',' ').split(','))

    return tmw
    
if __name__ == "__main__":
    import sys
    if 'qapp' not in locals() and 'qapp' not in globals():
        qapp = Qt.QApplication([])
    import taurus
    taurus.setLogLevel('WARNING')
    t = main(args = sys.argv[1:])
    sys.exit(qapp.exec_())
