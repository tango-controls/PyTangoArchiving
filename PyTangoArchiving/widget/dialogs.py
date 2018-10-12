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
PyTangoArchiving.widget.trend: This module provides access from TaurusTrend to the PyTangoArchiving api
"""

import numpy
import time
import traceback
import platform
import PyTango
import fandango as fn

import PyTangoArchiving
import PyTangoArchiving.utils as utils
from PyTangoArchiving.reader import Reader,ReaderProcess

## WARNING: AVOID TO IMPORT TAURUS OR QT OUTSIDE ANY METHOD OR CLASS, BE GREEN!
#from taurus.qt.qtgui.plot import TaurusTrend

from history import show_history
from fandango.objects import BoundDecorator, Cached
from fandango.debug import Timed
from fandango.dicts import defaultdict
from fandango import SingletonMap,str2time, time2str

from fandango.qt import Qt,Qwt5,QTextBuffer,setDialogCloser,QWidgetWithLayout

import taurus
from taurus.qt.qtgui.plot import TaurusTrend
from taurus.qt.qtgui.base import TaurusBaseWidget
from taurus.qt.qtgui.container import TaurusWidget,TaurusGroupBox

USE_MULTIPROCESS=False
try:
    prop = PyTango.Database().get_class_property('HdbExtractor',
                                        ['Multiprocess'])['Multiprocess']
    if any(a in str(prop).lower() for a in ('true','1')):
        USE_MULTIPROCESS=True
    elif any(fn.matchCl(p,platform.node()) for p in prop):
        USE_MULTIPROCESS=True
except:
    print traceback.format_exc()
print 'Multiprocess:%s'%USE_MULTIPROCESS

DECIMATION_MODES = [
    #('Hide Nones',fn.arrays.notnone),
    ('Pick One',fn.arrays.pickfirst),
    ('Minimize Noise',fn.arrays.mindiff),
    ('Maximize Peaks',fn.arrays.maxdiff),
    ('Average Values',fn.arrays.average),
    ('In Client', False),
    ('RAW',None),        
    ]

def getTrendObject(trend_set):
    return trend_set if hasattr(trend_set,'axisScaleDiv') else getObjectParent(trend_set)

def getObjectParent(obj):
    return getattr(obj,'_parent',None) or obj.parent()
        
def getTrendBounds(trend_set,rough=False):
    if isinstance(trend_set,TaurusTrend):
        parent = trend_set
    else:
        parent = getTrendObject(trend_set)
    lbound = parent.axisScaleDiv(parent.xBottom).lowerBound()
    ubound = parent.axisScaleDiv(parent.xBottom).upperBound()
    if not rough: ubound = min(time.time(),ubound)
    return [lbound,ubound]

    
def parseTrendModel(model):
    """ Attribute Name Parsing, Returns a tango_host,attribute,model tuple """
    modelobj = model
    if type(model) not in (str,):
        try: model = model.getFullName()
        except: 
            try: model = model.getModelName()
            except Exception,e:
                print e+'\n'+'getArchivedTrendValues():model(%s).getModelName() failed\, using str(model)'%model
                model = str(model)
    if '{' not in model: #Excluding "eval-like" models
        params = utils.parse_tango_model(model,fqdn=True)
        tango_host,attribute = '%s:%s'%(params['host'],params['port']),'%s/%s'%(params['devicename'],params['attributename'])
    else:
        tango_host,attribute='',modelobj.getSimpleName() if hasattr(modelobj,'getSimpleName') else model.split(';')[-1]

    model = fn.tango.get_full_name(model,fqdn=True)
    return tango_host,attribute,model

class MenuActionAppender(BoundDecorator):
    #self._showArchivingDialogAction = Qt.QAction("Show Archiving Dialog", None)
    #self.trend.connect(self._showArchivingDialogAction, Qt.SIGNAL("triggered()"), self.show_dialog)
    #TaurusTrend._canvasContextMenu = MenuActionAppender(self._showArchivingDialogAction)(TaurusTrend._canvasContextMenu)
    
    ACTIONS = [
          ('_showArchivingDialogAction',"Archiving Expert Dialog",'show_dialog',tuple()),
          ('_reloadArchiving',"Reload Archiving",'refreshCurves',(True,)), #'checkBuffers'),
          ]
    
    def __init__(self,before=None): #,action,before=None):
      #self.action = action
      self.tracer = 0
      MenuActionAppender.before = before
      
    #def wrapper(self,instance,f,*args,**kwargs):
    @staticmethod
    def wrapper(instance,f,*args,**kwargs):
        try:
            from taurus.qt.qtgui.plot import TaurusPlot
            obj = ArchivedTrendLogger(trend=instance)
            menu = f(instance)
            before = MenuActionAppender.before \
                or instance._usePollingBufferAction
            menu.insertSeparator(before)

            for t in MenuActionAppender.ACTIONS:
                if len(t)==4:
                    actname,label,method,args = t
                else:
                    actname,label,method = t
                    args = (instance,)
                action = getattr(instance,actname,None)
                if not action: 
                    setattr(instance,actname,Qt.QAction(label, None))
                    action = getattr(instance,actname)
                    instance.connect(action,Qt.SIGNAL("triggered()"),
                        (lambda o=instance,m=method,a=args,l=obj:
                            (l.warning(','.join(map(str,(o,m,a)))),
                            (m(o) if fn.isCallable(m) 
                                else getattr(o,m,
                                    getattr(l,m,None)))(*a)))
                        )

                menu.insertAction(before,action)
            menu.insertSeparator(before)
            return menu
        
        except:
            traceback.print_exc()
        
        
class QArchivedTrendInfo(Qt.QDialog):
    
    INSTANCE = None
    
    def __init__(self,parent=None,trend=None):
        Qt.QDialog.__init__(self,parent)
        self.setLayout(Qt.QVBoxLayout())
        self.panel = Qt.QTextBrowser()
        self.layout().addWidget(self.panel)
        self.reader = PyTangoArchiving.Reader()
        self.trend = trend
        self.setModel()
        self._bt = Qt.QPushButton('Refresh')
        self._bt.connect(self._bt,Qt.SIGNAL('clicked()'),self.setModel)
        self.layout().addWidget(self._bt)           
            
    def setModel(self,trend=None):
        trend = trend or self.trend
        if not trend: 
            self.panel.setText('')
            return
        models = []
        for n,ts in trend.trendSets.iteritems():
            model = ts.getModel()
            modes = self.reader.is_attribute_archived(model)
            buff = getattr(ts,'_xBuffer',[])
            if buff is None or not len(buff): buff = [0]
            models.append((model,modes,len(buff),buff[0],buff[-1]))
            
        self.panel.setText('\n'.join(sorted(
            '%s\n\t%s\n\t%d values\n\t%s - %s\n'
            %(m,n,l,time2str(b),time2str(e)) for m,n,l,b,e
            in models)))
        
    def show(self):
        if self.trend:
            self.setModel()
        Qt.QDialog.show(self)
    
class QReloadWidget(Qt.QWidget):
    
    def __init__(self,parent, logger, trend = None):
        Qt.QWidget.__init__(self,parent) #,*args)
        self.setup(parent,logger,trend)
        
    def setup(self,parent,logger,trend):
        self.trend = trend or logger.trend
        self.logger = logger
        self.setWindowTitle("Reload Archiving")
        lwidget,lcheck = Qt.QVBoxLayout(),Qt.QHBoxLayout()
        self.setLayout(lwidget)

        self._reloadbutton = Qt.QPushButton('Reload Archiving')
        self.layout().addWidget(self._reloadbutton)
        self._reloadbutton.connect(self._reloadbutton,Qt.SIGNAL('clicked()'),self.logger.resetBuffers)
        self._decimatecombo = Qt.QComboBox()
        self._decimatecombo.addItems([t[0] for t in DECIMATION_MODES])
        self._decimatecombo.setCurrentIndex(0)
        self._nonescheck = Qt.QCheckBox('Remove Nones')
        self._nonescheck.setChecked(True)
        self._nonescheck.connect(self._nonescheck,Qt.SIGNAL('toggled(bool)'),self.toggle_nones)
        self._windowedit = Qt.QLineEdit()
        self._windowedit.setText('0')

        dl = Qt.QGridLayout()
        dl.addWidget(Qt.QLabel('Decimation method:'),0,0,1,2)
        dl.addWidget(self._decimatecombo,0,3,1,2)
        dl.addWidget(Qt.QLabel('Fixed Period (0=AUTO)'),1,0,1,1)
        dl.addWidget(self._windowedit,1,1,1,1)
        dl.addWidget(self._nonescheck,1,2,1,2)
        self.layout().addLayout(dl)
        
    def toggle_nones(self,checked=False):
        return True        

    def getDecimation(self):
        # The decimation method must be a function that takes a series of values and return a single value that summarizes the interval
        try:
            t = str(self._decimatecombo.currentText())
            m = dict(DECIMATION_MODES)[t]
            self.logger.info('Decimation mode: %s,%s'%(m,t))
            return m
        except:
            self.logger.warning(traceback.format_exc())
            return None
        
    def getPeriod(self):
        return float(self._windowedit.text())
        
    def getNonesCheck(self):
        return self._nonescheck.isChecked()
    
class QReloadDialog(Qt.QDialog,QReloadWidget):
    
    def __init__(self, parent, logger, trend=None):
        Qt.QDialog(self,parent)
        self.setup(parent,logger,trend)
        
class ArchivedTrendLogger(SingletonMap):
    """
    This klass is attached to a TaurusTrendSet and keeps the information related to its archived values
    It will replace the tau_trend.TDBArchivingReader and tau_trend.HDBArchivingReader objects
    
    The object is a singleton for each trend,tango host pair; to get an existing logger just call:
       reader = ArchivedTrendLogger(trend,tango_host=tango_host).reader
       
    The ATLogger will show a QTextEdit with log messages, it can be closed once it has been executed
    """

    #Singleton behavior disabled
    #_instances = {}
    
    def __new__(cls,*p,**k):
        trend = p and p[0] or k['trend']
        override = k.get('override',False)
        tango_host = k.get('tango_host',None) or fn.get_tango_host(fqdn=True)
        schema = k.get('schema','*')
        if not getattr(trend,'_ArchiveLoggers',None):
            trend._ArchiveLoggers = {} #cls.__instances

        if override or tango_host not in trend._ArchiveLoggers:
            trend._ArchiveLoggers[tango_host] = object.__new__(cls)
            trend._ArchiveLoggers[tango_host].setup(*p,**k)
        return trend._ArchiveLoggers[tango_host]
        
    def setup(self,trend,use_db=True,db_config='',tango_host='',logger_obj=None,
          multiprocess=USE_MULTIPROCESS,schema='',show_dialog=False,
          filters=['*','!DEBUG'],force_period=10000, value_setter = None):
        #trend widget,attribute,start_date,stop_date,use_db,db_config,tango_host,logger_obj,multiprocess,schema=''):
        print('>'*80)
        
        from PyTangoArchiving.reader import Reader,ReaderProcess
        self.tango_host = tango_host
        self.trend = trend
        self.value_setter = value_setter
        self.model_trend_sets = defaultdict(list)
        self.last_check = (0,0) #Trend stores here the last bounds
        self.last_check_buffers = 0
        self.logger = logger_obj or trend
        self.log_objs = {}
        self.last_args = fn.CaselessDict() #{}
        self.last_bounds = (0,0,0) ##NOT USED
        self.on_check_scales = False
        self.last_msg = ''
        self.loglevel = 'INFO'
        
        self.recount = 0
        
        self.schema = schema or (use_db and '*') or 'hdb'
        print('In ArchivedTrendLogger.setup(%s,%s)'%(trend,self.schema))
        self.filters = filters
        self._dialog = None
        self.show_dialog(show_dialog) #The dialog is initialized here
        self.reader = (ReaderProcess if multiprocess else Reader)(
            schema=self.schema,config=db_config,tango_host=tango_host,logger=self)
        try:
            axis = self.trend.axisWidget(self.trend.xBottom)
            #self.trend.connect(axis,Qt.SIGNAL("scaleDivChanged ()"),
            #  lambda s=self:s.dialog()._checked or s.show_dialog()) 
            ## a new axis change will show archiving dialog
            self.trend.connect(axis,Qt.SIGNAL("scaleDivChanged ()"),self.checkScales)
            self.trend.connect(self.trend,Qt.SIGNAL("refreshData"),self.refreshCurves)
            
            #self.trend.connect(self.trend._useArchivingAction,Qt.SIGNAL("toggled(bool)"), self.show_dialog)
            
            MenuActionAppender.ACTIONS.extend(getattr(self.trend,'MENU_ACTIONS',[]))
            MenuActionAppender.ACTIONS=list(set(MenuActionAppender.ACTIONS))
            if True: 
                TaurusTrend._canvasContextMenu = MenuActionAppender()(TaurusTrend._canvasContextMenu)
            else: 
                self.trend._canvasContextMenu = MenuActionAppender()(self.trend._canvasContextMenu)
        except:
            self.warning(traceback.format_exc())

    def show_dialog(self,enable=True):
        if not self.dialog():
            try:
                self._dialog = QTextBuffer(title='Archiving Logs',maxlen=1000)
                
                self._trendinfo = QArchivedTrendInfo(trend=self.trend)
                self._trendinfobt = Qt.QPushButton('Show Models Info')
                self._trendinfobt.connect(self._trendinfobt,Qt.SIGNAL('clicked()'),
                                          self._trendinfo.show)
                self.dialog().layout().addWidget(self._trendinfobt)   
                
                self._forcedbt = Qt.QPushButton('Force trend update')
                self._forcedbt.connect(self._forcedbt,Qt.SIGNAL('clicked()'),
                                          #self.forceReadings)
                                          self.trend.setForcedReadingPeriod)
                self.dialog().layout().addWidget(self._forcedbt)
                
                self._reloader = QReloadWidget(
                    parent=self._dialog, trend=self.trend, logger=self)
                self.dialog().layout().addWidget(self._reloader)
                
                self._showhist = Qt.QPushButton('Show/Save buffers as a Table')
                self._showhist.connect(self._showhist,Qt.SIGNAL('clicked()'),self.showRawValues)
                self.dialog().layout().addWidget(self._showhist)                
                
                self._clearbutton = Qt.QPushButton('Clear Buffers and Redraw')
                self.dialog().layout().addWidget(self._clearbutton)
                self._clearbutton.connect(self._clearbutton,Qt.SIGNAL('clicked()'),
                    fn.partial(fn.qt.QConfirmAction,self.clearBuffers))                

                if hasattr(self.trend,'closeEvent'): 
                    #setDialogCloser(self.dialog(),self.trend)
                    setCloserTimer(self.dialog(),self.trend)
                    
            except: self.warning(traceback.format_exc())

        if self.dialog():
            ### @TODO
            self.dialog().toggle(not enable)
            #if not enable: 
                #print('show_dialog(False): hiding dialog')
                #self.dialog().hide()
            if enable:
                self.dialog().toggle(True)
                self.dialog().show()
                
    def show_dates_widget(self,show=True):
        self.trend.showDatesWidget(show=show)
            
    def instances(self):
        return self.trend._ArchiveLoggers
    def dialog(self):
        return self._dialog #ArchivedTrendLogger._dialog #Logger is single-trend based; having a singleton fails when having several trends
    
    def setLastArgs(self,model,start=0,stop=None,history=-1,date=None):
        #self.info('ArchivedTrendLogger.setLastArgs(%s)'%str((model,start,stop,history,date)))
        date = date or time.time()
        if fn.isSequence(history): history = len(history)
        self.last_args[model] = [start,stop,history,date] #It must be a list, not tuple
    
    def getDecimation(self): return self._reloader.getDecimation()
    def getPeriod(self): return self._reloader.getPeriod()
    def getNonesCheck(self): return self._reloader.getNonesCheck() 

    def refreshCurves(self,check_buffers=False):
        names =  self.trend.getCurveNames()
        self.warning('%s: In refreshCurves(%s,%s) ...'%
                     (fn.time2str(),names,check_buffers))

        if check_buffers:
            self.checkBuffers()
            
        try:
            self.forceReadings(emit=False)
        except:
            self.warning(traceback.format_exc())
            
        for n in names:
            c = self.trend.getCurve(n)
            v = c.isVisible()
            if v:
                c.setVisible(False)
                c.setYAxis(c.yAxis())
                c.setVisible(True)
            else:
                self.warning('%s curve is hidden'%v)
        return
    
    def checkScales(self):
        bounds = getTrendBounds(self.trend,True)
        
        if self.on_check_scales:
            return False
        
        try:
            self.on_check_scales = True
            ## Check to be done before triggering anything else
            diff = bounds[0]-self.last_bounds[0], bounds[1]-self.last_bounds[-1]
            diff = max(map(abs,diff))
            td = fn.now()-self.last_bounds[-1]
            r = max((30.,0.5*(bounds[1]-bounds[0])))
            ##This avoids re-entring calls into checkScales
            self.last_bounds = (bounds[0],bounds[1],fn.now())
            
            if self.trend.getXDynScale():
                if not getattr(self.trend,'_configDialog',None):
                    if (bounds[-1]<(time.time()-3600) 
                        or (bounds[-1]-bounds[0])>7200):
                        self.info('Disabling XDynScale when showing past data')
                        self.trend.setXDynScale(False) 

                if self.trend.isPaused(): # A paused trend will not load data
                    self.warning('resume plotting ...')
                    self.trend.setPaused(False)
                    self.trend._pauseAction.setChecked(False)

            self.debug('In checkScales(%s,%s,%s)'%(str(bounds),diff,r))
            self.checkBuffers()
            self.debug('Out of checkScales(%s,%s,%s)'%(str(bounds),diff,r))
        except:
            self.warning(traceback.format_exc())
        finally:
            self.on_check_scales = False
            
    def forceReadings(self,filters='',emit=True):
        for n,ts in self.trend.trendSets.iteritems():
            model = ts.getModel()
            if not filters or fn.clmatch(filters,model):
                self.warning('forceReadings(%s,%s)' % (model,emit))
                ts.forceReading()
        if emit:
            self.trend.emit(Qt.SIGNAL('refreshData'))
            
    def resetBuffers(self,*args):
        self.checkBuffers(self,*args,forced=True)

    def checkBuffers(self,*args,**kwargs):
        self.warning('In CheckBuffers(%s)'%str(self.trend.trendSets.keys()))
        #self.trend.doReplot()
        t0 = fn.now()
        if t0 - self.last_check_buffers < 1.:
            return
        
        self.show_dialog(not self.dialog()._checked)
        
        for n,ts in self.trend.trendSets.iteritems():
            try:
                model = ts.getModel()
                if model in self.last_args: self.last_args[model][-1] = 0
                self.debug('%s buffer has %d values' % 
                    (model, len(getattr(ts,'_xBuffer',[]))))
                
                # HOOK ADDED FROM CLIENT SIDE, getArchivedTrendValues
                self.value_setter(ts,model,
                    **{'insert':True,'forced':kwargs.get('forced')})
                
                if not fn.tango.check_attribute(model,readable=True):
                    ## THIS CODE MUST BE HERE, NEEDED FOR DEAD ATTRIBUTES
                    self.warning('checkBuffers(%s): attribute forced ...' % model)
                    ts.forceReading()
            except: 
                self.warning(traceback.format_exc())

        self.trend.doReplot()
        self.last_check_buffers = fn.now()
        #d = self.last_check_buffers - t0
        #if d > 0.2:
            #self.warning('checkBuffers is too intensive (%f), disable dynscale'%d)
            #self.stopPlotting()
        self.warning('Out of CheckBuffers(%s)'%str(self.trend.trendSets.keys()))
        
    def stopPlotting(self):
        self.trend.setXDynScale(False)
        #self.trend.setPaused(True) #TREND PAUSED DOES NOT REPLOT!?!?
        self.trend._pauseAction.setChecked(True)
        #for n,ts in self.trend.trendSets.iteritems():
            #model = ts.getModel()
            #logger_obj.warning('Pausing %s polling'%model)
            #taurus.Attribute(model).deactivatePolling()
        
    def clearBuffers(self,*args):
        self.warning('ArchivedTrendLogger.clearBuffers(%s)'%str(args))
        self.last_args = fn.CaselessDict() 
        self.reader.reset()

        for rd in self.reader.configs.values():
          try:
            rd.reset()
            rd.cache.clear()
            rd.last_dates.clear()
          except:
            self.debug('unable to clear %s buffers'%rd)

        for n,ts in self.trend.trendSets.iteritems():
            ts.clearTrends(replot=True)
            #self._xBuffer.moveLeft(self._xBuffer.maxSize())
            #self._yBuffer.moveLeft(self._yBuffer.maxSize())
            ts.forceReading()

        return
        
    def showRawValues(self):

        try:
            self._dl = fn.qt.QDialogWidget(buttons=True)
            qc = Qt.QComboBox()
            qc.addItems(sorted(self.last_args.keys()))
            self._dl.setWidget(qc)
            #dates = []
            #try:
              #self.warning('-------------------')
              #dates.append(self.trend.axisScaleDiv(self.trend.xBottom).lowerBound())
              #dates.append(self.trend.axisScaleDiv(self.trend.xBottom).upperBound())
              #self.warning(dates)
            #except: self.warning(traceback.format_exc())
            self._dl.setAccept(lambda q=qc:show_history(parseTrendModel(str(q.currentText()))[1])) #,dates=dates))
            self._dl.show()
        except:
            self.warning(traceback.format_exc())
        
    def log(self,severity,msg):
        if msg == self.last_msg: 
            msg = '+1'
        else: 
            self.last_msg = msg
            if self.logger:
                try:
                    if severity not in self.log_objs: self.log_objs[severity] = \
                        getattr(self.logger,severity.lower(),
                                lambda m,s=severity:'%s:%s: %s'%
                                (s.upper(),fn.time2str(),m))
                    self.log_objs[severity](msg)
                except: pass
        if self.dialog():
            if msg!='+1': 
                msg = '%s:%s: %s'%(severity.upper(),fn.time2str(),msg)
            if self.filters:
                msg = (fn.filtersmart(msg,self.filters) or [''])[0]
            if msg:
                if len(self.instances())>1: msg = self.tango_host+':'+msg
                self.dialog().append(msg)
                
    def setLogLevel(self,level): self.loglevel = level
    def getLogLevel(self): return self.loglevel
    def trace(self,msg): self.log('trace',msg)
    def debug(self,msg): self.log('debug',msg)
    def info(self,msg): self.log('info',msg)
    def warning(self,msg): self.log('warning',msg)
    def alarm(self,msg): self.log('alarm',msg)
    def error(self,msg): self.log('error',msg)


class DatesWidget(Qt.QWidget): #Qt.QDialog): #QGroupBox):
    """
    DatesWidget to control a TaurusTrend from an external widget
    
    Little panel with:
        Start date
        Length
        add Apply = Reload+Pause+ShowArchivingDialog
    """
    def __init__(self,trend,parent=None,layout=Qt.QVBoxLayout):
        print('DatesWidget(%s)'%trend)
        #parent = parent or trend.legend()
        Qt.QWidget.__init__(self,parent or trend)
        #trend.showLegend(True)
        self._trend = trend
        if not hasattr(trend,'_datesWidget'):
            trend._datesWidget = self
            
        self.setLayout(layout())
        self.DEFAULT_START = 'YYYY/MM/DD hh:mm:ss'
        self.setTitle("Show Archiving since ...")
        self.xLabelStart = Qt.QLabel('Start')
        self.xEditStart = Qt.QLineEdit(self.DEFAULT_START)
        self.xEditStart.setToolTip("""
            Start date, it can be: 
            <empty>              #apply -range to current date
            YYYY/MM/DD hh:mm:ss  #absolute
            -1d/h/m/s            #relative to current date
            """)
        self.xLabelRange = Qt.QLabel("Range")
        self.xRangeCB = Qt.QComboBox()
        self.xRangeCB.setEditable(True)
        self.xRangeCB.addItems(["","1 m","1 h","1 d","1 w","30 d","1 y"])
        self.xRangeCB.setToolTip("""
            Any range like:
            <empty> show data from Start 'til now
            1m : X minutes
            1h : X hours
            1d : X days
            1w : X weeks
            1y : X years
            """)
        self.xRangeCB.setCurrentIndex(1)
        self.xApply = Qt.QPushButton("Refresh")
        self.layout().addWidget(QWidgetWithLayout(self,child=[self.xLabelStart,self.xEditStart]))
        self.layout().addWidget(QWidgetWithLayout(self,child=[self.xLabelRange,self.xRangeCB]))
        self.layout().addWidget(QWidgetWithLayout(self,child=[self.xApply]))
        trend.connect(self.xApply,Qt.SIGNAL("clicked()"),self.refreshAction)
        
        if hasattr(self._trend,'getArchivedTrendLogger'):
            self.logger = self._trend.getArchivedTrendLogger()
            self.xInfo = Qt.QPushButton("Expert")        
            self.layout().addWidget(QWidgetWithLayout(self,child=[self.xInfo]))
            trend.connect(self.xInfo,Qt.SIGNAL("clicked()"),self.logger.show_dialog)
        
        #if parent is trend.legend():

            #trend.legend().setLayout(Qt.QVBoxLayout())
            #trend.legend().layout().setAlignment(Qt.Qt.AlignBottom)
            #self.setMinimumWidth(150)
            #self.setMinimumHeight(15*4)
            #trend.legend().layout().addWidget(self)
            #trend.legend().setMinimumWidth(150)
            #trend.setMinimumHeight(250)
            #try:
                #trend.legend().children()[0].setMinimumWidth(150)
                #trend.legend().children()[0].children()[0].setMinimumWidth(150)
            #except:
                #ms = Qt.QMessageBox.warning(trend,"Error!",traceback.format_exc())
                
        return
        
    def setTitle(self,title):
        self.setWindowTitle(title)
        
    def refreshAction(self):
        self._trend.applyNewDates()
        try:
            date = str2time(str(self.xEditStart.text()))
        except:
            try:
                date = getTrendBounds(self._trend)[0]
                self.xEditStart.setText(time2str(date))
            except:
                traceback.print_exc()
      
            
def setCloserTimer(dialog,parent=None,period=3000):
    if not parent: parent = getObjectParent(dialog)
    dialog._timer = Qt.QTimer()
    dialog._timer.setInterval(period)
    def closer(s=dialog,p=parent):
        try:
            if not p.isVisible():
                s.close()
        except:pass
    dialog._timer.connect(dialog._timer,Qt.SIGNAL('timeout()'),closer)
    dialog._timer.start()      
      
      
###############################################################################
