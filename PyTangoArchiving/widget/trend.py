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
import fandango
import PyTangoArchiving
import PyTangoArchiving.utils as utils
from PyTangoArchiving.reader import Reader,ReaderProcess

## WARNING: AVOID TO IMPORT TAURUS OR QT OUTSIDE ANY METHOD OR CLASS, BE GREEN!
#from taurus.qt.qtgui.plot import TaurusTrend

from history import show_history
from fandango.qt import Qt,QTextBuffer,setDialogCloser,QWidgetWithLayout
from fandango.dicts import defaultdict
from fandango import SingletonMap,str2time

from taurus.qt.qtgui.plot import TaurusTrend
from taurus.qt.qtgui.base import TaurusBaseWidget
from taurus.qt.qtgui.container import TaurusWidget,TaurusGroupBox
from taurus.external.qt import Qt, Qwt5
from fandango.objects import BoundDecorator

USE_MULTIPROCESS=False
try:
    prop = PyTango.Database().get_class_property('HdbExtractor',['Multiprocess'])['Multiprocess']
    if any(a in str(prop).lower() for a in ('true','1')):
        USE_MULTIPROCESS=True
    elif any(fandango.matchCl(p,platform.node()) for p in prop):
        USE_MULTIPROCESS=True
except:
    print traceback.format_exc()
print 'Multiprocess:%s'%USE_MULTIPROCESS

#################################################################################################
# Methods for enabling archiving values in TauTrends

try:
    fakeTrend = fandango.Struct({
        '_parent':fandango.Struct({'xIsTime':True}),'_history':[],
        'info':fandango.printf,'error':fandango.printf,'debug':fandango.printf,'warning':fandango.printf,
        })
except: pass

from PyTangoArchiving.reader import STARTUP
global STARTUP_DELAY
STARTUP_DELAY = 0.

ZONES = fandango.Struct({'BEGIN':0,'MIDDLE':1,'END':2})
DECIMATION_MODES = [
    #('Hide Nones',fandango.arrays.notnone),
    ('Maximize Peaks',fandango.arrays.maxdiff),
    ('Average Values',fandango.arrays.average),
    ('RAW',None),
    ]

class ArchivingTrendWidget(TaurusGroupBox):
    def __init__(self, parent = None, designMode = False):
      TaurusGroupBox.__init__(self, parent, designMode)
      self.setTitle('Archiving Trend')
      self.setLayout(Qt.QVBoxLayout())
      self._trend = ArchivingTrend(parent=self,designMode = designMode)
      self._datesWidget = DatesWidget(trend=self._trend,parent=self,layout=Qt.QHBoxLayout)
      self._trend._datesWidget = self._datesWidget
      self.layout().addWidget(self._trend)
      self.layout().addWidget(self._datesWidget)
      self._datesWidget.show()
      
    #def connect(self,*a,**k):
      #self._trend.connect(*a,**k)
      
    #def disconnect(self,*a,**k):
      #self._trend.disconnect(*a,**k)      
      
    def __getattr__(self,attr):
      if attr not in ('_trend','_datesWidget'):
        return getattr(self._trend,attr)

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
      self.xApply = Qt.QPushButton("Reload")
      self.layout().addWidget(QWidgetWithLayout(self,child=[self.xLabelStart,self.xEditStart]))
      self.layout().addWidget(QWidgetWithLayout(self,child=[self.xLabelRange,self.xRangeCB]))
      self.layout().addWidget(QWidgetWithLayout(self,child=[self.xApply]))
      trend.connect(self.xApply,Qt.SIGNAL("clicked()"),trend._applyNewDates)
      
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

class ArchivingTrend(TaurusTrend):
  
    MENU_ACTIONS =         [
        ('_datesWidgetOption','Show Dates Widget',(lambda o:o.showDatesWidget())),
        ('_zoomBackOption','Zoom Back (middle click)',(lambda o:o._zoomBack())),
        ('_setAxisFormatOption','Set Y Axis Format',(lambda o:o._showAxisFormatDialog())),
        ('_pausedOption','Pause (P)',(lambda o:o.setPaused(not o.isPaused()))),
        ]
  
    def __init__(self, parent = None, designMode = False):
      actions = [a[1] for a in MenuActionAppender.ACTIONS]
      print '>'*80+'\n'+str(MenuActionAppender.ACTIONS)
      TaurusTrend.__init__(self,parent,designMode)
      self.resetDefaultCurvesTitle()
      self.setXDynScale(True)
      self.setXIsTime(True)
      self.setUseArchiving(True)
      self.setModelInConfig(False)
      self.disconnect(self.axisWidget(self.xBottom), Qt.SIGNAL("scaleDivChanged ()"), self._scaleChangeWarning)
      #ArchivedTrendLogger(self,tango_host=fandango.get_tango_host(),multiprocess=USE_MULTIPROCESS)
      ArchivedTrendLogger(self,multiprocess=USE_MULTIPROCESS)
      #self.MENU_ACTIONS = [
        #('_zoomBackOption','Zoom Back (middle click)',(lambda o:o._zoomBack())),
        #('_setAxisFormatOption','Set Y Axis Format',(lambda o:o._showAxisFormatDialog())),
        #('_pausedOption','Pause (P)',(lambda o:o.setPaused(not o.isPaused()))),
        #]
      #self.MENU_ACTIONS.insert(0,)
      
    def getArchivedTrendLogger(self,model=None):
        host = fandango.get_tango_host(model or None)
        return ArchivedTrendLogger(self,tango_host=host)
      
    def setForcedReadingPeriod(self, msec=None, tsetnames=None):
        '''Sets the forced reading period for the trend sets given by tsetnames.
        
        :param msec: (int or None) period in milliseconds. If None passed, the user will be 
                     prompted
        :param tsetnames: (seq<str> or None) names of the curves for which the forced 
                          reading is set. If None passed, this will be set for all 
                          present *and future* curves added to this trend
    
        .. seealso: :meth:`TaurusTrendSet.setForcedReadingPeriod`
        '''
        if msec is None:
            msec = self._forcedReadingPeriod
            try: #API changed in QInputDialog since Qt4.4
                qgetint = Qt.QInputDialog.getInt
            except AttributeError:
                qgetint = Qt.QInputDialog.getInteger
            msec,ok = qgetint(self, 'New forced reading period', 
                                               'Enter the new period for forced reading (in ms).\n Enter "0" for disabling', 
                                               max(0,msec), 0, 604800000, 100)
            if not ok: 
                return
            if msec == 0: 
                msec=-1
        
        self._forcedReadingPeriod = msec
        
        if tsetnames is None: 
            tsetnames=self.getModel()
        self.curves_lock.acquire()
        try:
            for name in tsetnames:
                tset = self.trendSets[name]
                tset.setForcedReadingPeriod(msec)
                tset.setEventFilters([])
                        
        finally:
            traceback.print_exc()
            self.curves_lock.release()
  
    def _showAxisFormatDialog(self,axis=None):
      try:
        import PyQt4.Qwt5,PyQt4.Qt
        axis = axis or PyQt4.Qwt5.QwtPlot.yLeft
        qi = Qt.QInputDialog.getText(None,"Axis Format","Enter format for Axis labels (\%6.2f):")
        if qi[1]: 
          self.setAxisLabelFormat(axis,str(qi[0]))
          self.doReplot()
      except:
        ms = Qt.QMessageBox.warning(self,"Error!",traceback.format_exc())
        
    def _zoomBack(self,zoomer=None):
      try:
        self.setPaused(True)
        zoomer = zoomer or self._zoomer1
        zs = zoomer.zoomStack()
        if len(zs):
          zoomer.zoom(zs[0])
      except:
        ms = Qt.QMessageBox.warning(self,"Error!",traceback.format_exc())            
  
    def _axisContextMenu(self,axis=None):
      '''Returns a context menu for the given axis
      :param axis: (Qwt5.QwtPlot.Axis) the axis
      :return: (Qt.QMenu) the context menu for the given axis
      '''
      try:
        menu = Qt.QMenu(self)
        axisname = self.getAxisName(axis)
        menu.setTitle("Options for axis %s"%axisname)

        autoScaleThisAxis = lambda : self.setAxisAutoScale(axis=axis)
        autoscaleAction= menu.addAction("AutoScale %s"%axisname)
        self.connect(autoscaleAction, Qt.SIGNAL("triggered()"), autoScaleThisAxis)

        if not self.getXIsTime():
            switchThisAxis = lambda : self.setAxisScaleType(axis=axis, scale=None)
            switchThisAxisAction= menu.addAction("Toggle linear/log for %s"%axisname)
            self.connect(switchThisAxisAction, Qt.SIGNAL("triggered()"), switchThisAxis)

        if axis in (Qwt5.QwtPlot.yLeft, Qwt5.QwtPlot.yRight):
            zoomOnThisAxis = lambda : self.toggleZoomer(axis=axis)
            zoomOnThisAxisAction= menu.addAction("Zoom-to-region acts on %s"%axisname)
            self.connect(zoomOnThisAxisAction, Qt.SIGNAL("triggered()"), zoomOnThisAxis)
            yZoomBackAction=menu.addAction("Zoom back")
            self.connect(yZoomBackAction,Qt.SIGNAL("triggered()"),self._zoomBack)
            ySetFormatAction=menu.addAction("Set Axis Format")
            self.connect(ySetFormatAction,Qt.SIGNAL("triggered()"),self._showAxisFormatDialog)

        elif axis in (Qwt5.QwtPlot.xBottom, Qwt5.QwtPlot.xTop):
            if self.isXDynScaleSupported():
                xDynAction=menu.addAction("&Auto-scroll %s"%axisname)
                xDynAction.setToolTip('If enabled, the scale of %s will be autoadjusted to provide a fixed window moving to show always the last value')
                xDynAction.setCheckable(True)
                xDynAction.setChecked(self.getXDynScale())
                self.connect(xDynAction, Qt.SIGNAL("toggled(bool)"), self.setXDynScale)
            xZoomBackAction=menu.addAction("Zoom back")
            self.connect(xZoomBackAction,Qt.SIGNAL("triggered()"),self._zoomBack)
            xShowDatesAction=menu.addAction("Show Dates Widget")
            self.connect(xShowDatesAction,Qt.SIGNAL("triggered()"),self.showDatesWidget)
        return menu  
      except:
        ms = Qt.QMessageBox.warning(self,"Error!",traceback.format_exc())
        
    def _onUseArchivingAction(self, enable):
      '''slot being called when toggling the useArchiving action
      
      .. seealso:: :meth:`setUseArchiving`
      '''
      try:
        TaurusTrend._onUseArchivingAction(self,enable)
        self.replot()
      except:
        ms = Qt.QMessageBox.warning(self,"Error!",traceback.format_exc())
        
    def _applyNewDates(self):
      try:
        #self.setForcedReadingPeriod(3000)
        #self.setPaused(True)
        ui = self._datesWidget
        start = str(ui.xEditStart.text())
        end = str(ui.xRangeCB.currentText())
        print('applyNewDates(%s,%s)'%(start,end))
        try: t0 = str2time(start)
        except: t0 = None
        try: t1 = str2time(end)
        except: t1 = None
        if t1 is not None:
          if t0 is None:
            now = time.time()
            t0,t1 = time.time()-t1,time.time()
          else:
            if t0<0: t0 = time.time()+t0
            t0,t1 = t0,t0+t1
        if t1-t0 > 365*86400:
          v = Qt.QMessageBox.warning(self,'Warning!','Reading an interval so big may hung your PC!!',Qt.QMessageBox.Ok|Qt.QMessageBox.Cancel)
          if v == Qt.QMessageBox.Cancel:
            return
        if t0 is not None:
          print('applyNewDates(%s,%s)'%(fandango.time2str(t0),fandango.time2str(t1)))
          self.setAxisScale(Qwt5.QwtPlot.xBottom, t0, t1)
          hosts = map(fandango.get_tango_host,self.getModel())
          for m in fandango.toList(self.getModel()):
            self.getArchivedTrendLogger().checkBuffers()
      except:
        ms = Qt.QMessageBox.warning(self,"Error!",traceback.format_exc())
        
    def resetDefaultCurvesTitle(self):
        '''resets the defaultCurvesTitle property to '<label>'

        .. seealso:: :meth:`setDefaultCurvesTitle`'''
        self.setDefaultCurvesTitle('<label><[trend_index]><br>(<dev_name>/<attr_name>)')
        #self.setDefaultCurvesTitle('<label><[trend_index]>')
    
    def showDatesWidget(self,show=True):
      try:
        ui = getattr(self,'_datesWidget',None)
        try:
          ui.parent()
        except:
          ui = None
        if not ui:
          self._datesWidget = DatesWidget(self,self.legend(),Qt.QVBoxLayout())
          
        if show: self._datesWidget.show()
        else: self._datesWidget.hide()
        self.replot()
        return
        #xMin = self.parent.axisScaleDiv(Qwt5.QwtPlot.xBottom).lowerBound()
        #xMax = self.parent.axisScaleDiv(Qwt5.QwtPlot.xBottom).upperBound()
        #if self.parent.getXIsTime():
                #self.ui.xEditMin.setText(time.strftime('%Y/%m/%d %H:%M:%S',time.localtime(int(xMin))))
      except:
        ms = Qt.QMessageBox.warning(self,"Error!",traceback.format_exc())
        
    def pickDataPoint(self, pos, scope=20, showMarker=True, targetCurveNames=None):
        '''Finds the pyxel-wise closest data point to the given position. The
        valid search space is constrained by the scope and targetCurveNames
        parameters.

        :param pos: (Qt.QPoint or Qt.QPolygon) the position around which to look
                    for a data point. The position should be passed as a
                    Qt.QPoint (if a Qt.QPolygon is given, the first point of the
                    polygon is used). The position is expected in pixel units,
                    with (0,0) being the top-left corner of the plot
                    canvas.

        :param scope: (int) defines the area around the given position to be
                      considered when searching for data points. A data point is
                      considered within scope if its manhattan distance to
                      position (in pixels) is less than the value of the scope
                      parameter. (default=20)

        :param showMarker: (bool) If True, a marker will be put on the picked
                           data point. (default=True)

        :param targetCurveNames: (sequence<str>) the names of the curves to be
                                 searched. If None passed, all curves will be
                                 searched

        :return: (tuple<Qt.QPointF,str,int> or tuple<None,None,None>) if a point
                 was picked within the scope, it returns a tuple containing the
                 picked point (as a Qt.QPointF), the curve name and the index of
                 the picked point in the curve data. If no point was found
                 within the scope, it returns None,None,None
        '''
        if isinstance(pos,Qt.QPolygon):pos=pos.first()
        scopeRect=Qt.QRect(0,0,scope,scope)
        scopeRect.moveCenter(pos)
        mindist=scope
        picked=None
        pickedCurveName=None
        pickedIndex=None
        self.curves_lock.acquire()
        try:
            if targetCurveNames is None: targetCurveNames = self.curves.iterkeys()
            for name in targetCurveNames:
                curve = self.curves.get(name, None)
                if curve is None: self.error("Curve '%s' not found"%name)
                if not curve.isVisible(): continue
                data=curve.data()
                for i in xrange(data.size()):
                    point=Qt.QPoint(self.transform(curve.xAxis(),data.x(i)) , self.transform(curve.yAxis(),data.y(i)))
                    if scopeRect.contains(point):
                        dist=(pos-point).manhattanLength()
                        if dist < mindist:
                            mindist=dist
                            picked = Qt.QPointF(data.x(i),data.y(i))
                            pickedCurveName=name
                            pickedIndex=i
                            pickedAxes = curve.xAxis(), curve.yAxis()
        finally:
            self.curves_lock.release()

        if showMarker and picked is not None:
            self._pickedMarker.detach()
            self._pickedMarker.setValue(picked)
            self._pickedMarker.setAxis(*pickedAxes)
            self._pickedMarker.attach(self)
            self._pickedCurveName=pickedCurveName
            self._pickedMarker.pickedIndex=pickedIndex
            pickedCurveTitle = self.getCurveTitle(pickedCurveName)
            self.replot()
            label=self._pickedMarker.label()
            import PyQt4.Qwt5,PyQt4.Qt
            from datetime import datetime
            ax = pickedAxes[1]
            yformat = self.getAxisLabelFormat(ax) or "%.5g"
            print yformat
            s = "'%s'[%i]:\n\t (t=%s, y="+yformat+")"
            if self.getXIsTime():
                data = (pickedCurveTitle,pickedIndex,datetime.fromtimestamp(picked.x()).ctime(),picked.y())
            else:
                data = (pickedCurveTitle,pickedIndex,picked.x(),picked.y())
            try:
              infotxt = s%data
              print infotxt
            except:
              traceback.print_exc()
              infotxt = "'%s'[%i]:\n\t (t=%s, y=%.5g)"%data

            label.setText(infotxt)
            fits = label.textSize().width()<self.size().width()
            if fits:
                self._pickedMarker.setLabel(Qwt5.QwtText (label))
                self._pickedMarker.alignLabel()
                self.replot()
            else:
                popup = Qt.QWidget(self, Qt.Qt.Popup)
                popup.setLayout(Qt.QVBoxLayout())
                popup.layout().addWidget(Qt.QLabel(infotxt))  #@todo: make the widget background semitransparent green!
                popup.setWindowOpacity(self._pickedMarker.labelOpacity)
                popup.show()
                popup.move(self.pos().x()-popup.size().width(),self.pos().y() )
                popup.move(self.pos())
                Qt.QTimer.singleShot(5000, popup.hide)
                
        return picked,pickedCurveName,pickedIndex        

class MenuActionAppender(BoundDecorator):
    #self._showArchivingDialogAction = Qt.QAction("Show Archiving Dialog", None)
    #self.trend.connect(self._showArchivingDialogAction, Qt.SIGNAL("triggered()"), self.show_dialog)
    #TaurusTrend._canvasContextMenu = MenuActionAppender(self._showArchivingDialogAction)(TaurusTrend._canvasContextMenu)
    
    ACTIONS = [
          ('_showArchivingDialogAction',"Show Archiving Dialog",'show_dialog'),
          ('_reloadArchiving',"Reload Archiving",'checkBuffers'),
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
        before = MenuActionAppender.before or instance._usePollingBufferAction
        menu.insertSeparator(before)

        for actname,label,method in MenuActionAppender.ACTIONS:
          action = getattr(instance,actname,None)
          if not action: 
              method = method if fandango.isCallable(method) else getattr(obj,method,None)
              setattr(instance,actname,Qt.QAction(label, None))
              action = getattr(instance,actname)
              instance.connect(action,Qt.SIGNAL("triggered()"),(lambda o=instance,m=method:m(o)))
          menu.insertAction(before,action)
        menu.insertSeparator(before)
        return menu
      except:
        traceback.print_exc()
        
class ArchivedTrendLogger(SingletonMap):
    """
    This class is attached to a TaurusTrendSet and keeps the information related to its archived values
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
        tango_host = k.get('tango_host',None) or fandango.get_tango_host()
        schema = k.get('schema','*')
        if not getattr(trend,'_ArchiveLoggers',None):
            trend._ArchiveLoggers = {} #cls.__instances

        if override or tango_host not in trend._ArchiveLoggers:
            trend._ArchiveLoggers[tango_host] = object.__new__(cls)
            trend._ArchiveLoggers[tango_host].setup(*p,**k)
        return trend._ArchiveLoggers[tango_host]
        
    def setup(self,trend,use_db=True,db_config='',tango_host='',logger_obj=None,
          multiprocess=USE_MULTIPROCESS,schema='',show_dialog=False,
          filters=['*','!DEBUG'],force_period=10000):
        #trend widget,attribute,start_date,stop_date,use_db,db_config,tango_host,logger_obj,multiprocess,schema=''):
        print('>'*80)
        from PyTangoArchiving.reader import Reader,ReaderProcess
        self.tango_host = tango_host
        self.trend = trend
        self.model_trend_sets = defaultdict(list)
        self.last_check = (0,0)
        self.logger = logger_obj or trend
        self.log_objs = {}
        self.last_args = {}
        self.last_msg = ''
        self.loglevel = 'INFO'
        self.schema = schema or (use_db and '*') or 'hdb'
        print('In ArchivedTrendLogger.setup(%s,%s)'%(trend,self.schema))
        self.filters = filters
        self._dialog = None
        self.show_dialog(show_dialog) #The dialog is initialized here
        self.reader = (ReaderProcess if multiprocess else Reader)(
            schema=self.schema,config=db_config,tango_host=tango_host,logger=self)
        try:
            axis = self.trend.axisWidget(self.trend.xBottom)
            #self.trend.connect(axis,Qt.SIGNAL("scaleDivChanged ()"),lambda s=self:s.dialog()._checked or s.show_dialog()) #a new axis change will show archiving dialog
            self.trend.connect(axis,Qt.SIGNAL("scaleDivChanged ()"),self.checkScales)
            #self.trend.connect(self.trend._useArchivingAction,Qt.SIGNAL("toggled(bool)"), self.show_dialog)
            
            MenuActionAppender.ACTIONS.extend(getattr(self.trend,'MENU_ACTIONS',[]))
            MenuActionAppender.ACTIONS=list(set(MenuActionAppender.ACTIONS))
            if True: TaurusTrend._canvasContextMenu = MenuActionAppender()(TaurusTrend._canvasContextMenu)
            else: self.trend._canvasContextMenu = MenuActionAppender()(self.trend._canvasContextMenu)
        except:
            self.warning(traceback.format_exc())

    def show_dialog(self,enable=True):
        if not self.dialog():
            try:
                self._dialog = QTextBuffer(title='Archiving Logs',maxlen=1000)
                self._showhist = Qt.QPushButton('Show/Save values in a Table')
                self._showhist.connect(self._showhist,Qt.SIGNAL('clicked()'),self.showRawValues)
                self.dialog().layout().addWidget(self._showhist)
                self._reloadbutton = Qt.QPushButton('Reload Archiving')
                self.dialog().layout().addWidget(self._reloadbutton)
                self._reloadbutton.connect(self._reloadbutton,Qt.SIGNAL('clicked()'),fandango.partial(fandango.qt.QConfirmAction,self.checkBuffers))
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
                self.dialog().layout().addLayout(dl)
                self._clearbutton = Qt.QPushButton('Clear Buffers and Redraw')
                self.dialog().layout().addWidget(self._clearbutton)
                self._clearbutton.connect(self._clearbutton,Qt.SIGNAL('clicked()'),fandango.partial(fandango.qt.QConfirmAction,self.clearBuffers))
                if hasattr(self.trend,'closeEvent'): 
                    #setDialogCloser(self.dialog(),self.trend)
                    setCloserTimer(self.dialog(),self.trend)
            except: self.warning(traceback.format_exc())
        if self.dialog():
            self.dialog().toggle(not enable)
            if not enable: 
                self.dialog().hide()
            else:
                self.dialog().toggle(True)
                self.dialog().show()
                
    def show_dates_widget(self,show=True):
        self.trend.showDatesWidget(show=show)

    def toggle_nones(self,checked=False):
        return True
            
    def instances(self):
        return self.trend._ArchiveLoggers
    def dialog(self):
        return self._dialog #ArchivedTrendLogger._dialog #Logger is single-trend based; having a singleton fails when having several trends
    
    def setLastArgs(self,model,start=0,stop=None,history=-1,date=None):
        #self.info('ArchivedTrendLogger.setLastArgs(%s)'%str((model,start,stop,history,date)))
        date = date or time.time()
        if fandango.isSequence(history): history = len(history)
        self.last_args[model] = [start,stop,history,date] #It must be a list, not tuple
    
    def getDecimation(self):
        # The decimation method must be a function that takes a series of values and return a single value that summarizes the interval
        try:
            t = str(self._decimatecombo.currentText())
            m = dict(DECIMATION_MODES)[t]
            self.info('Decimation mode: %s,%s'%(m,t))
            return m
        except:
            self.warning(traceback.format_exc())
            return None
    
    def checkScales(self):
        if self.trend.getXDynScale() and not getattr(self.trend,'_configDialog',None):
            if getTrendBounds(self.trend,True)[-1]<(time.time()-3600):
                self.info('Disabling XDynScale when showing past data')
                self.trend.setXDynScale(False) 
        if not self.trend.getXDynScale():
            self.checkBuffers()

    def checkBuffers(self,*args):
        self.warning('CheckBuffers(%s)'%str(self.trend.trendSets.keys()))
        self.trend.doReplot()
        self.show_dialog(not self.dialog()._checked)
        for n,ts in self.trend.trendSets.iteritems():
            try:
                model = ts.getModel()
                if model in self.last_args: self.last_args[model][-1] = 0
                getArchivedTrendValues(ts,model,insert=True)
                ts.forceReading()
            except: self.warning(traceback.format_exc())
        self.trend.doReplot()
        
    def clearBuffers(self,*args):
        self.warning('ArchivedTrendLogger.clearBuffers(%s)'%str(args))
        self.last_args = {}
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
            self._dl = fandango.qt.QDialogWidget(buttons=True)
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
                        getattr(self.logger,severity,lambda m,s=severity:'%s:%s: %s'%(s.upper(),fandango.time2str(),m))
                    self.log_objs[severity](msg)
                except: pass
        if self.dialog():
            if msg!='+1': 
                msg = '%s:%s: %s'%(severity.upper(),fandango.time2str(),msg)
            if self.filters:
                msg = (fandango.filtersmart(msg,self.filters) or [''])[0]
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
    
###############################################################################
    
def emitHistoryChanged(trend_set):
    parent = getTrendObject(trend_set)
    #Initialization of refresh event:
    if getattr(trend_set,'_historyChangedSignal',None) is None:
        try:
            trend_set.info('PyTangoArchiving.Reader: Configuring historyChanged() event ...')
            parent.connect(parent,Qt.SIGNAL('historyChanged()'),parent.doReplot)
            trend_set._historyChangedSignal = True
        except: 
            trend_set.warning(traceback.format_exc())
    parent.info('PyTangoArchiving.Reader.emit(historyChanged())')
    parent._dirtyPlot = True
    #parent.emit(PyQt4.Qt.SIGNAL('historyChanged()'))
    def forceReplot(t=parent):
        t.info('PyTangoArchiving.Reader.forceReplot()')
        t._dirtyPlot = True
        t.doReplot()
    trend_set._historyChangedSignal = Qt.QTimer.singleShot(3000,forceReplot)
    
def get_history_buffer_from_model(trend_set,model):
    #by rsune@cells.es
    #THIS METHOD WAS USED IN TAU, PENDING TO ADD TO TAURUS
    self = trend_set
    parent = model.getParentObj()#parent should be a DeviceProxy instance
    if not self._history: self.trace('%s: creating new %s curve'%(time.ctime(),model.getFullName()))
    history = []
    try:
        if parent.is_attribute_polled(model.getSimpleName()): #polled
            self.info('%s:reading from polling buffer'%time.ctime())
            
            history_ = parent.attribute_history(model.getSimpleName(), int(total))
            lasttime = 0
            if self._history:
                lasttime = time2epoch(self._history[-1].time)
            history = [ h for h in history_ if lasttime<time2epoch(h.time) ]
            self.info(', %d values, %d are new'%(len(history_),len(history)))
            self._history=(self._history+history)[int(-total):]
    except Exception,e: 
        self.debug('Unexpected exception in TauTrendSet.handleEvent: '+str(e))
        self.traceback()
        #self._history = []
    return history
            
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
        
def getTrendObject(trend_set):
    return trend_set if hasattr(trend_set,'axisScaleDiv') else getObjectParent(trend_set)

def getObjectParent(obj):
    return getattr(obj,'_parent',None) or obj.parent()
        
def getTrendBounds(trend_set,rough=False):
    parent = getTrendObject(trend_set)
    lbound = parent.axisScaleDiv(parent.xBottom).lowerBound()
    ubound = parent.axisScaleDiv(parent.xBottom).upperBound()
    if not rough: ubound = min(time.time(),ubound)
    return [lbound,ubound]
    
def getTrendDimensions(self,value=None):
    if value is not None:
        v = getattr(value,'value',value)
        if numpy.isscalar(v): ntrends = 1
        else:
            try:
                v = float(v)
                ntrends = 1
            except: ntrends = len(v)
    else: ntrends = len(self._curves)
    return ntrends
    
def checkTrendBuffers(self,newdata=None,logger=None):
    logger = logger or self
    if None in (getattr(self,_b,None) for _b in ('_xBuffer','_yBuffer')):
        from taurus.core.util.containers import ArrayBuffer
        ntrends = max((getTrendDimensions(self),getTrendDimensions(self,newdata)))
        self._xBuffer = ArrayBuffer(numpy.zeros(min(128,self._maxBufferSize), dtype='d'), maxSize=self._maxBufferSize )
        self._yBuffer = ArrayBuffer(numpy.zeros((min(128,self._maxBufferSize), ntrends),dtype='d'), maxSize=self._maxBufferSize )
    if newdata is not None and self._xBuffer.maxSize()<(len(self._xBuffer)+len(newdata)):
        newsize = int(1.2*(self._xBuffer.maxSize()+len(newdata)))
        logger.info('reader.updateTrendBuffers(): Resizing xBuffer to %d to allocate archived values'%(newsize))
        #self.parent().setMaxDataBufferSize(max((newsize,self.parent().getMaxDataBufferSize()))) #<<<<< THIS METHOD DIDN'T WORKED!!!!
        self._xBuffer.setMaxSize(newsize),self._yBuffer.setMaxSize(newsize)
        #logger.info('new sizes : %s , %s'%(self._xBuffer.maxSize(),self._yBuffer.maxSize()))
    return self._xBuffer.maxSize()
    
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
        params = utils.parse_tango_model(model)
        tango_host,attribute = '%s:%s'%(params['host'],params['port']),'%s/%s'%(params['devicename'],params['attributename'])
    else:
        tango_host,attribute='',modelobj.getSimpleName() if hasattr(modelobj,'getSimpleName') else model.split(';')[-1]
    return tango_host,attribute,model

def getTrendGaps(trend,trend_set,bounds=None):
    """fins maximum gap in the shown part of the trend_set buffer and returns:
        start of the gap (t)
        end of the gap (t)
        zone where it appears(begin,middle,end)
        equivalent % of the total X axxis size
        
    The bounds argument can be used to restrict the check to an area smaller than X axis
    """
    import numpy as np
    now,tbounds,xbuffer = time.time()-10,getTrendBounds(trend),getattr(trend_set,'_xBuffer',[]).toArray()
    if bounds is not None and any(bounds): tbounds = bounds[0] or tbounds[0], bounds[1] or tbounds[1]
    bounds = min(tbounds[0],now),min(tbounds[1],now) #Ignoring the "future" part of the scale
    if not bounds[1]-bounds[0]: return bounds[0],bounds[1],ZONES.BEGIN,0.
    if not len(xbuffer): return bounds[0],bounds[1],ZONES.BEGIN,1.
    times = xbuffer[(xbuffer>bounds[0])&(xbuffer<bounds[1])] #~30ms for a 1e6 sample buffer
    if len(times)<2: return bounds[0],bounds[1],ZONES.BEGIN,1.
    first,last = (times[0],times[-1]) if len(times) else (now,bounds[1])
    gaps = (times[1:]-times[:-1])
    igaps = np.argsort(gaps)
    maxgap,start,end = gaps[igaps[-1]],times[igaps[-1]],times[igaps[-1]+1]
    if maxgap<(first-bounds[0]):
        start,end,zone = bounds[0],first,ZONES.BEGIN
    elif maxgap<(bounds[1]-last):
        start,end,zone = last,bounds[1],ZONES.END
    else:
        zone = ZONES.MIDDLE
    maxgap = end-start
    area = float(maxgap)/(bounds[1]-bounds[0])
    #trend.info('getTrendGaps(): bounds = %s ; gaps = %s'%(bounds,(start,end,zone,area)))
    return start,end,zone,area
    
def resetTrendBuffer(b,newsize,newdata=None):
    """ Cleans up the buffer, resizes and inserts new data """
    b.resizeBuffer(1)
    b.moveLeft(1)
    b.setMaxSize(newsize)
    if newdata is not None and len(newdata): b.extend(newdata)
    return len(b)

def updateTrendBuffers(self,data,logger=None):
    """
    This method implements decimation of archived and actual values when filling trend buffers
    It should also allow to patch non correlative inserts of archived data (inserting instead of extendLeft)
    """
    try:
        #self.curves_lock.acquire()
        import numpy,datetime,PyTangoArchiving.utils as utils
        from taurus.core.util.containers import ArrayBuffer
        logger = logger or self
        logmsg = lambda m: (fandango.printf(m),logger.warning(m))
        trend_set = self
        parent = logger.trend
        fromHistoryBuffer = data is not None and len(data) and hasattr(data[0],'time')
        ###Adding archiving values
        logger.info('In updateTrendBuffers(%d,fromHistoryBuffer=%s)'%(len(data or []),fromHistoryBuffer))
        if data is not None and len(data): 
            try:
                ntrends = self._checkDataDimensions(data[0].value 
                                        if fromHistoryBuffer 
                                        else data[0][1]) #It may clean existing buffers!
            except:
                print(data[0])
                raise
            newsize = checkTrendBuffers(self,data,logger)
            logger.debug('reader.updateTrendBuffers(): filling Buffer')
            try:
                if fromHistoryBuffer:
                    t = numpy.zeros(len(data), dtype=float)
                    y = numpy.zeros((len(data), ntrends), dtype=float)#self._yBuffer.dtype)
                    t[:] = [v.time.totime() for v in data]
                    y[:] = [v.value for v in data]
                else:
                    #CONVERT ALWAYS THE TWO ARRAYS SEPARATELY, np.array(data) is MUCH SLOWER
                    t = numpy.array([v[0] for v in data])
                    y = numpy.zeros((len(data), ntrends), dtype=float)
                    for i,v in enumerate(data):
                        try:
                            y[i] = v[1]
                        except Exception,e:
                            logmsg(e)
                    #y[:] = [v[1] for v in data]
                if (len(t) and numpy.max(t) or 0)>(len(self._xBuffer) and numpy.min(self._xBuffer) or fandango.END_OF_TIME): 
                    #History and current buffer overlap!; resorting data
                    t0 = time.time()
                    logmsg('updateBuffers(): Reorganizing the contents of the Trend Buffers!')
                    t = numpy.concatenate((t,self._xBuffer.toArray()))
                    y = numpy.concatenate((y,self._yBuffer.toArray()))
                    t_index = utils.sort_array(t,decimate=True,as_index=True)
                    t,y = t.take(t_index,0),y.take(t_index,0)
                    newsize = int(max((parent.DEFAULT_MAX_BUFFER_SIZE,1.5*len(t))))
                    resetTrendBuffer(self._xBuffer,newsize,t)
                    resetTrendBuffer(self._yBuffer,newsize,y)
                    logmsg('done in %f seconds, replotting'%(time.time()-t0))
                else: 
                    #No backtracking, normal insertion
                    self._xBuffer.extendLeft(t)
                    self._yBuffer.extendLeft(y)
            except Exception,e:
                import traceback #Import is needed!
                logger.warning('\tUnable to convert buffers[%d]! %s: %s'%(ntrends,data and data[0],traceback.format_exc()))
            
            #from PyQt4 import Qt
            pending = getattr(getattr(logger,'reader',None),'callbacks',None)
            if not pending:
                Qt.QApplication.instance().restoreOverrideCursor()
            emitHistoryChanged(self) #self.parent().replot() #To be done always, although it doesn't seem to be enough
    except Exception,e:
        import traceback
        logger.warning('updateBuffer failed: %s'%(traceback.format_exc()))
        return []
    finally:
        #self.curves_lock.release()
        pass
        
QT_CONNECTIONS = defaultdict(list)

def replaceQtConnection(qobj,signal,callback):
    import functools
    for i,t in enumerate(QT_CONNECTIONS[signal][:]):
        if t[0]==qobj:
            print 'disconnecting(%s,%s,%s)'%(qobj,signal,t[1])
            qobj.disconnect(qobj,Qt.SIGNAL(signal),t[1])
            #qobj.disconnect(qobj,Qt.SIGNAL(signal),0,0)
            QT_CONNECTIONS[signal].remove(t)
    qobj.connect(qobj,Qt.SIGNAL(signal),callback)
    QT_CONNECTIONS[signal].append((qobj,callback))

def getArchivedTrendValues(trend_set,model,start_date=0,stop_date=None,
            log='INFO',use_db=True,db_config='',decimate=True,
            multiprocess=USE_MULTIPROCESS,insert=False):
    """This method allows to extract the values from the archiving system either using HdbExtractor device servers or MySQL access (requires permissions).
    
    This method can be tested with the following code:
    def debug(s): print s
    trend = type('fake',(object,),{})()
    trend._history,type(trend).debug,trend._parent = [],(lambda c,s:debug(s)),type('',(object,),{'xIsTime':True} )()
    PyTangoArchiving.reader.getArchivedTrendValues(trend,'BO02/VC/SPBX-03/I1',time.time()-24*3600,time.time(),'DEBUG')
    
    From TaurusTrendSet is called just like: getArchivedTrendValues(self,model,insert=True)
    
    Arguments:
        trend_set ; a TaurusTrendSet object
        model
        start_date/stop_date = epoch or strings; start_date defaults to X axxis, stop_date defaults to now()
        log='INFO'
        use_db=True
        db_config=''
        decimate=True
        multiprocess=False
        insert=False ; but always True when called from a TaurusTrendSet
        
    """
    import functools
    logger_obj = trend_set
    try:
        tango_host,attribute,model = parseTrendModel(model)
        parent = getTrendObject(trend_set)
        logger_obj = ArchivedTrendLogger(parent,tango_host=tango_host,multiprocess=multiprocess)
        #logger_obj.info('< %s'%str((model,start_date,stop_date,use_db,decimate,multiprocess)))
        lasts = logger_obj.last_args.get(model,None)
        MARGIN = 60
        def hasChanged(prev,curr=None):
            v = all(curr) and (not prev or not any(prev[:2]) or any(abs(x-y)>MARGIN for x,y in zip(curr,prev)))
            return v #print(prev,curr,all(curr) and all(prev[:2]) and [abs(x-y) for x,y in zip(curr,prev)],v)
        logger,reader = logger_obj.info,logger_obj.reader
        logger_obj.debug('using reader: %s(%s)' %(type(reader),reader.schema))
        if not multiprocess and time.time() < STARTUP+STARTUP_DELAY:
            logger_obj.warning('PyTangoArchiving.Reader waiting until %s'%fandango.time2str(STARTUP+STARTUP_DELAY))
            return []
        if not parent.xIsTime:
            logger('PyTangoArchiving.Reader: Archiving is available only for trends')
            logger_obj.setLastArgs(model)
            return []
        if not reader or not reader.check_state(): #Cached check
            logger_obj.warning('Archiving readings not available!')
            logger_obj.setLastArgs(model)
            return []
        if not reader.is_attribute_archived(attribute): #Cached check
            if model not in logger_obj.last_args: logger('%s: attribute %s is not archived'%(time.ctime(),attribute))
            logger_obj.setLastArgs(model)
            return []
    except:
        logger_obj.error('Model parsing failed: %s'%traceback.format_exc())
        return []
    # Dates parsing ##########################################################################
    try:
        checkTrendBuffers(trend_set)
        if insert: logger_obj.debug('Current buffer size: %s'%len(trend_set._xBuffer))
        bounds = getTrendBounds(parent,rough=True)
        if lasts and lasts[2] and time.time()<lasts[3]+10.0:
            if bounds!=logger_obj.last_check:
                logger('PyTangoArchiving.Reader: Last %s query was %d (<10) seconds ago'%(model,time.time()-lasts[3]))
                logger_obj.last_check = bounds
            return []
        logger_obj.setLastArgs(model,*(lasts and lasts[:3] or [])) #<<<<<<<<<<< Overwrite lasts to store current timestamp
        lasts = logger_obj.last_args[model] #<<< obtain default values corrected
        if all((start_date,stop_date)): #Forcing overriding between 2 dates
            start_date,stop_date,zone,area = start_date,stop_date,ZONES.MIDDLE,1.
        else:
            start_date,stop_date,zone,area = getTrendGaps(parent,trend_set)
        args = 60*int(start_date/60),60*(1+int(stop_date/60)) #To simplify comparisons
        if stop_date-start_date<MARGIN or area<(.11,.05)[zone==ZONES.MIDDLE]:
            if hasChanged(lasts[:2],args): logger_obj.info('In getArchivedTrendValues(%s,%s,%s,%s): Interval too small, Nothing to read'%(model,start_date,stop_date,use_db))
            logger_obj.setLastArgs(model,args[0],args[1])
            try: logger_obj.trend.doReplot()
            except: traceback.print_exc()
            return []
        if lasts and not hasChanged(lasts[:2],args) and lasts[2]: #Data was already retrieved:
            logger('In getArchivedTrendValues(%s,%s,%s) already retrieved data ([%d] at %s) is newer than requested'%(
                attribute,args[0],args[1],len(trend_set._xBuffer),lasts))
            #logger_obj.setLastArgs(model,args[0],args[1],max(lasts[2],-1))#Reader keeps last_dates of query; here just keep last arguments
            try: logger_obj.trend.doReplot()
            except: traceback.print_exc()
            return []
    except:
        logger_obj.error('Date parsing failed: %s'%traceback.format_exc())
        return []
    # Data Retrieval ##########################################################################
    try:
        logger_obj.info('<3')
        logger_obj.info('In getArchivedTrendValues(%s, %s = %s, %s = %s)(%s): lasts=%s, getting new data (previous[%s]:%s at %s)'%(
            attribute,start_date,fandango.time2str(start_date),stop_date,fandango.time2str(stop_date),reader.schema,lasts and lasts[0],model,lasts,lasts[-1]))
        logger_obj.debug('prev %s != curr %s' % (lasts,args))
        Qt.QApplication.instance().restoreOverrideCursor()
        decimation = logger_obj.getDecimation()
        if not decimation:
            if logger_obj._nonescheck.isChecked(): decimation = fandango.arrays.notnone
            elif decimate: decimation = PyTangoArchiving.reader.data_has_changed
            
        if not multiprocess or not isinstance(reader,ReaderProcess):
            ###################################################################################
            history = reader.get_attribute_values(attribute,start_date,stop_date,
                asHistoryBuffer=False,decimate=decimation) or []
            (logger_obj.info if len(history) else logger_obj.debug)('getArchivedTrendValues(%s,%s,%s): %d %s readings: %s ...' % (
                attribute,start_date,stop_date,len(history),reader.schema,','.join([str(s) for s in history[:3]]) ))
                
            #DATA INSERTION INTO TRENDS IS DONE HERE!
            if insert: 
                updateTrendBuffers(trend_set,history,logger=logger_obj) #<<< it emits historyChanged event
                trend_set.emit(Qt.SIGNAL("dataChanged(const QString &)"), Qt.QString(trend_set.getModel()))
                logger_obj.info('Inserted %d values into %s trend buffer [%d]'%(len(history),attribute,len(trend_set._xBuffer)))
            logger_obj.setLastArgs(model,args[0],args[1],len(history))
            logger_obj.info('last_args = %s\n'%(logger_obj.last_args))
            return history #<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Success!
            ###################################################################################
        else: # If Multiprocess
            trend,tt = logger_obj.trend,(id(trend_set),attribute,start_date,stop_date)
            signal = "archivedDataIsReady"
            if not hasattr(reader,'Asked'): 
                reader.Asked = []
            if not hasattr(trend,'_processconnected'):
                trend._processconnected = True
                trend.connect(trend,Qt.SIGNAL(signal),lambda ts_data,lg=logger_obj:updateTrendBuffers(ts_data[0],ts_data[1],lg))
            if tt in reader.Asked:
                logger_obj.info('%s: query already being processed'%str(tt))
            else:
                reader.Asked.append(tt)
                logger_obj.info('%s: query sent to background process %s'%(str(tt),reader))
                Qt.QApplication.instance().setOverrideCursor(Qt.QCursor(Qt.Qt.WaitCursor))
                reader.get_attribute_values(attribute,callback = (
                        lambda q,ts=trend_set,lg=logger_obj,sg=signal,at=attribute,ref=tt,rd=reader:
                            #lambda q,a=attribute,p=parent,s=signal,r=reader:
                            (lg.info("... in ProcessCallback(%s)[%s]"%(ref,len(q) if q else q)),
                            lg.trend.emit(Qt.SIGNAL(sg),(ts,q)),
                            ref in rd.Asked and rd.Asked.remove(ref))
                        ),start_date=start_date,stop_date=stop_date,
                        asHistoryBuffer=False,decimate=decimation)

            logger_obj.setLastArgs(model,args[0],args[1],-1) #Dont use tuples here
            return []
    except:
        logger_obj.error('Exception in Reader.getArchivedTrendValues(%s): %s' % (model,traceback.format_exc()))
    #Default return if attribute is not archived or values were already returned
    return []


###############################################################################
# TaurusTrend -a helper for be4tter config

def get_archiving_trend(models=None,length=12*3600,show=False,n_trends=1):
    #class PressureTrend(TaurusTrend):
        #def showEvent(self,event):
            #if not getattr(self,'_tuned',False): 
                #setup_pressure_trend(self)
                #setattr(self,'_tuned',True)        
            #TaurusTrend.showEvent(self,event)
    global STARTUP_DELAY
    STARTUP_DELAY = 0
    try:from taurus.external.qt import Qwt5,Qt
    except:from PyQt4 import Qwt5,Qt

    qw = Qt.QWidget()
    qw.setLayout(Qt.QVBoxLayout())
    trends = [ArchivingTrendWidget() for i in range(n_trends)]
    
    try:
      for tt in trends:
        tt.setXDynScale(True)
        tt.setXIsTime(True)
        tt.setUseArchiving(True)
        tt.setModelInConfig(False)
        tt.disconnect(tt.axisWidget(tt.xBottom), Qt.SIGNAL("scaleDivChanged ()"), tt._scaleChangeWarning)
        xMax = time.time() #tt.axisScaleDiv(Qwt5.QwtPlot.xBottom).upperBound()
        rg = length #abs(self.str2deltatime(str(self.ui.xRangeCB.currentText())))
        xMin=xMax-rg
        tt.setAxisScale(Qwt5.QwtPlot.xBottom,xMin, xMax)
        if n_trends>1: qw.layout().addWidget(tt)
        elif show: tt.show()
      tt1 = trends[0]
      if models: 
          tt1.setModel(models)
          tt1.setWindowTitle(str(models))
    except:
        print 'Exception in set_pressure_trend(%s)'%tt
        print traceback.format_exc()
    if show and n_trends>1: qw.show()
    return qw if n_trends>1 else tt1
        
###############################################################################
            
if __name__ == "__main__":
    import sys
    from taurus.qt.qtgui.application import TaurusApplication
    app = TaurusApplication()
    
    aw = ArchivingTrendWidget()
    aw.show()
    sys.exit(app.exec_())
    
    args=sys.argv[1:]
    form = get_archiving_trend(models=args,n_trends=1,show=True)   
    form.show()
    #if no models are passed, show the data import dialog
    #if len(args) == 0:
        #form.showDataImportDlg()
    sys.exit(app.exec_())
