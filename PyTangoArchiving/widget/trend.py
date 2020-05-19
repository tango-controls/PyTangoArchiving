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

from PyTangoArchiving.widget.dialogs import (
    DatesWidget, ArchivedTrendLogger, QReloadDialog,
    getTrendObject, getObjectParent, getTrendBounds,
    parseTrendModel, MenuActionAppender, 
    DECIMATION_MODES, USE_MULTIPROCESS )

#################################################################################################
# Methods for enabling archiving values in TauTrends

try:
    fakeTrend = fn.Struct({
        '_parent':fn.Struct({'xIsTime':True}),'_history':[],
        'info':fn.printf,'error':fn.printf,'debug':fn.printf,
        'warning':fn.printf,
        })
except: pass

from PyTangoArchiving.reader import STARTUP
global STARTUP_DELAY
STARTUP_DELAY = 0.

MAX_QUERY_TIME = 3600*24*1000 #DISABLING THE BUNCHING SYSTEM (doesnt work)
MAX_QUERY_LENGTH = 65536*1024 #int(1e7)
MIN_REFRESH_PERIOD = 3.
MIN_WINDOW = 60

ZONES = fn.Struct({'BEGIN':0,'MIDDLE':1,'END':2})

def dateHasChanged(prev,curr=None):
    v = all(curr) and (not prev or not any(prev[:2]) 
                        or any(abs(x-y)>MIN_WINDOW 
                                for x,y in zip(curr,prev)))
    return v

class ArchivingTrendWidget(TaurusGroupBox):
    def __init__(self, parent = None, designMode = False):
        TaurusGroupBox.__init__(self, parent) #, designMode)
        self.setTitle('Archiving Trend')
        self.setLayout(Qt.QVBoxLayout())
        self._trend = ArchivingTrend(parent=self) #,designMode = designMode)
        self._datesWidget = DatesWidget(trend=self._trend,parent=self,
            layout=Qt.QHBoxLayout)
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

class ArchivingTrend(TaurusTrend):
  
    MENU_ACTIONS =         [
        ('_datesWidgetOption','Show Dates Widget','showDatesWidget'),
         #(lambda o:o.showDatesWidget())),
        #('_zoomBackOption','Zoom Back (middle click)','_zoomBack'),
         #(lambda o:o._zoomBack())),
        ('_setAxisFormatOption','Set Y Axis Format',(lambda o:o._showAxisFormatDialog())),
        ('_pausedOption','Pause (P)',(lambda o:o.setPaused(not o.isPaused()))),
        ]
  
    def __init__(self, parent = None, designMode = False):
        actions = [a[1] for a in MenuActionAppender.ACTIONS]
        print '>'*80+'\n'+str(MenuActionAppender.ACTIONS)
        TaurusTrend.__init__(self,parent) #,designMode)
        self.resetDefaultCurvesTitle()
        self.setXDynScale(True)
        self.setXIsTime(True)
        self.setUseArchiving(True)
        self.setModelInConfig(False)
        self.disconnect(self.axisWidget(self.xBottom), Qt.SIGNAL("scaleDivChanged ()"), self._scaleChangeWarning)
        #ArchivedTrendLogger(self,tango_host=fn.get_tango_host(fqdn=True),multiprocess=USE_MULTIPROCESS)
        ArchivedTrendLogger(self,multiprocess=USE_MULTIPROCESS, value_setter=getArchivedTrendValues)
        
        #self.MENU_ACTIONS = [
            #('_zoomBackOption','Zoom Back (middle click)',(lambda o:o._zoomBack())),
            #('_setAxisFormatOption','Set Y Axis Format',(lambda o:o._showAxisFormatDialog())),
            #('_pausedOption','Pause (P)',(lambda o:o.setPaused(not o.isPaused()))),
            #]
        #self.MENU_ACTIONS.insert(0,)
        
    def close(self):
        try:
            self.getArchivedTrendLogger.dialog().close()
        except:
            pass
        TaurusTrend.close(self)
      
    def getArchivedTrendLogger(self,model=None):
        host = fn.get_tango_host(model or None, fqdn=True)
        return ArchivedTrendLogger(self,tango_host=host, value_setter=getArchivedTrendValues)
      
    def setForcedReadingPeriod(self, msec=None, tsetnames=None):
        '''Sets the forced reading period for the trend sets given by tsetnames.
        
        :param msec: (int or None) period in milliseconds. If None passed, the user will be 
                     prompted
        :param tsetnames: (seq<str> or None) names of the curves for which the forced 
                          reading is set. If None passed, this will be set for all 
                          present *and future* curves added to this trend
    
        .. seealso: :meth:`TaurusTrendSet.setForcedReadingPeriod`
        '''
        self.warning('*'*1200)
        self.warning('setForcedReadingPeriod(%s,%s)' % (msec,tsetnames))
        
        if msec is None:
            msec = self._forcedReadingPeriod
            try: #API changed in QInputDialog since Qt4.4
                qgetint = Qt.QInputDialog.getInt
            except AttributeError:
                qgetint = Qt.QInputDialog.getInteger
            msec,ok = qgetint(self, 'New forced reading period', 
                'Enter the new period for forced reading (in ms).\n '
                'Enter "0" for disabling', 
                max((0,msec,3000)), 0, 604800000, 100)
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
                #tset.setEventFilters([])
                        
        finally:
            traceback.print_exc()
            self.curves_lock.release()
  
    def _showAxisFormatDialog(self,axis=None):
        try:
            import PyQt4.Qwt5,PyQt4.Qt
            axis = axis or PyQt4.Qwt5.QwtPlot.yLeft
            qi = Qt.QInputDialog.getText(None,"Axis Format",
                "Enter format for Axis labels (\%6.2f):")
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
        
    def applyNewDates(self,dates=None):
        """
        Dates could be a tuple (start,end) or just (end,)
        
        #If two dates are passed, they are start and end
        #If a single date is passed, then it is just the range
        """
        try:
            logger = self.getArchivedTrendLogger()
            # Update/Pause the Trend could cause unexpected behaviours!
            #   self.setForcedReadingPeriod(3000)
            #   self.setPaused(True)
            
            if dates is not None:
                self._datesWidget.setRange(dates[-1])
                if len(dates)>1:
                    self._datesWidget.setStartDate(dates[0])
                
            t0 = self._datesWidget.getStartDate()
            t1 = self._datesWidget.getRange()
            logger.warning('applyNewDates(%s,%s)'%(t0, t1))
        
            if t0 is None: # start date is relative
                now = time.time()
                t0, t1 = time.time()-abs(t1), time.time()
            else:
                t0 = t0 if t0>0 else time.time() + t0
                t0, t1 = t0, t0 + abs(t1)
                
            if t0 < fn.now() < t1 and t1-t0 > utils.MAX_RESOLUTION:
                # For periods > 3h set readings at 10s
                self.setForcedReadingPeriod(10000.)
                    
            if t1-t0 > 365*86400:
                v = Qt.QMessageBox.warning(self,'Warning!',
                    'Reading an interval so big may hung your PC!!',
                    Qt.QMessageBox.Ok|Qt.QMessageBox.Cancel)
            
                if t0 < 1000 or v == Qt.QMessageBox.Cancel:
                    return
        
            if t0 is not None:
                logger.warning('applyNewDates(%s,%s)'%(fn.time2str(t0),fn.time2str(t1)))
                #Set Axis Scale already triggers Check Buffers!!!!
                self.setAxisScale(Qwt5.QwtPlot.xBottom, t0, t1)

            ## DONT EVER APPLY SETPAUSED(TRUE); IT WILL NO ALLOW QT TO REFRESH
            #logger.warning('Setting XDynScale != Paused = %s' % str(t1<time.time()))
            #self.setXDynScale(t1>time.time()) #%It causes weird effects
            #self.setPaused(t1<time.time())
                
            self.emit(Qt.SIGNAL("refreshData"))
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

            if not ui:
                self._datesWidget = Qt.QDialog()
                self._datesWidget.setLayout(Qt.QVBoxLayout())
                dw = DatesWidget(self)#,self.legend(),Qt.QVBoxLayout())
                self._datesWidget.layout().addWidget(dw)
            
            if show: 
                self._datesWidget.show()
            self.replot()
            return

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

    
###############################################################################
    
def emitHistoryChanged(trend_set):
    parent = getTrendObject(trend_set)
    #Initialization of refresh event:
    if getattr(trend_set,'_historyChangedSignal',None) is None:
        try:
            trend_set.info('PyTangoArchiving.Reader: '
                'Configuring historyChanged() event ...')
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
    trend_set._historyChangedSignal = Qt.QTimer.singleShot(1500,forceReplot)
    
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
        logger.warning('reader.updateTrendBuffers(): Resizing xBuffer to %d to allocate archived values'%(newsize))
        #self.parent().setMaxDataBufferSize(max((newsize,self.parent().getMaxDataBufferSize()))) #<<<<< THIS METHOD DIDN'T WORKED!!!!
        self._xBuffer.setMaxSize(newsize),self._yBuffer.setMaxSize(newsize)
        #logger.info('new sizes : %s , %s'%(self._xBuffer.maxSize(),self._yBuffer.maxSize()))
        
    return self._xBuffer.maxSize()

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
    
    if tbounds[1] < tbounds[0]:
        print('#'*80)
        print('System date seems wrong!!!: %s'%fn.time2str(tbounds[1]))
    
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
    trend.info('getTrendGaps(): bounds = %s ; gaps = %s'
                  % (bounds,(start,end,zone,area)))
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
    This method implements decimation of archived and actual values 
    when filling trend buffers
    It should also allow to patch non correlative inserts of archived data 
    (inserting instead of extendLeft)
    """
    self.warning('In updateTrendBuffers(...)')
    t = []
    ntrends = 1
    try:
        #self.curves_lock.acquire()
        import numpy,datetime,PyTangoArchiving.utils as utils
        from taurus.core.util.containers import ArrayBuffer
        logger = logger or self
        logmsg = lambda m: (fn.printf(m),self.warning(m)) #logger.warning
        trend_set = self
        parent = getattr(logger,'trend',logger)
        fromHistoryBuffer = data is not None and len(data) and hasattr(data[0],'time')
        
        if data is not None and len(data): 
            ###Adding archiving values
            try:
                #It may clean existing buffers!
                value = data[0].value if fromHistoryBuffer else data[0][1]
                ntrends = self._checkDataDimensions(value) 
            except:
                logmsg(str(data[0]))
                raise
            
            newsize = checkTrendBuffers(self,data,logger)
            logger.warning('reader.updateTrendBuffers(%s): filling Buffer with %s' % (ntrends, newsize))
            try:
                if fromHistoryBuffer:
                    t = numpy.zeros(len(data), dtype=float)
                    y = numpy.zeros((len(data), ntrends), dtype=float)#self._yBuffer.dtype)
                    t[:] = [v.time.totime() for v in data]
                    y[:] = [v.value for v in data]
                else:
                    #CONVERT ALWAYS THE TWO ARRAYS SEPARATELY, np.array(data) is MUCH SLOWER
                    t = numpy.array([v[0] for v in data])
                    if 0: #ntrends == 1:
                        y = numpy.array([v[1] for v in data], dtype=float)
                    else:
                        y = numpy.zeros((len(data), ntrends), dtype=float)
                        for i,v in enumerate(data):
                            # Iterating will avoid getting stuck in errors
                            try:
                                y[i] = v[1]
                            except Exception,e:
                                logmsg(e)
                                
                overlap = ((len(t) and numpy.max(t) or 0) > 
                            (len(self._xBuffer) and numpy.min(self._xBuffer) 
                                or fn.END_OF_TIME ))
                minstep = abs(t[-1] - t[0]) / 1081.
                logger.warning('In updateTrendBuffers('
                        '(%s - %s)[%d] \n\t+ (%s - %s)[%d],'
                        'fromHistoryBuffer=%s, minstep=%s, overlap=%s)' % (
                            len(self._xBuffer) and time2str(self._xBuffer[0]),
                            len(self._xBuffer) and time2str(self._xBuffer[-1]),
                            len(self._xBuffer) or 0,
                            time2str(t[0]),
                            time2str(t[-1]),
                            len(t) or 0,
                            fromHistoryBuffer,minstep,overlap))
                
                t0 = time.time()
                #No backtracking, normal insertion
                t_index = utils.sort_array(t,decimate=True,as_index=True,
                                            minstep=minstep)
                t,y = t.take(t_index,0),y.take(t_index,0)                                

                if overlap: 
                    #History and current buffer overlap!; resorting data
                    t = numpy.concatenate((t,self._xBuffer.toArray()))
                    y = numpy.concatenate((y,self._yBuffer.toArray()))
                    t_index = utils.sort_array(t,decimate=False,as_index=True)
                    t,y = t.take(t_index,0),y.take(t_index,0)
                    newsize = int(max((parent.DEFAULT_MAX_BUFFER_SIZE,
                                       1.5*len(t))))
                    resetTrendBuffer(self._xBuffer,newsize,t)
                    resetTrendBuffer(self._yBuffer,newsize,y)
                else: 
                    self._xBuffer.extendLeft(t)
                    self._yBuffer.extendLeft(y)
                    
                #logger.warning('t[%d]: %s - %s' 
                                #% (len(t), time2str(t[0]), time2str(t[-1])))                   
                    
                logmsg('done in %f seconds, replotting'%(time.time()-t0))
                logger.warning(' ... new data length ('
                        '(%s - %s)[%d] \n\t+ (%s - %s)[%d],'
                        'fromHistoryBuffer=%s, minstep=%s)' % (
                            len(self._xBuffer) and time2str(self._xBuffer[0]),
                            len(self._xBuffer) and time2str(self._xBuffer[-1]),
                            len(self._xBuffer) or 0,
                            len(t) and time2str(t[0]),
                            len(t) and time2str(t[-1]),
                            len(t) or 0,
                            fromHistoryBuffer,minstep))
                        
                #logger.warning('new data length: %s, (%s,%s), (%s,%s)' 
                               #% (len(t), t[0], t[-1], y[0], y[-1]))
            except Exception,e:
                import traceback #Import is needed!
                logger.warning('\tUnable to convert buffers[%d]! %s: %s'
                        %(ntrends,data and data[0],traceback.format_exc()))
                t = []
            
            pending = getattr(getattr(logger,'reader',None),'callbacks',None)
            if not pending:
                Qt.QApplication.instance().restoreOverrideCursor()
                
            emitHistoryChanged(self)
            
    except Exception,e:
        import traceback
        logger.warning('updateBuffer failed: %s'%(traceback.format_exc()))
    finally:
        print('-'*80)
        pass #self.curves_lock.release()

    return len(t)
        
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
            multiprocess=USE_MULTIPROCESS,insert=False,forced=False):
    """
    This method allows to extract the values from the archiving system 
    either using HdbExtractor device servers or MySQL access 
    (requires permissions).
    
    This method can be tested with the following code::

        debug = fandango.printf()
        trend = type('fake',(object,),{})()
        trend._history,type(trend).debug,trend._parent = [], \
            (lambda c,s:debug(s)),type('',(object,),{'xIsTime':True} )()
        PyTangoArchiving.reader.getArchivedTrendValues(trend,'BO02/VC/SPBX-03/I1',
            time.time()-24*3600,time.time(),'DEBUG')
    
    From TaurusTrendSet is called just like: 
        getArchivedTrendValues(self,model,insert=True)
    
    Arguments:
        trend_set ; a TaurusTrendSet object
        model
        start_date/stop_date = epoch or strings; 
            #start_date defaults to X axxis, stop_date defaults to now()
        log='INFO'
        use_db=True
        db_config=''
        decimate=True
        multiprocess=False
        insert=False ; but always True when called from a TaurusTrendSet
        
    """
    import functools
    logger_obj = trend_set
    t00 = time.time()
    N = 0
    
    try:
        tango_host,attribute,model = parseTrendModel(model)
        parent = getTrendObject(trend_set)
        logger_obj = ArchivedTrendLogger(parent,tango_host=tango_host,
            multiprocess=multiprocess, value_setter = getArchivedTrendValues)
        
        #logger_obj.info('< %s'%str((model,start_date,stop_date,
        # use_db,decimate,multiprocess)))
        lasts = logger_obj.last_args.get(model,None)
        
        logger = logger_obj.warning
        reader = logger_obj.reader
        logger_obj.debug('using reader: %s(%s)' %(type(reader),reader.schema))
        
        if not multiprocess and time.time() < STARTUP+STARTUP_DELAY:
            logger_obj.warning('PyTangoArchiving.Reader waiting until %s'
                               %fn.time2str(STARTUP+STARTUP_DELAY))
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
            if model not in logger_obj.last_args: 
                logger('%s: attribute %s is not archived'%(time.ctime(),attribute))
            logger_obj.setLastArgs(model)
            return []
        
    except:
        logger_obj.error('Model parsing failed: %s'%traceback.format_exc())
        return []
    
    ###########################################################################
    #trend_set.warning('Dates parsing at + %f' % (time.time()-t00))
    try:
        # Create/Resize buffers if needed
        checkTrendBuffers(trend_set)
        if insert: 
            logger_obj.debug('Current %s buffer size: %s'
                %(attribute,len(trend_set._xBuffer)))

        # Check trend X scale (not data, just axxis)
        bounds = getTrendBounds(parent,rough=True)
            
        if lasts and time.time()<lasts[3]+MIN_REFRESH_PERIOD: #and lasts[2]
            if bounds!=logger_obj.last_check:
                logger_obj.debug('PyTangoArchiving.Reader: Last %s query was %d < %d'
                    ' seconds ago'%(attribute,time.time()-lasts[3],
                                       MIN_REFRESH_PERIOD))
                logger_obj.last_check = bounds
            return []
        
        # Overwrite lasts to store current timestamp
        logger_obj.setLastArgs(model,*(lasts and lasts[:3] or [])) 
        lasts = logger_obj.last_args[model] #<<< obtain default values corrected
        
        if forced:
            start_date,stop_date = bounds
            
        elif all((start_date,stop_date)): #Forcing overriding between 2 dates
            start_date,stop_date,zone,area = \
                        start_date,stop_date,ZONES.MIDDLE,1.
        else:
            start_date,stop_date,zone,area = \
                        getTrendGaps(parent,trend_set)

        # Forcing the update of data by bunches, 
        # it should be combined with gaps!!!
        if stop_date-start_date > MAX_QUERY_TIME:
            logger_obj.warning('<-'*40+'\n'+
                'In getArchivedTrendValues(%s,%s,%s,%s): '
                'Interval too big, restricted to %d rows'
                %(attribute,start_date,stop_date,use_db,MAX_QUERY_LENGTH))
            
            # Fills using data from the end of the query
            N = - MAX_QUERY_LENGTH
            logger_obj.last_args[model][2] = 0

        #To simplify comparisons
        args = 60*int(start_date/60),60*(1+int(stop_date/60)) 
        
        if not forced:
            #Check GAPS
            if stop_date-start_date<MIN_WINDOW or area<(.11,.05)[zone==ZONES.MIDDLE]:
                if dateHasChanged(lasts[:2],args): 
                    logger_obj.debug('In getArchivedTrendValues(%s,%s,%s,%s): '
                        'Interval too small, Nothing to read'
                        %(attribute,start_date,stop_date,use_db))
                logger_obj.setLastArgs(model,args[0],args[1])
                try:
                    logger_obj.trend.doReplot()
                except: 
                    traceback.print_exc()
                return []
            
            if lasts and not dateHasChanged(lasts[:2],args) and lasts[2]: 
                #Data was already retrieved:
                logger('In getArchivedTrendValues(%s,%s,%s) already retrieved data'
                        '([%s] at %s) is newer than requested'%(
                        attribute,args[0],args[1],len(trend_set._xBuffer),lasts))
                        
                #Reader keeps last_dates of query; here just keep last arguments                    
                #logger_obj.setLastArgs(model,args[0],args[1],max(lasts[2],-1))
                try: logger_obj.trend.doReplot()
                except: traceback.print_exc()
                return []
    except:
        logger_obj.error('Date parsing failed: %s'%traceback.format_exc())
        return []
    
    ###########################################################################
    #trend_set.warning('Data retrieval at + %f' % (time.time()-t00))
    was_paused = parent.isPaused()
    try:
        logger_obj.warning('-'*40+'\n'*8)
        logger_obj.warning('In getArchivedTrendValues(%s, %s = %s, %s = %s, forced=%s)(%s): '
            'lasts=%s, getting new data (previous[%s]:%s at %s)'%(
            attribute,start_date,fn.time2str(start_date),stop_date,
            fn.time2str(stop_date),forced,reader.schema,lasts and lasts[0],
            model,lasts,lasts[-1]))
        logger_obj.debug('prev %s != curr %s' % (lasts,args))
        
        Qt.QApplication.instance().restoreOverrideCursor()
        
        # decimation = method, decimate = whether to decimate or not
        decimation = logger_obj.getDecimation()
        logger_obj.warning('decimation = %s' % str(decimation))
        if not decimation: # RAW or client-side
            if decimation is not None: #RAW
                if logger_obj.getNonesCheck(): 
                    decimation = fn.arrays.notnone
                elif decimate: 
                    decimation = PyTangoArchiving.reader.data_has_changed
        if decimation:
            if decimation in ('AUTO','0'):
                decimation = (stop_date - start_date) / utils.MAX_RESOLUTION
            if fn.isNumber(decimation):
                decimation = int(decimation)            
                    
        logger_obj.warning('decimation = %s' % str(decimation))
            
        if not multiprocess or not isinstance(reader,ReaderProcess):
            Qt.QApplication.instance().setOverrideCursor(
                                    Qt.QCursor(Qt.Qt.WaitCursor))
            
            ###################################################################
            tr = fn.now()
            history = reader.get_attribute_values(model,start_date,stop_date,
                N=N,asHistoryBuffer=False,decimate=decimation) or []
            
            #(logger_obj.info if len(history) else logger_obj.debug)(
            logger_obj.warning( #@debug
                'trend.%s.get_attribute_values(%s,%s,%s,%s,%s):'
                    '\n\t%d %s readings in %f s: %s ...\n' % (reader.schema,
                    model,time2str(start_date),time2str(stop_date),
                    decimation,N,len(history),reader.schema,fn.now()-tr,
                    ','.join([str(s) for s in history[:3]]) ))
                    
            #logger_obj.warning('%s , %s , %s' % (start_date,bounds[0],start_date<=bounds[0]))

            # THIS CHECK IS ONLY FOR BUNCHED QUERIES (N!=0)
            if ((0 < len(history) < abs(N) and area>(.11,.05)[zone==ZONES.MIDDLE])
                or start_date<=bounds[0]):
                # Windowed query was finished, stop refreshing
                check = fn.tango.check_attribute(model,readable=True)
                #logger_obj.warning(check)
                if not check:
                    logger_obj.warning('Pausing %s ...'%attribute)
                    taurus.Attribute(model).deactivatePolling()
                    #was_paused = True
                    
                
            #DATA INSERTION INTO TRENDS IS DONE HERE!
            if insert: 
                # historyChanged event emits here
                h = updateTrendBuffers(trend_set,history,logger=logger_obj) 
                trend_set.emit(Qt.SIGNAL("dataChanged(const QString &)"), 
                               Qt.QString(trend_set.getModel()))
                 #@debug
                logger_obj.warning('Inserted %s values into %s trend [%s]'
                        %(h,attribute,len(trend_set._xBuffer)))
            else:
                h = len(history)

            logger_obj.setLastArgs(model,args[0],args[1],h)
            logger_obj.debug('last_args = %s\n'%(logger_obj.last_args)) #@debug
            
            if insert: # THIS MUST BE DONE AFTER UPDATING LAST ARGS!
                logger_obj.trend.emit(Qt.SIGNAL("refreshData"))                
                #c = logger_obj.recount
                #logger_obj.recount += 1
                #logger_obj.warning('forcing read %d ...' % c)
                #trend_set.forceReading()
                #logger_obj.warning('forcing read %d ... done' % c)
                
            #logger('Return history[%d] at + %f' % (h, time.time()-t00))
            return history if history is not None else [] # Success!
        
            ###################################################################

        else: # If Multiprocess
            #MULTIPROCESS IS USED BY PYEXTRACTOR, BE CAREFUL!!
            
            raise Exception('Reader.Multiprocess Disabled!')
            
            #trend,tt = logger_obj.trend,(id(trend_set),attribute,start_date,stop_date)
            #signal = "archivedDataIsReady"
            #if not hasattr(reader,'Asked'): 
                #reader.Asked = []
            #if not hasattr(trend,'_processconnected'):
                #trend._processconnected = True
                #trend.connect(trend,Qt.SIGNAL(signal),
                        #lambda ts_data,lg=logger_obj:updateTrendBuffers(
                                                    #ts_data[0],ts_data[1],lg))
            #if tt in reader.Asked:
                #logger_obj.info('%s: query already being processed'%str(tt))
            #else:
                #reader.Asked.append(tt)
                #logger_obj.info('%s: query sent to background process %s'
                                #%(str(tt),reader))
                #Qt.QApplication.instance().setOverrideCursor(
                                #Qt.QCursor(Qt.Qt.WaitCursor))
                #reader.get_attribute_values(attribute,callback = (
                        #lambda q,ts=trend_set,lg=logger_obj,sg=signal,
                            #at=attribute,ref=tt,rd=reader:
                            ##lambda q,a=attribute,p=parent,s=signal,r=reader:
                            #(lg.info("... in ProcessCallback(%s)[%s]"
                                     #%(ref,len(q) if q else q)),
                            #lg.trend.emit(Qt.SIGNAL(sg),(ts,q)),
                            #ref in rd.Asked and rd.Asked.remove(ref))
                            #),
                        #start_date=start_date,stop_date=stop_date,
                        #asHistoryBuffer=False,decimate=decimation)

            #logger_obj.setLastArgs(model,args[0],args[1],-1) #Dont use tuples here
            #logger('Return multiprocess at + %f' % (time.time()-t00))            
            #return []
            
    except:
        logger_obj.error('Exception in Reader.getArchivedTrendValues(%s): %s' 
                         % (model,traceback.format_exc()))
        
    finally:
        parent.setPaused(was_paused)
        Qt.QApplication.instance().restoreOverrideCursor()        
        try:
            parent._pauseAction.setChecked(was_paused)
        except:
            logger_obj.warning(traceback.format_exc())

    ##Default return if attribute is not archived or values were already returned
    return []


###############################################################################
# TaurusTrend -a helper for be4tter config

def get_archiving_trend(models=None,length=12*3600,show=False,n_trends=1):
    global STARTUP_DELAY
    STARTUP_DELAY = 0

    from fandango.qt import Qwt5,Qt

    qw = Qt.QWidget()
    qw.setLayout(Qt.QVBoxLayout())
    trends = [ArchivingTrendWidget() for i in range(n_trends)]
    
    try:
      for tt in trends:
        tt.setXDynScale(True)
        tt.setXIsTime(True)
        tt.setUseArchiving(True)
        tt.setModelInConfig(False)
        tt.disconnect(tt.axisWidget(tt.xBottom), 
            Qt.SIGNAL("scaleDivChanged ()"), tt._scaleChangeWarning)
        xMax = time.time() #tt.axisScaleDiv(Qwt5.QwtPlot.xBottom).upperBound()
        rg = length
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
    #from taurus.qt.qtgui.application import TaurusApplication
    from fandango.qt import Qt
    from taurus.qt.qtcore.util.emitter import TaurusEmitterThread
    app = Qt.QApplication([]) #TaurusApplication()
    
    opts = dict(a.split('=',1) if  '=' in a else (a,True)
            for a in sys.argv[1:] if a.startswith('-'))
    args = [a for a in sys.argv[1:] if a not in opts]
    print(args)
    args = fn.join(fn.find_attributes(a) for a in args)
    print(args)
    
    aw = ArchivingTrendWidget()
    aw.show()
    aw._trend.setModel(args)
    if '--range' in opts:
        dates = opts['--range'].split(',')
    else:
        dates = ('1h',)

    print('Setting trend range to %s' % str(dates))
    aw._trend.applyNewDates(dates)
    sys.exit(app.exec_())
    
    form = get_archiving_trend(models=args,n_trends=1,show=True)   
    form.show()
    #if no models are passed, show the data import dialog
    #if len(args) == 0:
        #form.showDataImportDlg()
    sys.exit(app.exec_())
