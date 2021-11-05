#!/usr/bin/env python

#############################################################################
##
# This file is part of Taurus
##
# http://taurus-scada.org
##
# Copyright 2011 CELLS / ALBA Synchrotron, Bellaterra, Spain
##
# Taurus is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
##
# Taurus is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
##
# You should have received a copy of the GNU Lesser General Public License
# along with Taurus.  If not, see <http://www.gnu.org/licenses/>.
##
#############################################################################

import time, sys, traceback
import threading
from taurus.external.qt import Qt
import pyqtgraph as pg
from taurus.qt.qtgui.application import TaurusApplication
from taurus.qt.qtgui.taurusgui import TaurusGui
from taurus.qt.qtgui.container import TaurusMainWindow, TaurusWidget
from taurus_tangoarchiving.tangoarchivingvalidator import TangoArchivingAttributeNameValidator
from taurus_tangoarchiving.tangoarchivingvalidator import str2localtime
from taurus_tangoarchiving.widget.tangoarchivingmodelchooser import TangoArchivingModelSelectorItem
from taurus_tangoarchiving.widget.tangoarchivingtools import TangoArchivingTimeSelector
from taurus.core.taurushelper import getValidatorFromName

import fandango
import fandango as fn
import fandango.qt
from operator import isSequenceType

try:
    from taurus.qt.qtgui.tpg import (TaurusPlot,
                                     DateAxisItem,
                                     TaurusPlotDataItem)
    from taurus.qt.qtgui.tpg.curvesmodel import TaurusItemConf
except ImportError:
    raise Exception('Missing dependency: taurus_pyqtgraph')

class ArchivingWidget(TaurusWidget): #Qt.QWidget
    
    addXYModelsSig = Qt.pyqtSignal(list,str,str)
    
    def __init__(self,parent=None):
        TaurusWidget.__init__(self,parent=parent)

        self.plot = ArchivingPlot(self)
        self.legend = ArchivingLegend(self.plot)
        self.modelchooser = None
        self.plot.updateSig[bool,bool].connect(self.updateAll)
        self.t0,self.t1 = 0,0
        msi = self.modelchooser
        if msi:
            # Connect button
            msi.modelsAdded.connect(onAddXYModel)
            
        self.pb = Qt.QProgressBar()
        self.pb.setGeometry(0, 0, 300, 25)            
        
        self.info('building layout')
        self.setLayout(Qt.QVBoxLayout())
        self.tc = TangoArchivingTimeSelector()

        l1 = Qt.QSplitter()
        l1.addWidget(self.plot)
        self.plot.setTimeChooser(self.tc)
        l1.addWidget(self.legend)

        l0 = Qt.QWidget()
        l0.setLayout(Qt.QHBoxLayout())
        l0.layout().addWidget(self.tc)
        
        self.refresh = Qt.QPushButton(Qt.QIcon.fromTheme("view-refresh"),
                            "refresh tgarch curves")
        self.refresh.clicked.connect(self.onRefresh)
        
        self.layout().addWidget(l0)
        l0.layout().addWidget(self.refresh)
        self.layout().addWidget(l1,10)
        self.layout().addWidget(self.pb)        
        
        self.updateProgressBar()
        self.addXYModelsSig.connect(self.addXYModels)
            
    @staticmethod
    def run(models, t0, t1):
        app = TaurusApplication(app_name='tpgArchiving')
        gui = ArchivingWidget()
        #gui.setTimes(args[1],args[2])
        #gui.setModel(args[0])
        gui.show()
        gui.addXYModelsSig.emit(list(models),t0,t1)
        app.exec_()
        
    def setTimes(self,t0,t1):
        pass
    
    def addXYModels(self,attrs,t0=None,t1=None):
        """
        Convert model, dates to 
        'tgarch://alba03.cells.es:10000/sr/id/scw01/pressure?db=*;t0=2019-11-11T11:41:59;t1=2020-01-03T09:03:03;ts',
        """
        c = self.cursor()
        self.setCursor(Qt.Qt.WaitCursor)
        attrs = fn.toList(attrs)

        if not t0 and not t1 and not self.t0 and not self.t1:
            t0,t1 = self.tc.getTimes()

        if t0 and t1:
            t0 = t0 if fn.isNumber(t0) else fn.str2time(t0,relative=True)
            t1 = t1 if fn.isNumber(t1) else fn.str2time(t1,relative=True)
            self.t0,self.t1 = fn.time2str(t0,iso=1),fn.time2str(t1,iso=1)
            self.t0 = self.t0.replace(' ','T')
            self.t1 = self.t1.replace(' ','T')
        
        ms = []
        for attr in attrs:
            attr = fn.tango.get_full_name(attr,fqdn=True)
            attr = attr.replace('tango://','')
            q = 'tgarch://%s?db=*;t0=%s;t1=%s' % (attr,self.t0,self.t1)
            m = (q+';ts',q)
            ms.append(m)
            
        self.plot.onAddXYModel(ms)
        self.setCursor(c)
        
    addModels = addXYModels #For backwards compatibility
        
    def getTimes(self):
        return self.t0,self.t1
        
    ###########################################################################
    # Create tgarch tool bar
    ###########################################################################

    def onRefresh(self):
        # Update progress bar
        self.updateProgressBar(False)
        t1 = threading.Thread(target=self._onRefresh)
        t1.start()

    def _onRefresh(self):
        t0, t1 = self.tc.getTimes()
        # Validate models
        v = TangoArchivingAttributeNameValidator()
        query = "{0};t0={1};t1={2}"
        
        for curve in self.plot.getPlotItem().listDataItems():

            if isinstance(curve, TaurusPlotDataItem):
                ymodel = curve.getModel()
                # tgarch attr
                if v.getUriGroups(ymodel).get('scheme') != 'tgarch':
                    continue
                fullname, _, _ = v.getNames(ymodel)
                bmodel, current_query = fullname.split('?')
                db = current_query.split(';')[0]
                q = query.format(db, t0, t1)
                model = "{0}?{1}".format(bmodel, q)
                xmodel = "{};ts".format(model)
                curve.setModel(None)
                curve.setXModel(None)
                curve.setModel(model)
                curve.setXModel(xmodel)
                
        self.updateAll(legend=False)
        

    def updateProgressBar(self, stop=True):
        if stop is True:
            final = 1
        else:
            final = 0
        self.pb.setRange(0, final)        
        
    ###########################################################################
    # Helper
    ###########################################################################
    def updateAll(self,legend=True,stop=True):
        print('updateAll(%s,%s)' % (legend,stop))
        # Update legend
        if legend is True:
            try:
                self.legend.updateExternalLegend()
            except:
                traceback.print_exc()

        # run plot auto range
        time.sleep(0.2)  # Wait till models are loading
        self.plot.plot_items.getViewBox().menu.autoRange()
        # Stop progress bar
        self.updateProgressBar(stop=stop)
        
class ArchivingPlot(TaurusPlot):
    
    updateSig = Qt.pyqtSignal(bool,bool)
    
    def __init__(self,parent=None):

        TaurusPlot.__init__(self)
        plot = self
        #plot.setBackgroundBrush(Qt.QColor('white'))
        self.time_selector = None
        axis = self.axis = DateAxisItem(orientation='bottom')
        plot_items = self.plot_items = plot.getPlotItem()

        axis.attachToPlotItem(plot_items)
        # TODO (cleanup menu actions)
        if plot_items.legend is not None:
            plot_items.legend.hide()
        
        vb = plot.getPlotItem().getViewBox()
        vb.sigXRangeChanged.connect(self.onUpdateXViewRange)
        
    ###########################################################################
    # onAddXYModel
    ###########################################################################

    def onAddXYModel(self, models=None):
        """
        models being a list like:
        
        [('tgarch://alba03.cells.es:10000/sr/id/scw01/pressure?db=*;t0=2019-11-11T11:41:59;t1=2020-01-03T09:03:03;ts', 
        'tgarch://alba03.cells.es:10000/sr/id/scw01/pressure?db=*;t0=2019-11-11T11:41:59;t1=2020-01-03T09:03:03')]
        """
        try:
            plot = self
            
            # Update progress bar
            self.updateSig.emit(True,False)
            print('onAddXYModel(%s)'%models)
            #if not isSequenceType(models):
                #print('Overriding models ...')
                #models = msi.getSelectedModels()

            current = plot._model_chooser_tool.getModelNames()
            print('current: %s' % str(current))
            models = [m for m in models if m not in current]
            print('new models: %s' % str(models))
            plot.addModels(models)
            traceback.print_exc()
            self.updateSig.emit(True,True)
        except:
            traceback.print_exc()

    ###########################################################################
    # Update t0 and t1 based on sigXRangeChanged
    ###########################################################################
    def onUpdateXViewRange(self):
        x, _ = self.viewRange()
        t0, t1 = x
        t0s = str2localtime(t0)
        t1s = str2localtime(t1)
        
        print('times: %s(%s) - %s(%s)' % (t0,t0s,t1,t1s))
        if t0s and t1s:
            self.updateTimeChooser(t0s,t1s)
        
    def setTimeChooser(self,time_selector):
        self.time_selector = time_selector
        
    def updateTimeChooser(self, t0s, t1s):
        if self.time_selector:
            self.time_selector.ui.comboBox_begin.setItemText(5, t0s)
            self.time_selector.ui.comboBox_end.setItemText(7, t1s)
            
            self.time_selector.ui.comboBox_begin.setItemText(5, t0s)
            self.time_selector.ui.comboBox_end.setItemText(7, t1s)
            
            self.time_selector.ui.comboBox_begin.setCurrentIndex(5)
            self.time_selector.ui.comboBox_end.setCurrentIndex(7)
        else:
            print('No time chooser widget defined')

class ArchivingLegend(Qt.QGraphicsView):
    ###########################################################################
    # Legend
    ###########################################################################
    
    
    def __init__(self, plot):
        Qt.QGraphicsScene.__init__(self,Qt.QGraphicsScene())
        gv = self
        self.plot = plot
        gv.setBackgroundBrush(Qt.QBrush(Qt.QColor('white')))
        self.legend = pg.LegendItem(None, offset=(0, 0))
        gv.scene().addItem(self.legend)

    def updateExternalLegend(self):

        plot_items = self.plot.plot_items
        for dataitem in plot_items.listDataItems():
            self.legend.removeItem(dataitem.name())

        for dataitem in plot_items.listDataItems():
            if dataitem.name():
                self.legend.addItem(dataitem, dataitem.name())
                
###########################################################################
# Connect CurvesAppearanceChooser to external legend
###########################################################################

from taurus_pyqtgraph.curveproperties import (CurvesAppearanceChooser,
                                              CurveAppearanceProperties)

def onApply(self):
    names = self.getSelectedCurveNames()
    prop = self.getShownProperties()
    # Update self.curvePropDict for selected properties
    for n in names:
        self.curvePropDict[n] = CurveAppearanceProperties.merge(
            [self.curvePropDict[n], prop],
            conflict=CurveAppearanceProperties.inConflict_update_a)
    # emit a (PyQt) signal telling what properties (first argument) need to
    # be applied to which curves (second argument)
    # self.curveAppearanceChanged.emit(prop, names)
    # return both values

    self.curvePropAdapter.setCurveProperties(self.curvePropDict, names)
    # Update legend
    updateExternalLegend()

    return prop, names

# Override CurvesAppearanceChooser.onApply
CurvesAppearanceChooser.onApply = onApply
    
###########################################################################    

__usage__ = """
    Usage:
        tpgarchivingwidget.py [attr0] [attr1] ... [date0] [date1]
        
    e.g.
        ./ctarchiving02 sr/di/dcct/averagecurrent 2020-05-24 2020-05-27
"""

def main(*args):
    """
    usage attr1, attr2, attr3, ..., t0, t1
    """
    try:
        assert len(args) >  2
        t0,t1 = args[-2],args[-1]
        models = args[:-2]
        ArchivingWidget.run(models,t0,t1)
    except:
        print(__usage__)
    
if __name__ == '__main__':
    main(*sys.argv[1:])
