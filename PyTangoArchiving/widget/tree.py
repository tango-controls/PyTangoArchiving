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

"""This module provides widgets that display the database in a tree format"""

# todo: tango-centric!!

__all__ = ["TaurusDbTreeWidget", "TaurusBaseTreeWidget"]

__docformat__ = 'restructuredtext'

from taurus.external.qt import Qt
from taurus.core.taurusbasetypes import TaurusElementType
from taurus.core.taurusauthority import TaurusAuthority
#from taurus.qt.qtcore.model import *
from taurus.qt.qtgui.base import TaurusBaseWidget
from taurus.qt.qtgui.icon import getElementTypeIcon, getElementTypeIconName

from taurus.qt.qtgui.model import TaurusBaseModelWidget
from taurus.qt.qtgui.tree.qtree import QBaseTreeWidget


from taurus.external.qt import Qt
from taurus.qt.qtgui.model import QBaseModelWidget, BaseToolBar
from taurus.qt.qtgui.util import ActionFactory
from taurus.qt.qtgui.tree import TaurusBaseTreeWidget

from taurus.qt.qtcore.model.taurusdatabasemodel import ElemType, TaurusTreeDbBaseItem

import sys
from taurus.external.qt import Qt
import taurus.core
from taurus.qt.qtgui.container import TaurusWidget
from taurus.core.util.containers import CaselessList
from taurus.qt.qtgui.panel.taurusmodellist import TaurusModelList

from PyTangoArchiving.widget.models import *
from PyTangoArchiving.widget.models import TaurusArchivingDatabase
from taurus.core.tango.tangodatabase import (TangoDevInfo,TangoAttrInfo)


class TaurusModelSelectorTree(TaurusWidget):

    addModels = Qt.pyqtSignal('QStringList')

    def __init__(self, parent=None, selectables=None, buttonsPos=None, designMode=None):
        TaurusWidget.__init__(self, parent)
        if selectables is None:
            selectables = [taurus.core.taurusbasetypes.TaurusElementType.Attribute, taurus.core.taurusbasetypes.TaurusElementType.Member,
                           taurus.core.taurusbasetypes.TaurusElementType.Device]
        self._selectables = selectables

        # tree
        self._deviceTree = TaurusDbTreeWidget(
            perspective=taurus.core.taurusbasetypes.TaurusElementType.Device)
        
        self._deviceTree.getQModel().setSelectables(self._selectables)
        #self._deviceTree.setUseParentModel(True)
        self._deviceTree.modelObj = TaurusArchivingDatabase()

        # toolbar
        self._toolbar = Qt.QToolBar("TangoSelector toolbar")
        self._toolbar.setIconSize(Qt.QSize(16, 16))
        self._toolbar.setFloatable(False)
        self._addSelectedAction = self._toolbar.addAction(
            Qt.QIcon.fromTheme("list-add"), "Add selected", self.onAddSelected)

        # defines the layout
        self.setButtonsPos(buttonsPos)

        self._deviceTree.recheckTaurusParent()  # NOT WORKING????
        # @todo: This is Workaround because UseSetParentModel is giving trouble again!
        self.modelChanged.connect(self._deviceTree.setModel)

    def setButtonsPos(self, buttonsPos):
        # we must delete the previous layout before we can set a new one
        currlayout = self.layout()
        if currlayout is not None:
            currlayout.deleteLater()
            Qt.QCoreApplication.sendPostedEvents(
                currlayout, Qt.QEvent.DeferredDelete)
        # add to layout
        if buttonsPos is None:
            self.setLayout(Qt.QVBoxLayout())
            self.layout().addWidget(self._deviceTree)
        elif buttonsPos == Qt.Qt.BottomToolBarArea:
            self._toolbar.setOrientation(Qt.Qt.Horizontal)
            self.setLayout(Qt.QVBoxLayout())
            self.layout().addWidget(self._deviceTree)
            self.layout().addWidget(self._toolbar)
        elif buttonsPos == Qt.Qt.TopToolBarArea:
            self._toolbar.setOrientation(Qt.Qt.Horizontal)
            self.setLayout(Qt.QVBoxLayout())
            self.layout().addWidget(self._toolbar)
            self.layout().addWidget(self._deviceTree)
        elif buttonsPos == Qt.Qt.LeftToolBarArea:
            self._toolbar.setOrientation(Qt.Qt.Vertical)
            self.setLayout(Qt.QHBoxLayout())
            self.layout().addWidget(self._toolbar)
            self.layout().addWidget(self._deviceTree)
        elif buttonsPos == Qt.Qt.RightToolBarArea:
            self._toolbar.setOrientation(Qt.Qt.Vertical)
            self.setLayout(Qt.QHBoxLayout())
            self.layout().addWidget(self._deviceTree)
            self.layout().addWidget(self._toolbar)
        else:
            raise ValueError("Invalid buttons position")

    def getSelectedModels(self):
        selected = []
        try:
            from taurus.core.tango.tangodatabase import (TangoDevInfo,
                                                         TangoAttrInfo)
            #from taurus.core.tango.tangodatabase import TangoAttrInfo
            #from PyTangoArchiving.widget.model import TangoDevInfo
        except:
            return selected
        # TODO: Tango-centric
        for item in self._deviceTree.selectedItems():
            nfo = item.itemData()
            if isinstance(nfo, TangoDevInfo):
                selected.append(nfo.fullName())
            elif isinstance(nfo, TangoAttrInfo):
                selected.append("%s/%s" %
                                (nfo.device().fullName(), nfo.name()))
            else:
                self.info("Unknown item '%s' in selection" % repr(nfo))
        return selected

    def onAddSelected(self):
        self.addModels.emit(self.getSelectedModels())

    def treeView(self):
        return self._deviceTree.treeView()

    @classmethod
    def getQtDesignerPluginInfo(cls):
        ret = TaurusWidget.getQtDesignerPluginInfo()
        ret['module'] = 'taurus.qt.qtgui.panel'
        ret['icon'] = "designer:listview.png"
        ret['container'] = False
        ret['group'] = 'Taurus Views'
        return ret


class TaurusModelChooser(TaurusWidget):
    '''A widget that allows the user to select a list of models from a tree representing
    devices and attributes from a Tango server.

    The user selects models and adds them to a list. Then the user should click on the
    update button to notify that the selection is ready.

    signals::
      - "updateModels"  emitted when the user clicks on the update button. It
        passes a list<str> of models that have been selected.
    '''

    updateModels = Qt.pyqtSignal('QStringList')
    UpdateAttrs = Qt.pyqtSignal(['QStringList'], ['QMimeData'])

    def __init__(self, parent=None, selectables=None, host=None, designMode=None, singleModel=False):
        '''Creator of TaurusModelChooser

        :param parent: (QObject) parent for the dialog
        :param selectables: (list<TaurusElementType>) if passed, only elements of the tree whose
                            type is in the list will be selectable.
        :param host: (QObject) Tango host to be explored by the chooser
        :param designMode: (bool) needed for taurusdesigner but ignored here
        :param singleModel: (bool) If True, the selection will be of just one
                            model. Otherwise (default) a list of models can be selected
        '''
        TaurusWidget.__init__(self, parent)
        if host is None:
            host = taurus.Authority().getNormalName()

        self._allowDuplicates = False

        self.setLayout(Qt.QVBoxLayout())

        self.tree = TaurusModelSelectorTree(
            selectables=selectables, buttonsPos=Qt.Qt.BottomToolBarArea)
        self.tree.setModel(host)
        self.list = TaurusModelList()
        self.list.setSelectionMode(Qt.QAbstractItemView.ExtendedSelection)
        applyBT = Qt.QToolButton()
        applyBT.setToolButtonStyle(Qt.Qt.ToolButtonTextBesideIcon)
        applyBT.setText('Apply')
        applyBT.setIcon(Qt.QIcon("status:available.svg"))

        self.setSingleModelMode(singleModel)

        # toolbar
        self._toolbar = self.tree._toolbar
        self._toolbar.addAction(self.list.removeSelectedAction)
        self._toolbar.addAction(self.list.removeAllAction)
        self._toolbar.addAction(self.list.moveUpAction)
        self._toolbar.addAction(self.list.moveDownAction)
        self._toolbar.addSeparator()
        self._toolbar.addWidget(applyBT)
        self.layout().addWidget(self.tree)
        self.layout().addWidget(self.list)

        # self.tree.setUseParentModel(True)  #It does not work!!!!
        # @todo: This is Workaround because UseSetParentModel is giving trouble again!
        self.modelChanged.connect(self.tree.setModel)

        # connections:
        self.tree.addModels.connect(self.addModels)
        applyBT.clicked.connect(self._onUpdateModels)
#        self.connect(self.tree._deviceTree, Qt.SIGNAL("itemDoubleClicked"), self.onTreeDoubleClick)

#    def onTreeDoubleClick(self, item, colum): #@todo: Implement this function properly
#        if item.Role in self.tree._selectables:
#            self.addModels([str(item.text())])

    def getListedModels(self, asMimeData=False):
        '''returns the list of models that have been added

        :param asMimeData: (bool) If False (default), the return value will be a
                           list of models. If True, the return value is a
                           `QMimeData` containing at least `TAURUS_MODEL_LIST_MIME_TYPE`
                           and `text/plain` MIME types. If only one model was selected,
                           the mime data also contains a TAURUS_MODEL_MIME_TYPE.

        :return: (list<str> or QMimeData) the type of return depends on the value of `asMimeData`'''
        models = self.list.getModelList()
        if self.isSingleModelMode():
            models = models[:1]
        if asMimeData:
            md = Qt.QMimeData()
            md.setData(taurus.qt.qtcore.mimetypes.TAURUS_MODEL_LIST_MIME_TYPE, str(
                "\r\n".join(models)))
            md.setText(", ".join(models))
            if len(models) == 1:
                md.setData(
                    taurus.qt.qtcore.mimetypes.TAURUS_MODEL_MIME_TYPE, str(models[0]))
            return md
        return models

    def setListedModels(self, models):
        '''adds the given list of models to the widget list
        '''
        self.list.model().clearAll()
        self.list.addModels(models)

    def resetListedModels(self):
        '''equivalent to setListedModels([])'''
        self.list.model().clearAll()

    def updateList(self, attrList):
        '''for backwards compatibility with AttributeChooser only. Use :meth:`setListedModels` instead'''
        self.info(
            'ModelChooser.updateList() is provided for backwards compatibility only. Use setListedModels() instead')
        self.setListedModels(attrList)

    def addModels(self, models):
        ''' Add given models to the selected models list'''
        if len(models) == 0:
            models = ['']
        if self.isSingleModelMode():
            self.resetListedModels()
        if self._allowDuplicates:
            self.list.addModels(models)
        else:
            listedmodels = CaselessList(self.getListedModels())
            for m in models:
                if m not in listedmodels:
                    listedmodels.append(m)
                    self.list.addModels([m])

    def onRemoveSelected(self):
        '''
        Remove the list-selected models from the list
        '''
        self.list.removeSelected()

    def _onUpdateModels(self):
        models = self.getListedModels()
        self.updateModels.emit(models)
        if taurus.core.taurusbasetypes.TaurusElementType.Attribute in self.tree._selectables:
            # for backwards compatibility with the old AttributeChooser
            self.UpdateAttrs.emit(models)

    def setSingleModelMode(self, single):
        '''sets whether the selection should be limited to just one model
        (single=True) or not (single=False)'''
        if single:
            self.tree.treeView().setSelectionMode(Qt.QAbstractItemView.SingleSelection)
        else:
            self.tree.treeView().setSelectionMode(Qt.QAbstractItemView.ExtendedSelection)
        self._singleModelMode = single

    def isSingleModelMode(self):
        '''returns True if the selection is limited to just one model. Returns False otherwise.

        :return: (bool)'''
        return self._singleModelMode

    def resetSingleModelMode(self):
        '''equivalent to setSingleModelMode(False)'''
        self.setSingleModelMode(self, False)

    @staticmethod
    def modelChooserDlg(parent=None, selectables=None, host=None, asMimeData=False, singleModel=False, windowTitle='Model Chooser'):
        '''Static method that launches a modal dialog containing a TaurusModelChooser

        :param parent: (QObject) parent for the dialog
        :param selectables: (list<TaurusElementType>) if passed, only elements of the tree whose
                            type is in the list will be selectable.
        :param host: (QObject) Tango host to be explored by the chooser
        :param asMimeData: (bool) If False (default),  a list of models will be.
                           returned. If True, a `QMimeData` object will be
                           returned instead. See :meth:`getListedModels` for a
                           detailed description of this QMimeData object.
        :param singleModel: (bool) If True, the selection will be of just one
                            model. Otherwise (default) a list of models can be selected
        :param windowTitle: (str) Title of the dialog (default="Model Chooser")

        :return: (list,bool or QMimeData,bool) Returns a models,ok tuple. models can be
                 either a list of models or a QMimeData object, depending on
                 `asMimeData`. ok is True if the dialog was accepted (by
                 clicking on the "update" button) and False otherwise
        '''
        dlg = Qt.QDialog(parent)
        dlg.setWindowTitle(windowTitle)
        dlg.setWindowIcon(Qt.QIcon("logos:taurus.png"))
        layout = Qt.QVBoxLayout()
        w = TaurusModelChooser(
            parent=parent, selectables=selectables, host=host, singleModel=singleModel)
        layout.addWidget(w)
        dlg.setLayout(layout)
        w.updateModels.connect(dlg.accept)
        dlg.exec_()
        return w.getListedModels(asMimeData=asMimeData), (dlg.result() == dlg.Accepted)

    @classmethod
    def getQtDesignerPluginInfo(cls):
        ret = TaurusWidget.getQtDesignerPluginInfo()
        ret['module'] = 'taurus.qt.qtgui.panel'
        ret['icon'] = "designer:listview.png"
        ret['container'] = False
        ret['group'] = 'Taurus Views'
        return ret

    singleModelMode = Qt.pyqtProperty(
        "bool", isSingleModelMode, setSingleModelMode, resetSingleModelMode)


## From TaurusPlot
def showDataImportDlg(self,trend):
    '''Launches the data import dialog. This dialog lets the user manage
    which attributes are attached to the plot (using
    :class:`TaurusModelChooser`) and also to generate raw data or import it
    from files
    '''
    if self.DataImportDlg is None:
        from taurus.qt.qtgui.panel import TaurusModelChooser
        self.DataImportDlg = Qt.QDialog(self)
        self.DataImportDlg.setWindowTitle(
            "%s - Import Data" % (str(trend.windowTitle())))
        self.DataImportDlg.modelChooser = TaurusModelChooser(
            selectables=[taurus.core.taurusbasetypes.TaurusElementType.Attribute])
        from taurus.qt.qtgui.panel import QRawDataWidget
        self.DataImportDlg.rawDataChooser = QRawDataWidget()

        tabs = Qt.QTabWidget()
        tabs.addTab(self.DataImportDlg.modelChooser, "&Attributes")
        tabs.addTab(self.DataImportDlg.rawDataChooser, "&Raw Data")
        mainlayout = Qt.QVBoxLayout(self.DataImportDlg)
        mainlayout.addWidget(tabs)

        self.DataImportDlg.modelChooser.updateModels.connect(trend.setModel)
        self.DataImportDlg.rawDataChooser.ReadFromFiles.connect(trend.readFromFiles)
        self.DataImportDlg.rawDataChooser.AddCurve.connect(trend.attachRawData)

    models_and_display = [(m, trend.getCurveTitle(
        m.split('|')[-1])) for m in trend._modelNames]

    self.DataImportDlg.modelChooser.setListedModels(models_and_display)
    self.DataImportDlg.show()
    
    
class TaurusTreeDeviceItem(TaurusTreeDbBaseItem):
    """A node designed to represent a device"""

    def child(self, row):
        self.updateChilds()
        return super(TaurusTreeDeviceItem, self).child(row)

    def hasChildren(self):
        return True
        nb = super(TaurusTreeDeviceItem, self).childCount()
        if nb > 0:
            return True
        data = self.itemData()
        #if data.state() != TaurusDevState.Ready:
            #return False
        return True

    def childCount(self):
        nb = super(TaurusTreeDeviceItem, self).childCount()
        if nb > 0:
            return nb
        data = self.itemData()
        #if data.state() != TaurusDevState.Ready:
            #return 0
        self.updateChilds()
        return super(TaurusTreeDeviceItem, self).childCount()

    def updateChilds(self):
        if len(self._childItems) > 0:
            return
        print(type(self._model),self._model,type(self._itemData),self._itemData)
        print(self.itemData().name)
        attrs = self._model.dataSource().get_device_attribute_list(self.itemData().name())
        print(attrs)
        for attr in attrs:
            #for attr in self._itemData.attributes():
            attr = TangoAttrInfo(self.itemData().container(), name = attr.lower(),
                full_name = (self.itemData().name()+'/'+attr).lower(), device = self.itemData(),
                info = None)
            c = TaurusTreeAttributeItem(self._model, attr, self)
            self.appendChild(c)
        return

    def data(self, index):
        column, model = index.column(), index.model()
        role = model.role(column, self.depth())
        obj = self.itemData()
        if role == ElemType.Device or role == ElemType.Name:
            return obj.name()
        elif role == ElemType.DeviceAlias:
            return obj.alias()
        elif role == ElemType.Server:
            return obj.server().name()
        elif role == ElemType.DeviceClass:
            return obj.klass().name()
        elif role == ElemType.Exported:
            return obj.state()
        elif role == ElemType.Host:
            return obj.host()
        elif role == ElemType.Domain:
            return obj.domain()
        elif role == ElemType.Family:
            return obj.family()
        elif role == ElemType.Member:
            return obj.member()

    def mimeData(self, index):
        return self.itemData().fullName()

    def role(self):
        return ElemType.Device    
    

class TaurusDbDeviceProxyModel(TaurusDbBaseProxyModel):
    """A Qt filter & sort model for model for the taurus models:
           - TaurusDbBaseModel
           - TaurusDbDeviceModel
           - TaurusDbSimpleDeviceModel
           - TaurusDbPlainDeviceModel"""

    def filterAcceptsRow(self, sourceRow, sourceParent):
        sourceModel = self.sourceModel()
        idx = sourceModel.index(sourceRow, 0, sourceParent)
        treeItem = idx.internalPointer()
        regexp = self.filterRegExp()

        # if domain node, check if it will potentially have any children
        if isinstance(treeItem, TaurusTreeDeviceDomainItem):
            domain = treeItem.display()
            devices = sourceModel.getDomainDevices(domain)
            for device in devices:
                if self.deviceMatches(device, regexp):
                    return True
            return False

        # if family node, check if it will potentially have any children
        if isinstance(treeItem, TaurusTreeDeviceFamilyItem):
            domain = treeItem.parent().display()
            family = treeItem.display()
            devices = sourceModel.getFamilyDevices(domain, family)
            for device in devices:
                if self.deviceMatches(device, regexp):
                    return True
            return False

        if isinstance(treeItem, TaurusTreeDeviceItem) or \
           isinstance(treeItem, TaurusTreeSimpleDeviceItem) or \
           isinstance(treeItem, TaurusTreeDeviceMemberItem):
            device = treeItem.itemData()
            return self.deviceMatches(device, regexp)
        return True

    def deviceMatches(self, device, regexp):
        name = device.name()

        # if Qt.QString(name).contains(regexp):
        if regexp.indexIn(name) != -1:
            return True
        name = device.alias()
        if name is None:
            return False
        # return Qt.QString(name).contains(regexp)
        return regexp.indexIn(name) != -1


class TaurusDbDeviceModel(TaurusDbBaseModel):
    """A Qt model that structures device elements in a 3 level tree organized
       as:

           - <domain>
           - <family>
           - <member>"""
    ColumnRoles = (ElemType.Device, ElemType.Domain, ElemType.Family, ElemType.Member,
                   ElemType.Attribute), ElemType.DeviceAlias, ElemType.Server, ElemType.DeviceClass, ElemType.Exported, ElemType.Host
    
    def __init__(self,*args,**kwargs):
        super(TaurusDbDeviceModel, self).__init__(*args,**kwargs)
        self.setDataSource(TaurusArchivingDatabase())

    def setupModelData(self, data):
        if data is None:
            return
        try:
            # TODO: Tango-centric
            # TODO: is this try needed? (not done in, e.g. TaurusDbPlainDeviceModel)
            from taurus.core.tango.tangodatabase import TangoDatabase
        except ImportError:
            return
        if isinstance(data, TangoDatabase):
            data = data.deviceTree()

        rootItem = self._rootItem
        for domain in data.keys():
            families = data[domain]
            domainItem = TaurusTreeDeviceDomainItem(
                self, domain.upper(), rootItem)
            for family in families.keys():
                members = families[family]
                familyItem = TaurusTreeDeviceFamilyItem(
                    self, family.upper(), domainItem)
                for member in members.keys():
                    dev = members[member]
                    memberItem = TaurusTreeDeviceItem(
                        self, dev, parent=familyItem)
                    familyItem.appendChild(memberItem)
                domainItem.appendChild(familyItem)
            rootItem.appendChild(domainItem)


class TaurusDbTreeWidget(TaurusBaseTreeWidget):
    """A class:`taurus.qt.qtgui.tree.TaurusBaseTreeWidget` that connects to a
    :class:`taurus.core.taurusauthority.TaurusAuthority` model. It can show the list of database
    elements in four different perspectives:

    - device : a three level hierarchy of devices (domain/family/name)
    - server : a server based perspective
    - class : a class based perspective

    Filters can be inserted into this widget to restrict the tree nodes that are
    seen.
    """

    KnownPerspectives = {
        TaurusElementType.Device: {
            "label": "By device",
            "icon": getElementTypeIconName(TaurusElementType.Device),
            "tooltip": "View by device tree",
            "model": [TaurusDbDeviceProxyModel, TaurusDbDeviceModel, ],
        },
        'PlainDevice': {
            "label": "By plain device",
            "icon": getElementTypeIconName(TaurusElementType.Device),
            "tooltip": "View by plain device tree (it may take a long time if there are problems with the exported devices)",
            "model": [TaurusDbDeviceProxyModel, TaurusDbPlainDeviceModel, ],
        },

        TaurusElementType.Server: {
            "label": "By server",
            "icon": getElementTypeIconName(TaurusElementType.Server),
            "tooltip": "View by server tree",
            "model": [TaurusDbServerProxyModel, TaurusDbServerModel, ],
        },
        TaurusElementType.DeviceClass: {
            "label": "By class",
            "icon": getElementTypeIconName(TaurusElementType.DeviceClass),
            "tooltip": "View by class tree",
            "model": [TaurusDbDeviceClassProxyModel, TaurusDbDeviceClassModel, ],
        },
    }

    DftPerspective = TaurusElementType.Device

    def getModelClass(self):
        return TaurusAuthority

    def sizeHint(self):
        return Qt.QSize(1024, 512)
    
    @Qt.pyqtSlot('QString')
    def setModel(self, model, obj=None):
        """Sets/unsets the model name for this component

        :param model: (str) the new model name"""
        super(TaurusDbTreeWidget, self).setModel(model)
        self.modelObj = obj
        
    def _attach(self):
        """Attaches the component to the taurus model.
        In general it should not be necessary to overwrite this method in a
        subclass.

        :return: (bool) True if success in attachment or False otherwise.
        """
        if self.isAttached():
            return self._attached

        self.preAttach()

        #if cls is None:
            #self._attached = False
            ##self.trace("Failed to attach: Model class not found")
        #elif self.modelName == '':
            #self._attached = False
            #self.modelObj = None
        #else:
            #try:
                #self.modelObj = taurus.Manager().getObject(cls, self.modelName)
                #if self.modelObj is not None:
                    #self.modelObj.addListener(self)
                    #self._attached = True
                    #self.changeLogName(self.log_name + "." + self.modelName)
            #except Exception:
                #self.modelObj = None
                #self._attached = False
                #self.debug(
                    #"Exception occured while trying to attach '%s'" % self.modelName)
                #self.traceback()

        self.postAttach()
        return self._attached           

    @classmethod
    def getQtDesignerPluginInfo(cls):
        ret = TaurusBaseWidget.getQtDesignerPluginInfo()
        ret['module'] = 'taurus.qt.qtgui.tree'
        ret['group'] = 'Taurus Views'
        ret['icon'] = "designer:listview.png"
        return ret


#class _TaurusTreePanel(Qt.QWidget, TaurusBaseWidget):
    #"""A demonstration panel to show how :class:`taurus.qt.qtcore.TaurusDbBaseModel`
    #models can interact with several model view widgets like QTreeView,
    #QTableView, QListView and QComboBox"""

    #def __init__(self, parent=None, designMode=False):
        #"""doc please!"""
        #name = self.__class__.__name__
        #self.call__init__wo_kw(Qt.QWidget, parent)
        #self.call__init__(TaurusBaseWidget, name, designMode=designMode)
        #self.init(designMode)

    #def init(self, designMode):
        #l = Qt.QGridLayout()
        #l.setContentsMargins(0, 0, 0, 0)
        #self.setLayout(l)

##        tb = self._toolbar = Qt.QToolBar("Taurus tree panel toolbar")
##        tb.setFloatable(False)
##        refreshAction = self._refreshAction = tb.addAction(Qt.QIcon.fromTheme("view-refresh"),"Refresh", self.refresh)

##        l.addWidget(tb, 0, 0)

        #main_panel = Qt.QTabWidget()
        #self._device_tree_view = TaurusDbTreeWidget(
            #perspective=TaurusElementType.Device)
        #self._device_table_view = Qt.QTableView()
        #self._device_table_view.setModel(TaurusDbBaseModel())
        #self._device_list_view = Qt.QListView()
        #self._device_list_view.setModel(TaurusDbSimpleDeviceModel())
        #self._server_tree_view = TaurusDbTreeWidget(
            #perspective=TaurusElementType.Server)
        #self._class_tree_view = TaurusDbTreeWidget(
            #perspective=TaurusElementType.DeviceClass)

        #self._device_combo_view = Qt.QWidget()
        #combo_form = Qt.QFormLayout()
        #self._device_combo_view.setLayout(combo_form)

        #self._combo_dev_tree_widget = TaurusDbTreeWidget(
            #perspective=TaurusElementType.Device)
        #qmodel = self._combo_dev_tree_widget.getQModel()
        #qmodel.setSelectables([TaurusElementType.Member])
        #device_combo = Qt.QComboBox()
        #device_combo.setModel(qmodel)
        #device_combo.setMaxVisibleItems(20)
        #device_combo.setView(self._combo_dev_tree_widget.treeView())
        #combo_form.addRow(
            #"Device selector (by device hierarchy):", device_combo)

        #self._combo_attr_tree_widget = TaurusDbTreeWidget(
            #perspective=TaurusElementType.Device)
        #qmodel = self._combo_attr_tree_widget.getQModel()
        #device_combo = Qt.QComboBox()
        #device_combo.setModel(qmodel)
        #device_combo.setMaxVisibleItems(20)
        #device_combo.setView(self._combo_attr_tree_widget.treeView())
        #combo_form.addRow(
            #"Attribute selector (by device hierarchy):", device_combo)

        #self._combo_dev_table_view = Qt.QTableView()
        #self._combo_dev_table_view.setModel(TaurusDbBaseModel())
        #qmodel = self._combo_dev_table_view.model()
        #qmodel.setSelectables([TaurusElementType.Device])
        #device_combo = Qt.QComboBox()
        #device_combo.setModel(qmodel)
        #device_combo.setMaxVisibleItems(20)
        #device_combo.setView(self._combo_dev_table_view)
        #combo_form.addRow("Device selector (by plain device):", device_combo)

        #main_panel.addTab(self._device_tree_view, "Device (Tree View)")
        #main_panel.addTab(self._device_table_view, "Device (Table View)")
        #main_panel.addTab(self._device_list_view, "Device (List View)")
        #main_panel.addTab(self._server_tree_view, "Server (Tree View)")
        #main_panel.addTab(self._class_tree_view, "Class (Tree View)")
        #main_panel.addTab(self._device_combo_view, "ComboBox Views")

        #l.addWidget(main_panel, 1, 0)

        #self._main_panel = main_panel

    #def deviceTreeWidget(self):
        #return self._device_tree_view

    #def deviceTableWidget(self):
        #return self._device_table_view

    #def deviceListWidget(self):
        #return self._device_list_view

    #def serverTreeWidget(self):
        #return self._server_tree_view

    #def classTreeWidget(self):
        #return self._class_tree_view

    #def sizeHint(self):
        #return Qt.QSize(1024, 512)

    #def _updateTreeModels(self):
        #db_name, db = self.getModel(), self.getModelObj()

        #self._device_tree_view.setModel(db_name)

        #model = self._device_table_view.model()
        #if model is not None:
            #model.setDataSource(db)

        #model = self._device_list_view.model()
        #if model is not None:
            #model.setDataSource(db)

        #self._server_tree_view.setModel(db_name)
        #self._class_tree_view.setModel(db_name)
        #self._combo_dev_tree_widget.setModel(db_name)
        #self._combo_attr_tree_widget.setModel(db_name)

        #model = self._combo_dev_table_view.model()
        #if model is not None:
            #model.setDataSource(db)

    #def refresh(self):
        #db = self.getModelObj()
        #if db is None:
            #return
        #db.refreshCache()
        #self._device_tree_view.refresh()
        #self._device_table_view.model().refresh()
        #self._device_list_view.model().refresh()
        #self._server_tree_view.refresh()
        #self._class_tree_view.refresh()

    #def goIntoTree(self):
        #index = self._device_tree_view.currentIndex()
        #if index is None:
            #return
        ##index_parent = index.parent()
        ## if index_parent is None:
        ##    return
        #self._device_tree_view.setRootIndex(index)

    #def goUpTree(self):
        #index = self._device_tree_view.rootIndex()
        #if index is None:
            #return
        #index_parent = index.parent()
        #if index_parent is None:
            #return
        #self._device_tree_view.setRootIndex(index_parent)

    ##-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-
    ## TaurusBaseWidget overwriting
    ##-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-

    #def getModelClass(self):
        #return TaurusAuthority

    #@Qt.pyqtSlot('QString')
    #def setModel(self, model, obj=None):
        #"""Sets/unsets the model name for this component

        #:param model: (str) the new model name"""
        #super(_TaurusTreePanel, self).setModel(model)
        #self.modelObj = obj
        #self._updateTreeModels()
        
    #def _attach(self):
        #"""Attaches the component to the taurus model.
        #In general it should not be necessary to overwrite this method in a
        #subclass.

        #:return: (bool) True if success in attachment or False otherwise.
        #"""
        #if self.isAttached():
            #return self._attached

        #self.preAttach()

        ##if cls is None:
            ##self._attached = False
            ###self.trace("Failed to attach: Model class not found")
        ##elif self.modelName == '':
            ##self._attached = False
            ##self.modelObj = None
        ##else:
            ##try:
                ##self.modelObj = taurus.Manager().getObject(cls, self.modelName)
                ##if self.modelObj is not None:
                    ##self.modelObj.addListener(self)
                    ##self._attached = True
                    ##self.changeLogName(self.log_name + "." + self.modelName)
            ##except Exception:
                ##self.modelObj = None
                ##self._attached = False
                ##self.debug(
                    ##"Exception occured while trying to attach '%s'" % self.modelName)
                ##self.traceback()

        #self.postAttach()
        #return self._attached        

    ##-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-
    ## QT property definition
    ##-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-

    ##: This property holds the unique URI string representing the model name
    ##: with which this widget will get its data from. The convention used for
    ##: the string can be found :ref:`here <model-concept>`.
    ##:
    ##: In case the property :attr:`useParentModel` is set to True, the model
    ##: text must start with a '/' followed by the attribute name.
    ##:
    ##: **Access functions:**
    ##:
    ##:     * :meth:`TaurusBaseWidget.getModel`
    ##:     * :meth:`TaurusBaseWidget.setModel`
    ##:     * :meth:`TaurusBaseWidget.resetModel`
    ##:
    ##: .. seealso:: :ref:`model-concept`
    #model = Qt.pyqtProperty("QString", TaurusBaseWidget.getModel,
                            #TaurusBaseWidget.setModel, TaurusBaseWidget.resetModel)


#def main_TaurusTreePanel(host):
    #w = _TaurusTreePanel()
    #w.setWindowIcon(getElementTypeIcon(TaurusElementType.Device))
    #w.setWindowTitle("A Taurus Tree Example")
    #w.setModel(host)
    #w.show()
    #return w


#def main_TaurusDbTreeWidget(host, perspective=TaurusElementType.Device):
    #w = TaurusDbTreeWidget(perspective=perspective)
    #w.setWindowIcon(getElementTypeIcon(perspective))
    #w.setWindowTitle("A Taurus Tree Example")
    #w.setModel(host)
    #w.show()
    #return w


#def demo():
    #"""DB panels"""
    #import taurus
    #db = TaurusArchivingDatabase() #taurus.Authority()
    #host = db.getNormalName()
    ##w = main_TaurusTreePanel(host)
    ## w = main_TaurusDbTreeWidget(host, TaurusElementType.Device)

    #w = _TaurusTreePanel()
    #w.setWindowIcon(getElementTypeIcon(TaurusElementType.Device))
    #w.setWindowTitle("A Taurus Tree Example")
    #w.setModel(host,db)
    #w.show()

    #return w


#def main1():
    #import sys
    #import taurus.qt.qtgui.application
    #Application = taurus.qt.qtgui.application.TaurusApplication

    #app = Application.instance()
    #owns_app = app is None

    #if owns_app:
        #app = Application(app_name="DB model demo", app_version="1.0",
                          #org_domain="Taurus", org_name="Tango community")
    #w = demo()
    #w.show()

    #if owns_app:
        #sys.exit(app.exec_())
    #else:
        #return w
    
#def main2(args = []):
    #if len(sys.argv) > 1:
        #host = sys.argv[1]
    #else:
        #host = None

    #app = Qt.QApplication(args)
    #tm = TaurusModelChooser()
    ##.modelChooserDlg() #host=host)
    #ta = TaurusArchivingDatabase()
    #ta.get_archived_devices_list()
    #print dlg
    #sys.exit()
    
def main3():
    asMimeData = True
    app = Qt.QApplication([])
    dlg = Qt.QDialog()
    dlg.setWindowTitle('Archiving Tree')
    dlg.setWindowIcon(Qt.QIcon("logos:taurus.png"))
    layout = Qt.QVBoxLayout()
    w = TaurusModelChooser(
        parent=dlg) #, selectables=selectables, host=host, singleModel=singleModel)
    layout.addWidget(w)
    dlg.setLayout(layout)
    w.updateModels.connect(dlg.accept)
    dlg.exec_()
    return w.getListedModels(asMimeData=asMimeData), (dlg.result() == dlg.Accepted)    

if __name__ == "__main__":
    print main3()
