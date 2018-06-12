#############################################################################
##
## file :       PyTangoArchiving/widget/snaps/snapdialogs.py
##
## description : see below
##
## project :     Tango Control System
##
## $Author: Zbigniew Reszela
##
## copyleft :    ALBA Synchrotron Controls Section, CELLS
##               Bellaterra
##               Spain
##
#############################################################################
##
## This file is part of Tango Control System
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
###########################################################################

#!/usr/bin/python
import sys, traceback, re, time
import PyTango,fandango
from fandango.tango import get_all_models

import taurus
from taurus.external.qt import Qt,QtCore,QtGui

from taurus.qt.qtgui.application import TaurusApplication
from taurus.qt.qtgui.container import TaurusWidget
from taurus.qt.qtgui.button import TaurusLauncherButton
from taurus.qt.qtgui.panel import TaurusValue,TaurusForm,DefaultTaurusValueCheckBox
#from PyTangoArchiving.widget.taurusattributechooser import TaurusAttributeChooser as AttrChooser
from taurus.qt.qtgui.panel.taurusmodelchooser import TaurusModelChooser as AttrChooser
from taurus.qt.qtgui.input import TaurusValueLineEdit
from taurus.qt.qtgui.table import TaurusValuesTable
from taurus.qt.qtgui.plot import TaurusPlot

from PyTangoArchiving import SnapAPI

def areListsValuesEqual(list1, list2):
    if None in (list1,list2):
        return True
    if len(list1) != len(list2):
        return False
    for t in zip(list1, list2):
        if t[0] != t[1]:
            return False
    return True

#def areStrValuesEqual(v1,v2):
#    try:
#        if "nan" == str(v1).lower() == str(v2).lower(): return True
#        return self.encode(v1) == self.encode(v2)
#    except:
#        return False

###################ShowSpectrumButton##########################

class ShowSpectrumButton(TaurusLauncherButton):
    """
    This class is used as a replacement for edit button of TaurusValues representing
    spectrum attributes
    """
    def __init__(self, parent = None, designMode = False, model = None):
        #TaurusLauncherButton.__init__(self, parent = parent, designMode = designMode, widget = TaurusPlot(), icon=':/qwtplot.png', text = 'Show snap')
        TaurusLauncherButton.__init__(self, parent = parent, designMode = designMode, widget = TaurusValuesTable(), icon=':/designer/table.png', text = 'Show snap')
        
    def setValue(self, value, dimy=1):
        print 'ShowSpectrumButton(%s).setValue(%s)'%(self.modelName,value)
        self._value = value
        try:
            self._widget = getattr(self,'_widget',None) or self.widget()
            if hasattr(self._widget,'attachRawData'):
                self._widget.attachRawData({"name:":"","x":range(len(value)),"y":value})
            elif isinstance(self._widget,TaurusValuesTable):
                m = self.modelName
                tm = self._widget.setModel(m)
                self._widget.setPaused(True)
                tm = self._widget._tableView.model()
                tm.setAttr(self.getModelObj().read(cache=False))
                tiod = self._widget._tableView.itemDelegate()
                for i in range(len(value)):
                    for j in range(dimy):
                        ti = tm.index(i,j)
                        o = str(tm.getReadValue(ti))
                        v = fandango.toList(value[i])[j]
                        s = ['true','false'][fandango.isFalse(v)] if ti.model().getType() == bool else str(v)
                        tiod = self._widget._tableView.itemDelegate(ti)
                        te = tiod.createEditor(tiod.parent(),None,ti)
                        tiod.setEditorData(te,ti)
                        if o!=s:
                            #print '%d,%d: %s != %s'%(i,j,o,s)
                            if ti.model().getType() == bool: te.setCurrentIndex(te.findText(s))
                            else: te.setText(s)
                            tiod.setModelData(te,tm,ti)
                        tiod.emit(Qt.SIGNAL('commitData'),te)
        except:
            print 'URG!: %s: TaurusLauncherButton has no _widget!?'%time.ctime()
            traceback.print_exc()
         
    def getValue(self):
        return getattr(self,'_value',None)
        
    def onClicked(self):
        '''Note that the dialog will only be created once. Subsequent clicks on
        the button will only raise the existing dialog'''
        if self._dialog is None:
            self._dialog = Qt.QDialog(self, Qt.Qt.WindowTitleHint)
            self._dialog.setWindowTitle(str(self.getModel() + " - snap value"))
            layout = Qt.QVBoxLayout(self._dialog)
            layout.addWidget(self._widget)
        self._dialog.show()
        self._dialog.raise_()
        
################### ShowImageButton ##########################
#will be continued when api will allow to store images

class ShowImageButton(ShowSpectrumButton):
    """
    This class is used as a replacement for edit button of TaurusValues representing
       spectrum attributes
    """
    def setValue(self, value):
        ShowSpectrumButton.setValue(self,value,dimy=len(len(value) and value[0] or []))

###################LoadTaurusValue##########################
class LoadTaurusValue(TaurusValue):
    """This class is a modified TaurusValue, with check box at the very right,
       Checking this box indicates if changes on this widget are considered
       as a pending operation.
       Changes cannot be done by user interaction - write widgets are disabled."""
       
    def __init__(self, parent = None):
        TaurusValue.__init__(self, parent)
        self._parent = parent
        self._parentLayout = parent.layout()
        self._checkBoxWidget = None
        self.setLabelConfig('attr_fullname')
        
    def getDefaultWriteWidgetClass(self):
        """ShowSpectrumButton instead of default widget for spectrum attributes"""
        if self.isReadOnly(): return None
        modelobj = self.getModelObj()
        if modelobj is None: return TaurusValueLineEdit
        config = modelobj.getConfig()
        if config.isScalar():
            configType = config.getType() 
            if configType == PyTango.ArgType.DevBoolean:
                result = DefaultTaurusValueCheckBox
            else:
                result = TaurusValueLineEdit
        elif config.isSpectrum():
            result = ShowSpectrumButton
        elif config.isImage():
            result = ShowImageButton #ShowImageButton
        else:
            self.warning('Unsupported attribute type')
            result = None
        return result
        
    def checkBoxWidget(self):
        return self._checkBoxWidget
    
    def setCheckBoxWidget(self, checkBoxWidget):
        self._checkBoxWidget = checkBoxWidget
    
    def setModel(self, model):
        """Disables write widgets"""
        #print 'LoadTaurusValue.setModel(%s)'%model
        TaurusValue.setModel(self, model)
        self.updateCheckBoxWidget()
        #if isinstance(self.writeWidget(),Qt.QLineEdit): self.writeWidget().setReadOnly(True)
        #elif isinstance(self.writeWidget(),Qt.QCheckBox): self.writeWidget().setEnabled(False)
    
    def updateWriteWidget(self):
        """This hack is used to pass resposibility of returning pending operations
        from writable widgets to TaurusValue"""
        TaurusValue.updateWriteWidget(self)
        w = self.writeWidget()
        if w: w.getPendingOperations = lambda : []
    
    def updateCheckBoxWidget(self):
        """Create and put check box at the very end of TaurusValue subwidgets"""
        self.setCheckBoxWidget(Qt.QCheckBox())
        if self.minimumHeight() is not None:
            self.checkBoxWidget().setMinimumHeight(self.minimumHeight())
        self._parentLayout.addWidget(self.checkBoxWidget(), self._row, 5)
        return self.checkBoxWidget()
    
    def updatePendingOperations(self):
        """Checks if value loaded from snapshot is different than write value
        (not cached) of the attribute, if yes it creates WriteAttrOperation 
        and adds it to _operations list"""
        model = self.getModelObj()
        try:
            snapValue = self.writeWidget().getValue()
            v = model.getValueObj(cache=False)
            modelValue = getattr(v,'r_value',v.value)
            equal = False
            #print 'updatePendingOperations(%s,%s,%s)'%(self.modelName,modelValue,snapValue)
            if model.isScalar():
                equal = model.areStrValuesEqual(modelValue, snapValue)
            elif model.isSpectrum():
                equal = areListsValuesEqual(modelValue, snapValue)
            if equal:
                self._operations = []
            else:
                operation = taurus.core.WriteAttrOperation(model, snapValue, 
                                                            self.getOperationCallbacks())
                operation.setDangerMessage(self.writeWidget().getDangerMessage())
                self._operations = [operation]
        except:
            self._operations = []
            print '!'*80+'\n'+'updatePendingOperations(%s)'%model
            print traceback.format_exc()
        self.updateStyle()
#    

    def updateLabelWidget(self):
        TaurusValue.updateLabelWidget(self)
        self.labelWidget().setAlignment(QtCore.Qt.AlignLeft)

    def getOperationCallbacks(self):
        return []
        
    def getPendingOperations(self):
        """This method returns pending operations only if this taurus value is selected"""
        if self.checkBoxWidget() and self.checkBoxWidget().isChecked():
            return self._operations
        return []
    
    def hasPendingOperations(self):
        return len(self._operations) > 0
    
    def handleEvent(self, src, type, value):
        """When Change and Periodic event comes we want to update pending operations,
        in case when value of attribute changed."""
        TaurusValue.handleEvent(self, src, type, value)
        if type == taurus.core.TaurusEventType.Change or type == taurus.core.TaurusEventType.Periodic:
            self.updatePendingOperations()
            self.updatePendingOpsStyle()
        
class ContextTaurusValue(TaurusValue):
    """TaurusValue used in new savesnap view of SnapSaver,
    it will hide from the user set value of attribute, because it is not used in 
    neither in SnapSaver nor in SnapLoader"""
    def __init__(self, parent = None):
        TaurusValue.__init__(self, parent)
        self.setLabelConfig('attr_fullname')
        self.setWriteWidgetClass(None)

    def updateLabelWidget(self):
        TaurusValue.updateLabelWidget(self)
        self.labelWidget().setAlignment(QtCore.Qt.AlignLeft)  
    
class NewContextTaurusValue(TaurusValue):
    """TaurusValue used in new context view of SnapSaver,
    it will hide from the user set value of attribute, because it is not used in 
    neither in SnapSaver nor in SnapLoader"""
    def __init__(self, parent = None):
        TaurusValue.__init__(self, parent)
        self.setCheckBoxWidget(None)
        self.setWriteWidgetClass(None)
        self.setLabelConfig('attr_fullname')
        
    def checkBoxWidget(self):
        return self._checkBoxWidget
    
    def setCheckBoxWidget(self, checkBoxWidget):
        self._checkBoxWidget = checkBoxWidget
    
    def setParent(self, parent):
        TaurusValue.setParent(self, parent)
        self.updateCheckBoxWidget()
        
    def setModel(self, model):
        TaurusValue.setModel(self, model)
#        if isinstance(self.writeWidget(),Qt.QLineEdit): 
#            self.writeWidget().setReadOnly(True)
#        elif isinstance(self.writeWidget(),Qt.QCheckBox):
#            self.writeWidget().setEnabled(False)
#        elif isinstance(self.writeWidget(),Qt.QAbstractButton):
#            self.writeWidget().setEnabled(False)
        self.updateCheckBoxWidget()
        
    def updateCheckBoxWidget(self):
        """Create and put check box at the very end of TaurusValue subwidgets"""
        self.setCheckBoxWidget(Qt.QCheckBox())
        if self.minimumHeight() is not None:
            self.checkBoxWidget().setMinimumHeight(self.minimumHeight())
        self._parentLayout.addWidget(self.checkBoxWidget(), self._row, 5)
        return self.checkBoxWidget()
        

    def updateLabelWidget(self):
        TaurusValue.updateLabelWidget(self)
        self.labelWidget().setAlignment(QtCore.Qt.AlignLeft)  
        
class LoadForm(TaurusForm):
    """Form used in main view of SnapLoader dialog"""
    
    def __init__(self, parent = None,
            formWidget = LoadTaurusValue,
            buttons = Qt.QDialogButtonBox.Apply|Qt.QDialogButtonBox.Cancel,
            withButtons = True ):    
        TaurusForm.__init__(self, parent, formWidget, buttons, withButtons)
        self._parent = parent
        self.buttonBox.button(Qt.QDialogButtonBox.Apply).setText('Load')
        self.setSorter(cmp)
        
    def sorter(self):
        return self._sorter
    
    def setSorter(self, sorter):
        self._sorter = sorter
        
    def fillWithChildren(self):
        """Overwrites super method. For writable attributes create a special Taurus value,
        with additional check box for choosing to 'load/not load' snap value."""
        print 'LoadForm.fillWithChildren(%s)'%str(self.getModel())[:80]
        frame = TaurusWidget()
        frame.setLayout(Qt.QGridLayout())
        models = self.getModel()              
        for model in sorted(models,self.sorter()): 
            model = str(model)
            if taurus.Attribute(model).isWritable():
                widget = self.getFormWidget()[0]
                widget = widget(frame)
            else:
                widget = TaurusValue(frame)
            widget.setMinimumHeight(24)
            widget.setModel(model)
        self.scrollArea.setWidget(frame)
        self.scrollArea.setMinimumWidth(frame.layout().sizeHint().width()+20)
        print('done')
            
    def loadValues(self, attrVal):
        """
        This method tries to load snap values (read values of attribute) into the 
        write widget of taurusvalues representing contexts attributes, 
        
        If this attribute is not accessible at that moment, the popup will inform 
        the user about this issue.
        
        Snapshot values are added as pending operations, but not applied yet.
        """
        damagedAttr = []
        from taurus.core import AttributeNameValidator
        print 'LoadForm.loadValues([%d])'%len(attrVal.keys())
        option,ok = Qt.QInputDialog.getItem(self,'Load Values',
          ('Choose values to load:\n(attribute,(read value,write value))\n\n'+
          '\n'.join(map(str,attrVal.items())))[:512],
          ['read value','write value'])
        if not ok: 
            return
        for child in self.scrollArea.widget().children():
            if isinstance(child, LoadTaurusValue):
                m = child.modelName.lower()
                if m not in attrVal:
                    m = AttributeNameValidator().getParams(m)
                    m = m['devicename']+'/'+m['attributename']
                #if child.getModelObj().isReadOnly(): continue
                if m in attrVal:
                    value = attrVal[m]
                    print('\t%s: %s'%(m,value))
                    writeWidget = child.writeWidget()
                    try:
                        v0 = str(writeWidget.getValue())
                        writeWidget.setValue(value[str(option)=='write value'])
                        try: writeWidget.emitValueChanged()
                        except: pass
                        child.updatePendingOperations()
                        child.updatePendingOpsStyle()
                        child.checkBoxWidget().setChecked(str(writeWidget.getValue())!=v0) 
                    except:
                        damagedAttr.append(m)
                        traceback.print_exc()
                else:
                    print '%s not in snapshot values'%m
        print '%d worked, %d failed'%(len(self.scrollArea.widget().children()),len(damagedAttr))
        if len(damagedAttr) > 0:
            Qt.QMessageBox.warning(self,"Could not load all the values from snapshot",
                                    "Could not load all the values from snapshot<br>" +
                                    "Problems occured for following attributes:<br>" + 
                                    "%s<br>" % str(damagedAttr) + 
                                    "Probably these attributes are not accessible.")
        print 'LoadForm.LoadValues(...): done'
        return True
    
    def checkAll(self, state):
        """This method selects/unselects all attributes from the form"""
        for child in self.scrollArea.widget().children():
            if isinstance(child, LoadTaurusValue):
                child.checkBoxWidget().setCheckState(Qt.Qt.CheckState(state))
                
    def apply(self):
        attrs = sorted((l.getModel(),l.writeWidget().getValue()) 
                       for l in self.scrollArea.widget().children() 
                       if isinstance(l, LoadTaurusValue) 
                       and l.getPendingOperations())
        qms = Qt.QMessageBox(Qt.QMessageBox.Information,"Load Values",
            "The following attribute values will be applied:\n%s"%'\n'.join('%s:\t%s'%(a,v) for a,v in attrs),
            Qt.QMessageBox.Ok|Qt.QMessageBox.Cancel)
        if qms.exec_()==qms.Ok:
            TaurusForm.apply(self)
            print('TaurusForm.apply(): Done!')
                
                
    
class ContextForm(TaurusForm):
    """Taurus form with 'Snap' button instead of 'Apply'. 
    This form is used in main view of SnapSaver dialog"""
    def __init__(self, parent = None,
                        formWidget = ContextTaurusValue, 
                        buttons = Qt.QDialogButtonBox.Save|Qt.QDialogButtonBox.Cancel,
                        withButtons = True):
        TaurusForm.__init__(self, parent, formWidget, buttons, withButtons)
        self.setSorter(cmp)
        self.buttonBox.button(Qt.QDialogButtonBox.Save).setText('Save')
        
    def sorter(self):
        return self._sorter
    
    def setSorter(self, sorter):
        self._sorter = sorter
        
    def fillWithChildren(self):
        frame = TaurusWidget()
        frame.setLayout(Qt.QGridLayout())
        models = sorted(self.getModel(), self.sorter())
        for model in models: 
            model = str(model)
            widget = self.getFormWidget()[0](frame)
            widget.setMinimumHeight(20)
            widget.setModel(model)
            widget.setParent(frame)
        self.scrollArea.setWidget(frame)
        self.scrollArea.setMinimumWidth(frame.layout().sizeHint().width()+20)
    
class NewContextForm(TaurusForm):
    """Form used while creating new context which will contain only selected taurusvalues"""

    def __init__(self, parent = None,
                formWidget = NewContextTaurusValue,
                buttons = Qt.QDialogButtonBox.Save|Qt.QDialogButtonBox.Cancel,
                withButtons = True ):    
        TaurusForm.__init__(self, parent, formWidget, buttons, withButtons)
        
        self.buttonBox.button(Qt.QDialogButtonBox.Save).setText('Create')
        self.addAttrButton = Qt.QPushButton(Qt.QIcon(":/actions/list-add.svg"),"Add Attribute...")
        self.buttonBox.addButton(self.addAttrButton, Qt.QDialogButtonBox.ActionRole)
        self.connect(self.addAttrButton, Qt.SIGNAL("clicked()"),self.onAddAttrButtonClicked )
        
    def checkAll(self, state):
        """Selects all attributes from the form"""
        for child in self.scrollArea.widget().children():
            if isinstance(child, NewContextTaurusValue):
                child.checkBoxWidget().setCheckState(Qt.Qt.CheckState(state))
        
    def checkedAttributes(self):
        """Returns a list of selected attributes"""
        attrs = []
        for child in self.scrollArea.widget().children():
            if isinstance(child, NewContextTaurusValue) and child.checkBoxWidget().isChecked():
                attrs.append(child._localModelName)
        return attrs
    
    def onAddAttrButtonClicked(self):
        """Called when 'Add attribute' button clicked, popups attrchooser dialog"""
        attrChooserDialog = AttributeChooserDialog(self)
        self.connect(attrChooserDialog.attrChooser, Qt.SIGNAL("AddAttrs"), self.onAddAttr)
        attrChooserDialog.exec_()
        
    def onAddAttr(self, attrList):
        """Creates new taurusvalues and adds them to the frame"""
        frame = self.scrollArea.widget()
        for attr in attrList:
            widget = self.getFormWidget()[0](frame)
            widget.setMinimumHeight(24)
            widget.setModel(attr)        
            
class LoadValuesWidget(Qt.QWidget):
    def __init__(self, parent=None, sorter=None):
        print '#'*80
        print ':: LoadValuesWidget(%s,%s)'%(parent,sorter)
        Qt.QWidget.__init__(self, parent)
        self.setLayout(Qt.QGridLayout())   
        selectAllLabel = Qt.QLabel("<b>Select attributes to be loaded:</b>")
        self.layout().addWidget(selectAllLabel, 0, 0, Qt.Qt.AlignLeft)   
        self.selectAll = Qt.QCheckBox("SelectAll", self)
        self.layout().addWidget(self.selectAll, 0, 1, Qt.Qt.AlignRight)
        self.setWindowTitle('Load Values from Snapshot')
        self.form = LoadForm(self)
        self.form.setSorter(sorter)
        self.layout().addWidget(self.form, 1, 0, 1, 2)
        Qt.QObject.connect(self.selectAll, Qt.SIGNAL("stateChanged(int)"), self.onSelectAllStateChanged)
        Qt.QObject.connect(self.form.buttonBox, Qt.SIGNAL("cancelClicked()"),self.onCancel)
        Qt.QObject.connect(self.form.buttonBox, Qt.SIGNAL("applyClicked()"),self.onApply)
        
    def setModel(self, model):
        self.form.setModel(model)
        
    def loadValues(self, attrVal):
        print '#'*80
        print 'LoadValuesWidget.loadValues([%d])'%len(attrVal)
        ok = self.form.loadValues(attrVal)
        if not ok:
            self.hide()
            self.form.close()
            self.close()
            return
        if len(attrVal):
            self.setEnableLoad(True)
        else:
            self.setEnableLoad(False)
        return True
        
    def onSelectAllStateChanged(self, state):
        self.form.checkAll(state)
        
    def onApply(self):
        print '#'*80
        print 'LoadValuesWidget.onApply()'
        self.emit(Qt.SIGNAL("loaded"))
        
    def onCancel(self):
        self.emit(Qt.SIGNAL("canceled"))
    
    def setEnableLoad(self, trueFalse):
        self.form.buttonBox.button(Qt.QDialogButtonBox.Apply).setEnabled(trueFalse)
        
#class LoadValuesDialog(Qt.QDialog):
    #def __init__(self,parent=None):
        #Qt.QDialog.__init__(self,parent=parent)
        #self.setLayout(Qt.QVBoxLayout())
        #self.loadValuesWidget = LoadValuesWidget(self)
        #self.layout().addWidget(self.loadValuesWidget)
        #Qt.QObject.connect(self.loadValuesWidget, Qt.SIGNAL("canceled"),self.reject)
    #def loadValues(self,attrVal): self.loadValuesWidget(attrVal)
    #def setModel(self,model): self.loadValuesWidget(model)
        
class AttributeChooserDialog(Qt.QDialog):
    """Dialog for choosing additional attribute in process of creating new context"""
    def __init__(self, parent=None):
        Qt.QDialog.__init__(self,parent)
        self.setLayout(Qt.QVBoxLayout())
        self.attrChooser = ContextAttrChooser(self)
        self.layout().addWidget(self.attrChooser)
        
class ContextAttrChooser(AttrChooser):
    """Widget used for choosing attributes from current database.
       Selected attributes are passed with signal to the NewContextForm object"""
    
    def __init__(self, parent=None, readWriteOnly=False):
        """readWriteOnly - indicates if we want to choose only from a list of
                           read/write attributes"""
        AttrChooser.__init__(self,parent)
        self.setReadWriteOnly(readWriteOnly)
        self.updateList([])
        self.ui.updateButton.setText("ADD TO CONTEXT")
        
    def isReadWriteOnly(self):
        return self._readWriteOnly
    
    def setReadWriteOnly(self, readWriteOnly):
        self._readWriteOnly = readWriteOnly
        
    def updateButtonClicked(self):
        """Emits a signal with list of selected attributes"""
        self.emit(Qt.SIGNAL("AddAttrs"), self.selectedItemsComplete)
        self.parent().accept()
        
    def setAttributes(self):
        """Fill the attributes list with attributes, if readWriteOnly was set to true,
           it will filter out only read/write attributes"""
        self.ui.attrList.clear()
        self.dev_name = str(self.ui.devList.currentItem().text())
        try:
            attrList = PyTango.DeviceProxy(self.dev_name).attribute_list_query()
            if self.isReadWriteOnly():  
                attrList = [a for a in attrList if a.writable != PyTango.AttrWriteType.READ]
            items=[a.name for a in attrList]
        except Exception,e:
            Qt.QMessageBox.warning(self,"Could not load list of attributes",
                  "Unable to contact with device %s: %s" % (self.dev_name,str(e)))
            items=[]

        for i in range(len(items)):
            self.ui.attrList.addItem(items[i])


class SnapDialog(Qt.QDialog):
    
    def __init__(self, credentials=None, parent=None, standalone=False, defaultContextID=None, sorter=None):
        """Constructor of the SnapLoader dialog
            credentials - if None SnapApi will connect to the host specified by 
                          db property "SnapArchiver"
                          if not None should be a list of 3 items where
                          credentials[0] - username
                          credentails[1] - hostname
                          credentials[2] - password
            parent - widget, parent of this dialog
            standalone - if set to true dialog has a features of standalone application"""
        Qt.QDialog.__init__(self, parent)
        self.context = None
        try: 
            if credentials is None:
                self.snapapi = SnapAPI()
            else:
                self.snapapi = SnapAPI('%s@%s' % (credentials[0], credentials[1]), credentials[2])
        except:
            print traceback.format_exc()
            Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                                    "Could not establish connection to SnapManager DS.<br>" + \
                                    "Please check if DS is running or if credentials are correct.")
        self.setStandalone(standalone)
        self.setSorter(sorter)
        self.setDefaultContextID(defaultContextID)
        self.initComponents()
        
    def initComponents(self):
        raise Exception('NotImplemented!')
        
    def initContexts(self):
        self.contextComboBox.clear()
        try:
            contexts = self.snapapi.get_contexts()
        except:
            err = traceback.format_exc()
            print err
            Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                                    "Could not talk with SnapManager DS.<br>" + \
                                    err)
        for context in contexts.values():
            self.contextComboBox.addItem("%s [%d]" % (context.name, context.ID), Qt.QVariant(context.ID))
        self.contextComboBox.model().sort(0, Qt.Qt.AscendingOrder)
        
    def isStandalone(self):
        return self._standalone
    
    def setStandalone(self, standalone):
        self._standalone = standalone
    
    def defaultContextID(self):
        return self._defaultContextID
        
    def setDefaultContextID(self, defaultContextID):
        self._defaultContextID = defaultContextID
        
    def onCancel(self):
        self.reject()
        
    def sorter(self):
        return self._sorter
        
    def setSorter(self, sorter):
        self._sorter = sorter
        

class SnapLoader(SnapDialog):
    
    def initComponents(self):
        self.setWindowTitle('Load Values from Snapshot')
        self.setLayout(Qt.QVBoxLayout())
        splitter = Qt.QSplitter(self)
        self.layout().addWidget(splitter)
        leftWidget = Qt.QWidget()
        leftWidget.setLayout(Qt.QGridLayout())
        #Author
        authorLabel = Qt.QLabel("Author:", self)
        leftWidget.layout().addWidget(authorLabel, 1, 0)
        self.author = Qt.QLabel(self)
        leftWidget.layout().addWidget(self.author, 1, 1)
        #Description
        descriptionLabel = Qt.QLabel("Description:", self)
        leftWidget.layout().addWidget(descriptionLabel, 2, 0)
        self.description = Qt.QLabel(self)
        leftWidget.layout().addWidget(self.description, 2, 1)
        #Snapshots
        snapshotsLabel = Qt.QLabel("Snapshots:", self)
        leftWidget.layout().addWidget(snapshotsLabel, 3, 0, 1, 2)
        self.snapshots = Qt.QListWidget()
        self.snapshots.setSelectionMode(Qt.QAbstractItemView.SingleSelection)
        leftWidget.layout().addWidget(self.snapshots, 4, 0, 1, 2)
        splitter.addWidget(leftWidget)
        #Attributes
        self.loadAttrsWidget = LoadValuesWidget(parent=self, sorter=self.sorter())
        splitter.addWidget(self.loadAttrsWidget)
        #Context
        contextLabel = Qt.QLabel("Context:", self)
        leftWidget.layout().addWidget(contextLabel, 0, 0)
        self.connect(self, Qt.SIGNAL("contextIDChanged"),self.onContextIDChanged)
        if self.defaultContextID():
            self.defaultContextLabel = Qt.QLabel()
            self.defaultContextLabel.setSizePolicy(Qt.QSizePolicy.Expanding, Qt.QSizePolicy.Preferred)
            leftWidget.layout().addWidget(self.defaultContextLabel, 0, 1)
            self.emit(Qt.SIGNAL("contextIDChanged"),self.defaultContextID())
        else:
            self.contextComboBox = Qt.QComboBox(self)
            self.contextComboBox.setSizePolicy(Qt.QSizePolicy.Expanding, Qt.QSizePolicy.Preferred)
            self.contextComboBox.setInsertPolicy(Qt.QComboBox.InsertAlphabetically)
            leftWidget.layout().addWidget(self.contextComboBox, 0, 1)
            self.connect(self.contextComboBox, Qt.SIGNAL("currentIndexChanged(int)"),self.onContextComboBoxChanged)
            self.initContexts()
        Qt.QObject.connect(self.snapshots, Qt.SIGNAL("currentItemChanged (QListWidgetItem *,QListWidgetItem *)"),self.onSnapshotChanged)
        Qt.QObject.connect(self.loadAttrsWidget, Qt.SIGNAL("canceled"),self.onCancel)
        Qt.QObject.connect(self.loadAttrsWidget, Qt.SIGNAL("loaded"),self.onApply)

        
    def onContextComboBoxChanged(self, idx):
        if idx == -1 or idx > self.contextComboBox.count(): return
        id = self.contextComboBox.itemData(idx)
        if hasattr(id,'toInt'): id = id.toInt()[0]
        self.emit(Qt.SIGNAL("contextIDChanged"),id)
        
    def onContextIDChanged(self, id):
        try:
            self.context = self.snapapi.get_context(id)
        except:
            Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                                    "Could not retrieve context with following ID: %d.<br>" % id + \
                                    "Please check if SnapManagerDS is running.<br>" + \
                                    "Also check if this context exists.")
            self.form.buttonBox.button(Qt.QDialogButtonBox.Apply).setEnabled(False)
            return
        self.author.setText("<b>%s</b>" % self.context.author)
        self.description.setText("<b>%s</b>" % self.context.description)
        if self.defaultContextID():
            self.defaultContextLabel.setText("<b>%s</b> [%d]" % (self.context.name, self.context.ID))
        contextAttributes = [attr['full_name'] for attr in self.context.get_attributes().values()]
        self.loadAttrsWidget.setModel(contextAttributes)
        if not len(contextAttributes):
            self.loadAttrsWidget.setEnableLoad(False)
            Qt.QMessageBox.warning(self,"Empty context",
                                   "This context appears to have no contents.")
        self.initSnapshots()
            
    def initSnapshots(self):
        self.snapshots.clear()
        snapshots = self.context.get_snapshots()
        for id in snapshots:
            item = Qt.QListWidgetItem()
            item.setText("%s - %s [ID: %d]" % (snapshots.get(id)[0], snapshots.get(id)[1], id))
            item.setData(Qt.Qt.UserRole, Qt.QVariant(id))
            self.snapshots.addItem(item)
        self.snapshots.model().sort(0, Qt.Qt.AscendingOrder)        
        
    def onSnapshotChanged(self, current, previous):
        if current == None: return
        id = current.data(Qt.Qt.UserRole).toInt()[0]
        snapshot = self.context.get_snapshot(id)     
        self.loadAttrsWidget.loadValues(snapshot)
        if not len(snapshot):
            Qt.QMessageBox.warning(self,"Empty snapshot", "You tried to load an empty snapshot.")
        
    def onApply(self):
        print 'SnapLoader.onApply()'
        self.loadAttrsWidget.selectAll.setChecked(False)
        if not self.isStandalone():
            self.accept()
            
def TakeSnapshot(context_id,parent=None):
    try:
        prompt=Qt.QInputDialog
        comment, ok=prompt.getText(parent, 'Input dialog', 'Type a name for the snapshot:')
        if ok and len(str(comment)) != 0:
            try:
                ctx=SnapAPI().get_context(context_id)
                assert ctx.take_snapshot(str(comment)),'Snapshot failed!'
            except:
                fandango.qt.QExceptionMessage(traceback.format_exc())
                return
            Qt.QMessageBox.information(parent,"Snapshot","Snapshot taken succesfully!")
            return context_id
        elif ok and len(str(comment)) == 0:
            return TakeSnapshot(context_id)
    except: Qt.QMessageBox.warning(parent,"Error",traceback.format_exc())
        
class SnapSaver(SnapDialog):
    """This class extends SnapDialog, it works in two views,
    one for making snapshots, and the other for creating new context.
    By using method setStartupRegexp, instead of creating new context 
    from the scratch you will start with a list of attributes fulfilling 
    this regexp."""
    
    def __init__(self, credentials=None, parent=None, standalone=False, defaultContextID=None, sorter=None):
        #this will store list of attributes in case when while an exception occurs while creating new context
        self.safetyAttribues = []
        self.setStartupModels([])
        SnapDialog.__init__(self, credentials, parent, standalone, defaultContextID, sorter)
        
        
    def onContextComboBoxChanged(self, idx):
        if idx == -1 or idx > self.contextComboBox.count(): return
        id = self.contextComboBox.itemData(idx)
        if hasattr(id,'toInt'): id = id.toInt()[0]
        self.emit(Qt.SIGNAL("contextIDChanged"),id)
        
    def onContextIDChanged(self, id):
        try:
            self.context = self.snapapi.get_context(id)
        except:
            Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                                    "Could not retrieve context with following ID: %d.<br>" % id +\
                                    "Please check if SnapManagerDS is running.<br>" + \
                                    "Also check if this context exists.")
            self.form.buttonBox.button(Qt.QDialogButtonBox.Save).setEnabled(False)
            return
        self.author.setText("<b>%s</b>" % self.context.author)
        self.reason.setText("<b>%s</b>" % self.context.reason)
        self.description.setText("<b>%s</b>" % self.context.description)
        contextAttributes = [attr['full_name'] for attr in self.context.get_attributes().values()]
        self.form.setModel(contextAttributes)
        if len(contextAttributes):
            self.form.buttonBox.button(Qt.QDialogButtonBox.Save).setEnabled(True)
            self.comment.setEnabled(True)
        else:
            self.form.buttonBox.button(Qt.QDialogButtonBox.Save).setEnabled(False)
            self.comment.setEnabled(False)
            Qt.QMessageBox.warning(self,"Empty context",
                                    "This context appears to have no contents,<br> you will not be able to create any snapshots.")
        
    def startupModels(self):
        return self._startupModels
    
    def setStartupModels(self, models):
        self._startupModels = models
        
    def startupRegexp(self):
        return self._startupRegexp
    
    def setStartupRegexp(self, regexp):
        self._startupRegexp = regexp
        self.setStartupModels(get_all_models(self.startupRegexp()))
        
    def initComponents(self):
        self.setLayout(Qt.QVBoxLayout())
        gridLayout = Qt.QGridLayout()
        #Author
        authorLabel = Qt.QLabel("Author:", self)
        gridLayout.addWidget(authorLabel, 1, 0)
        self.author = Qt.QLabel(self)
        gridLayout.addWidget(self.author, 1, 1, 1, 2)
        self.authorLineEdit = Qt.QLineEdit(self)
        self.authorLineEdit.setVisible(False)
        gridLayout.addWidget(self.authorLineEdit, 1, 1, 1, 2)
        #Reason
        reasonLabel = Qt.QLabel("Reason:", self)
        gridLayout.addWidget(reasonLabel,2, 0)
        self.reason = Qt.QLabel(self)
        gridLayout.addWidget(self.reason, 2, 1, 1, 2)
        self.reasonLineEdit = Qt.QLineEdit(self)
        self.reasonLineEdit.setText("SnapSaver")
        self.reasonLineEdit.setVisible(False)
        gridLayout.addWidget(self.reasonLineEdit, 2, 1, 1, 2)
        #Description
        descriptionLabel = Qt.QLabel("Description:", self)
        gridLayout.addWidget(descriptionLabel, 3, 0)
        self.description = Qt.QLabel(self)
        gridLayout.addWidget(self.description, 3, 1, 1, 2)   
        self.descriptionLineEdit = Qt.QLineEdit(self)
        self.descriptionLineEdit.setVisible(False)
        gridLayout.addWidget(self.descriptionLineEdit, 3, 1, 1, 2)
        #SeparationLine
        self.separationLine = Qt.QFrame(self)
        self.separationLine.setFrameStyle(Qt.QFrame.HLine + Qt.QFrame.Plain)
        self.separationLine.setFixedHeight(30)
        gridLayout.addWidget(self.separationLine,4,0,1,3)
        #SnapComment
        self.commentLabel = Qt.QLabel("Comment:", self)
        gridLayout.addWidget(self.commentLabel, 5, 0)
        self.comment = Qt.QLineEdit(self)
        gridLayout.addWidget(self.comment, 5, 1, 1, 2)   
        
        self.layout().addLayout(gridLayout)
        
        #Form
        self.selectAll = Qt.QCheckBox("SelectAll", self)
        self.selectAll.hide()
        gridLayout.addWidget(self.selectAll, 4, 1, Qt.Qt.AlignRight)
        
        self.form = ContextForm(self)
        self.form.setSorter(self.sorter())
        self.layout().addWidget(self.form)
        
        self.newForm = NewContextForm(self)
        self.newForm.hide()
           
        
        self.layout().addWidget(self.newForm)
        #Context
        self.contextLabel = Qt.QLabel("Context:", self)
        gridLayout.addWidget(self.contextLabel, 0, 0)
        self.connect(self, Qt.SIGNAL("contextIDChanged"),self.onContextIDChanged)
        if self.defaultContextID():
            self.emit(Qt.SIGNAL("contextIDChanged"),self.defaultContextID())
            if self.context:
                self.defaultContextLabel = Qt.QLabel("<b>%s</b> [%d]" % (self.context.name, self.context.ID),self)
                self.defaultContextLabel.setSizePolicy(Qt.QSizePolicy.Expanding, Qt.QSizePolicy.Preferred)
                gridLayout.addWidget(self.defaultContextLabel, 0, 1)
        else:
            self.contextComboBox = Qt.QComboBox(self)
            self.contextComboBox.setInsertPolicy(Qt.QComboBox.InsertAlphabetically)
            self.contextComboBox.setSizePolicy(Qt.QSizePolicy.Expanding, Qt.QSizePolicy.Preferred)
            gridLayout.addWidget(self.contextComboBox, 0, 1)
            self.contextLineEdit = Qt.QLineEdit(self)
            self.contextLineEdit.setSizePolicy(Qt.QSizePolicy.Expanding, Qt.QSizePolicy.Preferred)
            self.contextLineEdit.hide()
            gridLayout.addWidget(self.contextLineEdit, 0, 1)
            self.newButton = Qt.QPushButton("New...",self)
            self.newButton.setSizePolicy(Qt.QSizePolicy.Minimum, Qt.QSizePolicy.Preferred)
            gridLayout.addWidget(self.newButton, 0, 2)
            Qt.QObject.connect(self.contextComboBox, Qt.SIGNAL("currentIndexChanged(int)"),self.onContextComboBoxChanged)
            Qt.QObject.connect(self.newButton, Qt.SIGNAL("clicked()"),self.onNew)
            self.initContexts()

        
        
        
        Qt.QObject.connect(self.form.buttonBox, Qt.SIGNAL("saveClicked()"), self.onSaveSnapshot)
        Qt.QObject.connect(self.newForm.buttonBox, Qt.SIGNAL("saveClicked()"), self.onCreateContext)
        Qt.QObject.connect(self.newForm.buttonBox, Qt.SIGNAL("cancelClicked()"), self.onFinishNew)
        Qt.QObject.connect(self.form.buttonBox, Qt.SIGNAL("cancelClicked()"), self.onCancel)
        Qt.QObject.connect(self.selectAll, Qt.SIGNAL("stateChanged(int)"), self.newForm.checkAll)
        
        
    def onNew(self):
        """This method is used to switch from snapshot view to new context view."""
        self.contextLabel.setText("Name:")
        self.contextComboBox.hide()
        self.newButton.hide()
        self.author.hide()
        self.authorLineEdit.show()
        self.reason.hide()
        self.reasonLineEdit.show()
        self.description.hide()
        self.descriptionLineEdit.show()
        self.comment.hide()
        self.commentLabel.hide()
        self.form.hide()
        self.separationLine.hide()
        
        self.contextLineEdit.show()
        self.selectAll.show()
        self.newForm.setModel(self.startupModels())
        if self.safetyAttribues:
            self.newForm.setModel(self.safetyAttribues)
        self.newForm.show()
        self.adjustSize()
    
    def onFinishNew(self):
        """This method is used to switch from new context view to new snapshot view."""
        self.contextLabel.setText("Context:")
        self.contextLineEdit.hide()
        self.selectAll.setChecked(False)
        self.selectAll.hide()
        self.newForm.setModel([])
        self.newForm.hide()
        
        self.contextComboBox.show()
        self.onContextComboBoxChanged(1)
        self.newButton.show()
        self.authorLineEdit.hide()
        self.author.show()
        self.reasonLineEdit.hide()
        self.reason.show()
        self.descriptionLineEdit.hide()
        self.description.show()
        self.comment.show()
        self.commentLabel.show()
        self.form.show()
        self.separationLine.show()
        
        self.adjustSize()
        
            
    def setModels(self, models):
        self.newFormModels = models
        
    def onSaveSnapshot(self):
        description = str(self.comment.text())
        if len(description.lstrip()) == 0:
            Qt.QMessageBox.information(self,"Comment missing",
                                    "Please fill the comment field,<br>" + \
                                    "otherwise you will not be able to save the snapshot.<br>")
            return                
        if len(self.context.attributes) > 100:
            Qt.QMessageBox.information(self,"Long time operation",
                                "You are going to make a snapshot<br>" + \
                                "on more than 100 attributes.<br>" + \
                                "It will take a while.")
        try:    
            self.context.take_snapshot(description)
        except PyTango.DevFailed:
            traceback.print_exc()
            Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                                    "Could not save snapshot.<br>" + \
                                    "Was not possible to talk with SnapManager DS.<br>" + \
                                    "Please check if DS is running.<br>")
                
        if not self.isStandalone():
            self.accept()
            
    def onCreateContext(self):
        name = str(self.contextLineEdit.text())
        author = str(self.authorLineEdit.text())
        reason = str(self.reasonLineEdit.text())
        description = str(self.descriptionLineEdit.text())
        attributes = self.newForm.checkedAttributes()
        notAllowedAttrs = [attr for attr in attributes if not self.snapapi.check_attribute_allowed(attr)]
        
        alert = ""
        if len(name.lstrip()) == 0:
            alert += "Field <b>name</b> is empty. Please fill it out.<br>"
        if len(author.lstrip()) == 0:
            alert += "Field <b>author</b> is empty. Please fill it out.<br>"
        if len(reason.lstrip()) == 0:
            alert += "Field <b>reason</b> is empty. Please fill it out.<br>"
        if len(description.lstrip()) == 0:
            alert += "Field <b>description</b> is empty. Please fill it out.<br>"
        if len(attributes) == 0:
            alert += "No <b>attributes</b> have been checked. Please select at least one.<br>"
        if len(notAllowedAttrs) > 0:
            alert += "Archiving system doesn't allow to store following attributes: <br><b>" + str(notAllowedAttrs)[1:-1] + \
                     "</b><br>Please remove them from context."
        if len(alert) != 0:
            Qt.QMessageBox.warning(self,"Wrong or missing context information", alert)
            return
        
        alert = ""
        nr_attr = len(attributes)
        attributes = set(attributes) 
        nr_without_dup = len(attributes)
        if nr_attr > nr_without_dup:
            alert += "Duplicate attributes has been found and filtered.<br>"
        if nr_attr > 100:
            alert += "You are going to create a context<br>" + \
                     "from more than 100 attributes.<br>" + \
                     "It will take a while.<br>"
        if len(alert) != 0:
            Qt.QMessageBox.information(self,"Context creation informations", alert)

        try:
#            raise Exception
            self.snapapi.create_context(author, name, reason, description, attributes)
            self.safetyAttribues = []
        except Exception:
            self.safetyAttribues = attributes
            traceback.print_exc()
            Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                                    "Could not create new context.<br>" + \
                                    "Was not possible to talk with SnapManager DS.<br>" + \
                                    "Please check if DS is running.")
        self.onFinishNew()
        self.initContexts()
        
    
def main():
    app = TaurusApplication(sys.argv)
    #credentials = ('browser', 'controls02', 'browser')
    if len(sys.argv) > 1:    
        if sys.argv[1] == "save":
            dialog = SnapSaver(standalone=True)
            dialog.setWindowTitle("Snap saver")
#            dialog.setMinimumSize(Qt.QSize(400,500))
            if len(sys.argv) == 3:
                dialog.setStartupRegexp(sys.argv[2])
        elif sys.argv[1] == "load":
            dialog = SnapLoader(standalone=True)
            dialog.setWindowTitle("Snap loader")
#            dialog.setMinimumSize(Qt.QSize(400,500))
        elif sys.argv[1:]:
            dialog = SnapLoader(standalone=True)
            dialog.setWindowTitle("Snap saver (%s)"%sys.argv[1])
            dialog.setStartupRegexp(sys.argv[1])
        dialog.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main() 
    
    
    
    
#import tau
#from tau.widget import TauForm, TauValue, TauWidget,\
                       #TauLauncherButton, TauValueLineEdit
#from tau.widget.qwt import TauPlot 
#from tau.widget.dialog import AttrChooser 
#from tau.widget import TauValuesTable
#from tau.widget.resources import qrc_tango_icons_emblems

#def re_match_low(regexp,target): return re.match(regexp.lower(),target.lower())

#def get_all_models(expressions, readWrite=False, limit=1000):
    #print 'In get_all_models(%s:"%s") ...' % (type(expressions),expressions)
    #if isinstance(expressions,str):
        #if any(re.match(s,expressions) for s in ('\{.*\}','\(.*\)','\[.*\]')):
            #print 'evaluating expressions ....'
            #expressions = list(eval(expressions))
        #else:
            #print 'expressions as string separated by commas ...'
            #expressions = expressions.split(',')    
    #elif any(isinstance(expressions,klass) for klass in (Qt.QStringList,list,tuple,dict)):
        #print 'expressions converted from list ...'
        #expressions = list(str(e) for e in expressions)
        
    #print 'In TauGrid.get_all_models(%s:"%s") ...' % (type(expressions),expressions)
    #tau_db = tau.Database()
    #if 'SimulationDatabase' in str(type(tau_db)):
        #print 'Using a simulated database ...'
        #models = expressions
    #else:
        #all_devs = tau_db.get_device_exported('*')
        #models = []
        #for exp in expressions:
            #print 'evaluating exp = "%s"' % exp
            #exp = str(exp)
            #devs = []
            #targets = []
            #if exp.count('/')==3:
                #device,attribute = exp.rsplit('/',1)
            #else: 
                #device,attribute = exp,'.*'
                
            #if any(c in device for c in '.*[]()+?'):
                #if '*' in device and '.*' not in device: device = device.replace('*','.*')
                #devs = [s for s in all_devs if re_match_low(device,s)]
            #else:
                #devs = [device]
              
            #print 'get_all_models(): devices matched by %s / %s are %d:' % (device,attribute,len(devs))
            #print '%s' % (devs)
            #for dev in devs:
                #if any(c in attribute for c in '.*[]()+?'):
                    #if '*' in attribute and '.*' not in attribute: attribute = attribute.replace('*','.*')
                    #try: 
                        #tau_dp = tau.Device(dev)
                        #if readWrite:     
                            #attrs = [att.name for att in tau_dp.attribute_list_query() if (re_match_low(attribute,att.name) and att.writable != PyTango.AttrWriteType.READ)]
                        #else:
                            #attrs = [att.name for att in tau_dp.attribute_list_query() if re_match_low(attribute,att.name)]
                        #targets.extend(dev+'/'+att for att in attrs)
                    #except Exception,e: 
                        #print 'ERROR! get_all_models(): Unable to get attributes for device %s: %s' % (dev,str(e))
                        #print traceback.format_exc()
                #else: targets.append(dev+'/'+attribute)
            #models.extend(targets)
    #models = models[:limit]
    #return models
