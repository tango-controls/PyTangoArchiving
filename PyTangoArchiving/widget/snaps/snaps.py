#############################################################################
##
## file :       PyTangoArchiving/widget/snaps/snaps.py
##
## description : see below
##
## project :     Tango Control System
##
## $Author: Sergi Rubio
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

import sys, traceback, re, os, time
import taurus, fandango, fandango.qt
from fandango.dicts import reversedict
from fandango.qt import QExceptionMessage
from PyTangoArchiving import SnapAPI

try:
    from taurus.external.qt import Qt, QtCore, QtGui
except:
    from PyQt4 import Qt,QtCore,QtGui
from taurus.qt.qtgui import container
from snapdialogs import SnapDialog
from ui.core import *
from ui.diff import *
from ui.modify import *

class diffWidget(QtGui.QWidget):
    def __init__(self,parent=None):
        QtGui.QWidget.__init__(self,parent)
        self._wi=Diff_Ui_Form()
        self._wi.diffSetupUi(self)
        
def cast_value(TYPE,V):
    try:
        return TYPE(V)
    except Exception,e:
        print '%s(%s) Failed!!'%(TYPE,str(V)[:80])
        print traceback.format_exc()
        return 0
    
def get_as_int(item):
    try:
        return item.toInt()[0]
    except:
        try:
            return int(item)
        except:
            return item
    
def get_item_id(item):
    i = item.data(Qt.Qt.UserRole)
    return get_as_int(i)

class ContextEditWidget(QtGui.QWidget):
    def __init__(self,parent=None,ctxID=None):
      self.ctxID = ctxID
      QtGui.QWidget.__init__(self,None)
      self.ui = ContextEditUi()
      self.ui.setupUi(self)
      if ctxID is not None:
          self.fillForm(ctxID)

    def fillForm(self,cid):
        self.snapapi = SnapAPI()
        self.ctx = self.snapapi.get_context(cid)
        try: self.ctx.get_attributes(update=True)
        except:
            self.ctx.attributes = {}
            self.ctx.get_attributes()#update=True may not be updated in the last bliss package?
        self.ui.line_name.setText(self.ctx.name)
        self.ui.line_author.setText(self.ctx.author)
        self.ui.line_reason.setText(self.ctx.reason)
        self.ui.line_description.setText(self.ctx.description)
        attributes = self.ctx.get_attributes()#get_attributes_data
        self.ui.attch.updateList([v['full_name'] for v in attributes.values()])

    def onCreatePressed(self):

        alert = ""
        if len(str(self.ui.line_name.text()).lstrip()) == 0:
            alert += "Field <b>name</b> is empty. Please fill it out.<br>"
        if len(str(self.ui.line_author.text()).lstrip()) == 0:
            alert += "Field <b>author</b> is empty. Please fill it out.<br>"
        if len(str(self.ui.line_reason.text()).lstrip()) == 0:
            alert += "Field <b>reason</b> is empty. Please fill it out.<br>"
        if len(str(self.ui.line_description.text()).lstrip()) == 0:
            alert += "Field <b>description</b> is empty. Please fill it out.<br>"
        if self.ui.final_List.count() == 0:
            alert += "No <b>attributes</b> have been checked. Please select at least one.<br>"
        if len(alert) != 0:
            Qt.QMessageBox.warning(self, "Error", alert)
            return
        if self.ctxID:
            self.modifyContext(self.ctxID)
        else:
            self.createNewContext()

    def createNewContext(self):
        try:
            self.snapapi = SnapAPI()
            attributes=[]
            for i in range(self.ui.final_List.count()):
                attributes.append(str(self.ui.final_List.item(i).text()))
            self.snapapi.create_context(str(self.ui.line_author.text()),str(self.ui.line_name.text()),str(self.ui.line_reason.text()),str(self.ui.line_description.text()),attributes)
        except Exception:
            print traceback.format_exc()
            Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                                    "Could not create new context.<br>" + \
                                    "Was not possible to talk with SnapManager DS.<br>" + \
                                    "Please check if DS is running.")
            return
        self.newID = self.snapapi.contexts.keys()[len(self.snapapi.contexts.items())-1]
        print('un contexte nou: %d' %self.newID)
        self.emit(QtCore.SIGNAL("NewContextCreated(int)"), self.newID)
        self.onCancelPressed()
        Qt.QMessageBox.information(self,"Context","Context created succesfully!")

    def modifyContext(self, cid):
        print 'In modifyContext(%s)'%cid
        try:
            attributes=[]
            for i in range(self.ui.final_List.count()):
                attributes.append(str(self.ui.final_List.item(i).text()))
            #self.snapapi = SnapAPI()
            self.ctx.name=str(self.ui.line_name.text())
            self.ctx.author=str(self.ui.line_author.text())
            self.ctx.reason=str(self.ui.line_reason.text())
            self.ctx.description=str(self.ui.line_description.text())
            #self.snapapi.db.update_context(self.ctx)
            #self.snapapi.db.update_context_attributes(cid, attributes)
            self.snapapi.modify_context(cid,self.ctx.author,self.ctx.name,
                self.ctx.reason,self.ctx.description,attributes)
        except Exception:
            print traceback.format_exc()
            Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                                    "Could not modify context.<br>" + \
                                    "Was not possible to talk with SnapManager DS.<br>" + \
                                    "Please check if DS is running.")
            return
        Qt.QMessageBox.information(self,"Context","Context modified succesfully!")
        self.emit(QtCore.SIGNAL("ContextModified(int)"), cid)
        self.onCancelPressed()

    def onCancelPressed(self):
        self.close()

class SnapForm(Snap_Core_Ui_Form, SnapDialog):
    
    def show(self):
        if getattr(self,'_Form',None):
            self._Form.show()
        else:
            SnapDialog.show(self)

    def setupUi(self, Form, load=True):
        Snap_Core_Ui_Form.setupUi(self,Form)
        
        self.context = None
        self.snapshots = []
        self.snapapi = SnapAPI()
        
        self._Form=Form
        Form.setWindowTitle(QtGui.QApplication.translate("Form",'!:'+fandango.get_tango_host().split(':',1)[0]+' Snaps', None, QtGui.QApplication.UnicodeUTF8))
        self.contextComboBox.setToolTip(QtGui.QApplication.translate("Form", "Choose a Context", None, QtGui.QApplication.UnicodeUTF8))
        print 'connecting signals ...'
        QtCore.QObject.connect(self.contextComboBox,QtCore.SIGNAL("currentIndexChanged(int)"), self.onContextChanged)
        QtCore.QObject.connect(self.contextComboBox,QtCore.SIGNAL("activated(int)"), self.onContextChanged)
        self.contextComboBox.setMaximumWidth(250)
        self.comboLabel.setText(QtGui.QApplication.translate("Form", "Context:", None, QtGui.QApplication.UnicodeUTF8))
        
        self.buttonNew.setText(QtGui.QApplication.translate("Form", "New", None, QtGui.QApplication.UnicodeUTF8))
        icon_view=QtGui.QIcon(":/actions/document-new.svg")
        self.buttonNew.setIcon(icon_view)
        QtCore.QObject.connect(self.buttonNew,QtCore.SIGNAL("pressed()"), self.onNewPressed)
        self.buttonNew.setToolTip(QtGui.QApplication.translate("Form", "New Context", None, QtGui.QApplication.UnicodeUTF8))
        
        self.buttonEditCtx.setText(QtGui.QApplication.translate("Form", "Edit", None, QtGui.QApplication.UnicodeUTF8))
        icon_view=QtGui.QIcon(":/apps/accessories-text-editor.svg")
        self.buttonEditCtx.setIcon(icon_view)
        QtCore.QObject.connect(self.buttonEditCtx,QtCore.SIGNAL("pressed()"), self.onEditPressed)
        self.buttonEditCtx.setToolTip(QtGui.QApplication.translate("Form", "Edit Context", None, QtGui.QApplication.UnicodeUTF8))
        print 'connected signals ...'
        
        self.filterLabel.setText(QtGui.QApplication.translate("Form", "Filter:", None, QtGui.QApplication.UnicodeUTF8))
        self.filterComboBox.addItem('Name')
        self.filterComboBox.addItem('Reason')
        self.filterComboBox.addItem('Attributes')
        self.filterComboBox.setMaximumWidth(90)
        self.filterComboBox2.setEditable(True)
        QtCore.QObject.connect(self.filterComboBox,QtCore.SIGNAL("currentIndexChanged(int)"), self.onFilterComboChanged)        
        refresh_icon=QtGui.QIcon(":/actions/view-refresh.svg")
        self.refreshButton.setIcon(refresh_icon)
        QtCore.QObject.connect(self.refreshButton,QtCore.SIGNAL("pressed()"), self.onRefreshPressed)
        self.refreshButton.setToolTip(QtGui.QApplication.translate("Form", "Refresh List", None, QtGui.QApplication.UnicodeUTF8))

        self.infoLabel1_1.setText(QtGui.QApplication.translate("Form", "Author:", None, QtGui.QApplication.UnicodeUTF8))
        self.infoLabel2_1.setText(QtGui.QApplication.translate("Form", "Reason:", None, QtGui.QApplication.UnicodeUTF8))
        self.infoLabel3_1.setText(QtGui.QApplication.translate("Form", "Description:", None, QtGui.QApplication.UnicodeUTF8))
        self.infoLabel4_1.setText(QtGui.QApplication.translate("Form", "Snapshots:", None, QtGui.QApplication.UnicodeUTF8))
        
        self.buttonTake.setText(QtGui.QApplication.translate("Form", "Take Snapshot", None, QtGui.QApplication.UnicodeUTF8))
        self.buttonTake.setToolTip(QtGui.QApplication.translate("Form", "Save Snapshot in Database", None, QtGui.QApplication.UnicodeUTF8))
        icon_save=QtGui.QIcon(":/devices/camera-photo.svg")
        #icon_save=QtGui.QIcon(":/actions/document-save.svg")not_archived_HDB = [a for a in [t for t in csv if 'HDB' in csv[t]] if a not in last_values_HDB or last_values_HDB[a] is None]
        self.buttonTake.setIcon(icon_save)
        
        QtCore.QObject.connect(self.buttonTake,QtCore.SIGNAL("pressed()"), self.onSavePressed)
        self.buttonLoad.setText(QtGui.QApplication.translate("Form", "Load to Devices", None, QtGui.QApplication.UnicodeUTF8))
        QtCore.QObject.connect(self.buttonLoad,QtCore.SIGNAL("pressed()"), self.onLoadPressed)
        self.buttonLoad.setToolTip(QtGui.QApplication.translate("Form", "Load Snapshot to Devices", None, QtGui.QApplication.UnicodeUTF8))
        icon_load=QtGui.QIcon(":/actions/go-jump.svg")
        self.buttonLoad.setIcon(icon_load)
        
        self.buttonImport.setText(QtGui.QApplication.translate("Form", "Import from CSV", None, QtGui.QApplication.UnicodeUTF8))
        QtCore.QObject.connect(self.buttonImport,QtCore.SIGNAL("pressed()"), self.onImportPressed)
        self.buttonImport.setToolTip(QtGui.QApplication.translate("Form", "Import from CSV File", None, QtGui.QApplication.UnicodeUTF8))
        icon_csv=QtGui.QIcon(":/actions/document-open.svg")
        self.buttonImport.setIcon(icon_csv)        

        self.buttonExport.setText(QtGui.QApplication.translate("Form", "Export to CSV", None, QtGui.QApplication.UnicodeUTF8))
        QtCore.QObject.connect(self.buttonExport,QtCore.SIGNAL("pressed()"), self.onExportPressed)
        self.buttonExport.setToolTip(QtGui.QApplication.translate("Form", "Export to CSV File", None, QtGui.QApplication.UnicodeUTF8))
        icon_csv=QtGui.QIcon(":/actions/document-save-as.svg")
        self.buttonExport.setIcon(icon_csv)
        
        self.buttonDelSnap.setText(QtGui.QApplication.translate("Form", "Delete", None, QtGui.QApplication.UnicodeUTF8))
        QtCore.QObject.connect(self.buttonDelSnap,QtCore.SIGNAL("pressed()"), self.onDelSnapPressed)
        self.buttonDelSnap.setToolTip(QtGui.QApplication.translate("Form", "Delete Snapshot", None, QtGui.QApplication.UnicodeUTF8))
        icon_csv=QtGui.QIcon(":/actions/edit-clear.svg")
        self.buttonDelSnap.setIcon(icon_csv)
        
        self.buttonEditSnap.setText(QtGui.QApplication.translate("Form", "Edit", None, QtGui.QApplication.UnicodeUTF8))
        QtCore.QObject.connect(self.buttonEditSnap,QtCore.SIGNAL("pressed()"), self.onEditSnapPressed)
        self.buttonEditSnap.setToolTip(QtGui.QApplication.translate("Form", "Edit Snap Comment", None, QtGui.QApplication.UnicodeUTF8))
        icon_csv=QtGui.QIcon(":/apps/accessories-text-editor.svg")
        self.buttonEditSnap.setIcon(icon_csv)        
        
        self.buttonDelCtx.setText(QtGui.QApplication.translate("Form", "Delete", None, QtGui.QApplication.UnicodeUTF8))
        QtCore.QObject.connect(self.buttonDelCtx,QtCore.SIGNAL("pressed()"), self.onDelCtxPressed)
        self.buttonDelCtx.setToolTip(QtGui.QApplication.translate("Form", "Delete Context", None, QtGui.QApplication.UnicodeUTF8))
        icon_csv=QtGui.QIcon(":/actions/mail-mark-junk.svg")
        self.buttonDelCtx.setIcon(icon_csv)        

        QtCore.QObject.connect(self.buttonClose,QtCore.SIGNAL("pressed()"), self.onClosePressed)


        self.tableLabel.setText(QtGui.QApplication.translate("Form", "Attribute list:", None, QtGui.QApplication.UnicodeUTF8))
        self.tableWidget.setSelectionBehavior(QtGui.QAbstractItemView.SelectRows)
        self.listWidget.setSelectionMode(QtGui.QAbstractItemView.SingleSelection)
        QtCore.QObject.connect(self.listWidget, QtCore.SIGNAL("currentItemChanged (QListWidgetItem *,QListWidgetItem *)"), self.onSnapshotChanged)
        QtCore.QObject.connect(self.viewComboBox,QtCore.SIGNAL("currentIndexChanged(int)"), self.changeView)
        #self.frame.setMaximumWidth(450)
        self.comp=diffWidget()
        self.comp.setMinimumWidth(450)
        QtCore.QObject.connect(self.comp._wi.diffButtonCompare,QtCore.SIGNAL("pressed()"), self.onCompareButtonPressed)
        self.viewComboBox.setToolTip(QtGui.QApplication.translate("Form", "Change View Mode", None, QtGui.QApplication.UnicodeUTF8))
        self.comboLabel.show()
        
        self.gridLayout.addWidget(self.tableWidget)
        self.gridLayout.addWidget(self.taurusForm)
        self.gridLayout.addWidget(self.comp)

        if load: self.initContexts()
        
    def initComponents(self):
        pass #Just to be compatible with SnapDialog
        
    def initContexts(self, attrlist=[], sid=None):
        """
        This method overrides SnapDialog.initContexts()
        """
        #self.contextComboBox.blockSignals(True)
        #self.contextComboBox.clear()
        contexts = None
        self._Form.setWindowTitle(QtGui.QApplication.translate("Form",
            fandango.get_tango_host().split(':',1)[0]+
            ' -> Snapshoting', None, QtGui.QApplication.UnicodeUTF8))
        print('SnapForm.initContexts(%s(%s),%s(%s))' 
            % (type(attrlist),attrlist,type(sid),sid))
        try:
            if attrlist:
                if all(map(fandango.isNumber,(attrlist,sid))):
                    print('SnapForm.initContexts(int(%s),%s)' % (attrlist,sid))
                    contexts={attrlist: self.snapapi.get_context(attrlist)}
                    self._Form.setWindowTitle(QtGui.QApplication.translate("Form",
                        fandango.get_tango_host().split(':',1)[0]+' -> Snapshots for Context "'
                        +str(self.snapapi.db.get_id_contexts(attrlist)[0]['name'])+'"', 
                        None, QtGui.QApplication.UnicodeUTF8))
                elif fandango.isSequence(attrlist):
                    contexts=dict((ID,self.snapapi.get_context(ID)) for ID in self.snapapi.db.find_context_for_attribute(attrlist))
                    self._Form.setWindowTitle(QtGui.QApplication.translate("Form",
                        fandango.get_tango_host().split(':',1)[0]
                        +' -> Snapshots filtered by window content', 
                        None, QtGui.QApplication.UnicodeUTF8))
            else:
                contexts=self.snapapi.get_contexts()
        except:
            Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                "Could not talk with SnapManager DS.<br>" + \
                    "Please check if DS is running.")

        if contexts is not None:
            ctxs=sorted(contexts.values(), key=lambda s:s.name.lower())
            for context in ctxs:
                self.contextComboBox.addItem("%s [%d]" % (context.name, context.ID), Qt.QVariant(context.ID))
            #self.contextComboBox.model().sort(0, Qt.Qt.AscendingOrder)
            if sid>=0: self.listWidget.setCurrentRow(sid)
            #self.contextComboBox.blockSignals(False)
        
    def getCurrentSnap(self):
        """
        Return SnapID of the currently selected item in the list
        """
        return get_item_id(self.listWidget.currentItem())
    
    def getCurrentContext(self,idx=None):
        """
        Return ContextID of the currently selected item in the combo box
        """
        if not self.contextComboBox.count():
            return None
        elif idx is None:
            idx = self.contextComboBox.currentIndex()
        return get_as_int(self.contextComboBox.itemData(idx))

    def onFilterComboChanged(self):
        print 'onFilterComboChanged()'
        if self.filterComboBox.currentText() == 'Reason':
            try:
                reasons = list(set(c.reason for c in self.snapapi.get_contexts().values()))        
                self.filterComboBox2.clear()
                self.filterComboBox2.setEditable(False)            
                reasons.sort(key=lambda x: x.lower())
                for r in reasons:
                    self.filterComboBox2.addItem(str(r))                                
            except:
                Qt.QMessageBox.critical(self,"Tango Archiving Problem",
                                        "Please check if SnapManagerDS is running.<br>" + \
                                        "Also check if this context exists.")
                return              
        else:
            self.filterComboBox2.clear()
            self.filterComboBox2.clearEditText()
            self.filterComboBox2.setEditable(True)

    def onRefreshPressed(self):
        fkey = str(self.filterComboBox.currentText())
        fvalue = str(self.filterComboBox2.currentText())
        print 'onRefreshPressed(%s,%s)'%(fkey,fvalue)
        try:
            self.listWidget.blockSignals(True)
            self.listWidget.clear()
            self.listWidget.blockSignals(False)
            all_ctx = self.snapapi.get_contexts().values()
            if fkey!='Attributes':
                ctxs = [c for c in all_ctx if fandango.searchCl(fvalue, str(c.name) if fkey=='Name' else str(c.reason))]
            else:
                ctxs = [c for c in all_ctx if any(fandango.functional.searchCl(fvalue,a) \
                    for a in [c.name,c.reason,c.description]+[av['full_name'] for av in c.attributes.values()])] 
            if not len(ctxs):
                msg = QExceptionMessage(self,'Warning','No context in SnapDB is named like "%s"'%(fvalue))
                ctxs = all_ctx
            self.contextComboBox.blockSignals(True)
            self.contextComboBox.clear()
            current = self.getCurrentContext()
            if current in ctxs:
                ctxs.remove(current)
                ctxs.insert(0,current)
            for c in ctxs:
                self.contextComboBox.addItem("%s [%d]" % (c.name, c.ID), Qt.QVariant(c.ID))
            self.contextComboBox.blockSignals(False)
            if current is not None or len(ctxs)==1: self.onContextChanged()
        except: 
            msg = QExceptionMessage(self,"Error(%s,%s)"%(
                self.filterComboBox.currentText(),self.filterComboBox2.currentText()),traceback.format_exc())

    def refresh(self, cid):
        #self.contextComboBox.blockSignals(True)
        self.initContexts(sid=cid)
        #self.contextComboBox.blockSignals(False)
        pos = self.contextComboBox.findText('['+str(cid)+']', QtCore.Qt.MatchEndsWith)
        self.contextComboBox.setCurrentIndex(pos)
        self.buttonTake.show()

    def onNewPressed(self):
        self.CtxEditForm=ContextEditWidget()
        QtCore.QObject.connect(self.CtxEditForm, QtCore.SIGNAL("NewContextCreated(int)"), self.refresh)
        self.CtxEditForm.show()

    def onEditPressed(self):
        cid=self.getCurrentContext()
        self.CtxEditForm=ContextEditWidget(ctxID=cid)
        QtCore.QObject.connect(self.CtxEditForm, QtCore.SIGNAL("ContextModified(int)"), self.refresh)
        self.CtxEditForm.show()

    def onContextChanged(self, idx=None):
        try:
            cid = self.getCurrentContext(idx)
            print "onContextChanged(%s,(%s,%s,%s))"%(cid,self.filterComboBox.currentText(),
                self.filterComboBox2.currentText(),self.contextComboBox.currentText())
            try:
                self.context=self.snapapi.get_context(cid)
            except:
                msg = QExceptionMessage('\n'.join(("Tango Archiving Problem",
                                        "Could not retrieve context with following ID: %s.<br>" % cid + \
                                        "Please check if SnapManagerDS is running.<br>" + \
                                        "Also check if this context exists.")))
                return
            self.infoLabel1_2.setText("<b>%s</b>" % self.context.author)
            self.infoLabel2_2.setText("<b>%s</b>" % self.context.reason)
            if len(self.context.description)>80:
                self.infoLabel3_2.setText("<b>%s</b>" % (self.context.description[:75]+' ...'))
            else:
                #self.infoLabel3_2.setWordWrap(True)
                self.infoLabel3_2.setText("<b>%s</b>" % (self.context.description))
            self.infoLabel3_2.setToolTip(fandango.str2lines(self.context.description))
            
            if self.defaultContextID(): self.defaultContextLabel.setText("<b>%s</b> [%d]" % (self.context.name, self.context.ID))
            
            self.listWidget.clear()
            print "onContextChanged(%s): get_snapshots()"%(cid)
            self.snapshots=self.context.get_snapshots()
            print '[%d]'%len(self.snapshots)
            for id,snapshot in self.snapshots.items():
                item=Qt.QListWidgetItem()
                item.setText("%s - %s [ID: %d]" % (snapshot[0], snapshot[1].split('\n')[0], id))
                item.setData(Qt.Qt.UserRole, Qt.QVariant(id))
                item.setToolTip(snapshot[1])
                self.listWidget.addItem(item)
            self.listWidget.model().sort(0, Qt.Qt.DescendingOrder)
            self.buttonTake.show()
            self.buttonImport.show()
            self.infoLabel1_1.show()
            self.infoLabel1_2.show()
            self.infoLabel2_1.show()
            self.infoLabel2_2.show()
            self.infoLabel3_1.show()
            self.infoLabel3_2.show()
            self.infoLabel4_1.show()
            self.tableWidget.clear()
            self.tableWidget.setColumnCount(0)
            self.tableWidget.setRowCount(0)
            self.tableWidget.setHorizontalHeaderLabels([""])
            self.comp._wi.tableWidget.clear()
            self.comp._wi.tableWidget.setColumnCount(0)
            self.comp._wi.tableWidget.setRowCount(0)
            self.comp._wi.tableWidget.setHorizontalHeaderLabels([""])
            self.attributes = []
            self.comp._wi.diffComboBox.clear()
            self.tableView()
        except: 
            msg = QExceptionMessage("Error")


    def onSnapshotChanged(self, current=None, previous=None):
        try:
            if current==None: return
            id = get_item_id(current)
            print('onSnapshotChanged(%s)'%id)
            snap = self.snapshots[id]
            attributes = self.snapapi.db.get_snapshot_attributes(id).values()
            self.attributes = [(self.snapapi.db.get_attribute_name(a['id_att']), a['value'] if 'value' in a else (a['read_value'],a['write_value'])) for a in attributes]
            self.tableWidget.clear()
            if not attributes:
                Qt.QMessageBox.warning(self,"Empty snapshot", "This snapshot appears to have no contents.")
            try:
                print "onSnapshotChanged(%s)"%(id)
                self.taurusForm.setModel([])
                self.buildTable(self.attributes)
                self.tableView()
                self.tableLabel.setText('%s: %s'%(snap[0],snap[1])) #.split('\n')[0]))
                self.tableLabel.setToolTip(snap[-1])
                self.tableLabel.show()
                self.buttonLoad.show()
                self.buttonEditSnap.show()
                self.buttonDelSnap.show()
                self.buttonExport.show()
                self.viewComboBox.show()
                self.viewComboBox.setCurrentIndex(0)
                self.buildSnap2Box(id)
            except:
                print traceback.format_exc()
        except: Qt.QMessageBox.warning(self,"Error",traceback.format_exc())

    def buildSnap2Box(self, sid):
        cid=self.getCurrentContext()
        self.comp._wi.diffComboBox.clear()
        try:
            snaps=self.snapapi.db.get_context_snapshots(cid)
        except:
            QtCore.QMessageBox.critical(self,"Tango Archiving Problem", "Could not talk with SnapManager DS.<br>Please check if DS is running.")
        self.comp._wi.diffComboBox.addItem("Actual values")
        for snapshot in snaps:
            if snapshot[0] != sid:
                self.comp._wi.diffComboBox.addItem("%s : \"%s\"" % (snapshot[1], snapshot[2].split('\n')[0]), QtCore.QVariant(snapshot[0]))

    def is_numeric(self, src):
        try:
            i=float(str(src))
        except ValueError:
            return False
        else:
            return True

    def compare2Values(self, sid):
        """
        compare snapshot values against actual values
        """
        try:
            factory=taurus.core.taurusmanager.TaurusManager().getDefaultFactory()()
            #snap1Data=self.snapapi.db.get_snapshot_attributes(sid,['t_sc_num_2val']).values() or self.snapapi.db.get_snapshot_attributes(sid,['t_sc_num_1val']).values()
            attr_names = reversedict(self.snapapi.db.get_attributes_ids())
            snap1Data = self.snapapi.db.get_snapshot_attributes(sid)
            vals,data = [],[]
            for name,K,V in sorted((attr_names[int(i)],i,j) for i,j in snap1Data.items()):
                current_attr=factory.getAttribute(name)
                if ('write_value' in V): #RW
                    rv1=V['read_value']#.values()[0]
                    wv1=V['write_value']#.values()[2]
                    try:
                        rv2=current_attr.getValueObj().value
                        wv2=current_attr.getValueObj().w_value
                    except:
                        print('ERROR: unable to read %s'%name)
                        rv2,wv2 = None,None
                else: #RO
                    rv1=V['value']#.values()[1]
                    wv1=None
                    rv2=current_attr.getValueObj().value
                    wv2=None
                x,y = V.get('dim_x',1),V.get('dim_y',1)
                if (x,y) == (1,1):
                    vals.append((name,rv1,wv1,rv2,wv2))
                else:
                    print x,y
                    print rv1
                    rv1,wv1 = str(rv1).split(','),str(wv1).split(',')
                    if y!=1: rv2,wv2 = [v for w in rv2 for v in w],[v for w in wv2 for v in w]
                    getix = lambda l,ix: (l[ix] if ix<len(l) else None)
                    for j in range(y):
                        for i in range(x):
                            c = j*x+i
                            vals.append(('%s[%s][%s]'%(name,j,i),getix(rv1,c),getix(wv1,c),getix(rv2,c),getix(wv2,c)))
            for name,rv1,wv1,rv2,wv2 in vals:
                if self.is_numeric(rv1) and self.is_numeric(rv2):
                    if self.is_numeric(wv1) and self.is_numeric(wv2):
                        data.append([name, rv1, wv1, rv2, wv2, cast_value(float,rv1)-cast_value(float,rv2), cast_value(float,wv1)-cast_value(float,wv2)])
                    else:
                        data.append([name, rv1, wv1, rv2, wv2, cast_value(float,rv1)-cast_value(float,rv2), None])
                else:
                    if self.is_numeric(wv1) and self.is_numeric(wv2):
                        data.append([name, rv1, wv1, rv2, wv2, None, cast_value(float,wv1)-cast_value(float,wv2)])
                    else:
                        data.append([name, rv1, wv1, rv2, wv2, None, None])
            return(data)
        except: Qt.QMessageBox.warning(self,"Error",traceback.format_exc())

    def build_diff_table(self, data):
        self.comp._wi.tableWidget.clear()
        cols=7
        self.comp._wi.tableWidget.setColumnCount(cols)
        self.comp._wi.tableWidget.setHorizontalHeaderLabels(["Attribute Name", "RV1", "WV1", "RV2", "WV2", "diff1", "diff2"])
        self.comp._wi.tableWidget.setGeometry(QtCore.QRect(20, 190, 500, 400))
        if data:
            rows=len(data)
            self.comp._wi.tableWidget.setRowCount(rows)
            for row in range(0, rows):
                    for col in range(0, cols):
                        item=QtGui.QTableWidgetItem("%s" % data[row][col])
                        if (data[row][col]==None or data[row][col]=='None'):
                            item=QtGui.QTableWidgetItem("%s" %"X")
                            item.setTextColor(QtGui.QColor(255,0,0))
                        if row%2==0:
                            item.setBackgroundColor(QtGui.QColor(225,225,225))
                        if (col==5 or col==6) and (data[row][col]!=0) and (data[row][col]!='None') and (data[row][col]!=None):
                            item.setBackgroundColor(QtGui.QColor(45,150,255))
                        elif (col==1 or col==2):
                            if (data[row][col]>data[row][col+2]):
                                item.setBackgroundColor(QtGui.QColor(255,0,0))
                            elif(data[row][col]<data[row][col+2]):
                                item.setBackgroundColor(QtGui.QColor(255,255,0))
                        elif (col==3 or col==4):
                            if (data[row][col]>data[row][col-2]):
                                item.setBackgroundColor(QtGui.QColor(255,0,0))
                            elif (data[row][col]<data[row][col-2]):
                                item.setBackgroundColor(QtGui.QColor(255,255,0))
                        self.comp._wi.tableWidget.setItem(row,col,item)
        else:
            self.comp._wi.tableWidget.setRowCount(1)
            item=QtGui.QTableWidgetItem("%s" % QtCore.QString('No Data!'))
            self.comp._wi.tableWidget.setItem(0, 0, item)
        self.comp._wi.tableWidget.resizeColumnsToContents()

    def onCompareButtonPressed(self):
        sid1 = get_item_id(self.listWidget.currentItem())
        sid2 = get_as_int(self.comp._wi.diffComboBox.itemData(self.comp._wi.diffComboBox.currentIndex()))
        if self.comp._wi.diffComboBox.currentText()!='Actual values':
            data=self.snapapi.db.get_diff_between_snapshots(sid1, sid2)
        else:
            data=self.compare2Values(sid1)
        self.build_diff_table(data)

    def changeView(self, idx):
        if(idx==0):
            self.tableView()
        elif(idx==1):
            self.liveView()
        else:
            self.compareView()

    def onSavePressed(self):
        try:
            cid=self.getCurrentContext()
            prompt=QtGui.QInputDialog
            comment, ok=prompt.getText(self, 'Input dialog', 'Type a comment to continue:')
            if ok and len(str(comment)) != 0:
                try:
                    ctx=self.snapapi.get_context(cid)
                    assert ctx.take_snapshot(str(comment)),'Snapshot failed!'
                except:
                    fandango.qt.QExceptionMessage(traceback.format_exc())
                    return
                Qt.QMessageBox.information(self,"Snapshot","Snapshot taken succesfully!")
                pos = self.contextComboBox.findText('['+str(cid)+']', QtCore.Qt.MatchEndsWith)
                self.onContextChanged(pos)
            elif ok and len(str(comment)) == 0:
                self.onSavePressed()
        except: Qt.QMessageBox.warning(self,"Error",traceback.format_exc())

    def onLoadPressed(self):
        try:
            print '%s: onLoadPressed()'%time.ctime()
            sid = get_item_id(self.listWidget.currentItem())
            self.loadSnapshot(sid)
        except: Qt.QMessageBox.warning(self,"Error",traceback.format_exc())

    def onClosePressed(self):
        self._Form.close()
        
    def onEditSnapPressed(self):
        try:
            sid = self.getCurrentSnap()
            qd = Qt.QDialog()
            qd.setWindowTitle('Edit Snap Comment')
            print str(self.snapshots[sid][1])
            qtt = Qt.QTextEdit()
            qtt.setPlainText(str(self.snapshots[sid][1]))
            qbb = Qt.QDialogButtonBox(qd)
            qbb.addButton(qbb.Ok),qbb.addButton(qbb.Cancel)
            qbb.connect(qbb,Qt.SIGNAL('accepted()'),qd.accept),qbb.connect(qbb,Qt.SIGNAL('rejected()'),qd.reject)
            qd.setLayout(Qt.QVBoxLayout())
            [qd.layout().addWidget(w) for w in (Qt.QLabel('Insert your new commment for Snap:'),qtt,qbb)]
            if qd.exec_() == qd.Accepted:
                comment = qtt.toPlainText()
                print comment
                try:
                    self.snapapi.modify_snapshot(sid,comment)
                    self.snapshots[sid][1] = comment
                    self.tableLabel.setText('%s: %s'%tuple(self.snapshots[sid]))
                except: fandango.qt.QExceptionMessage(traceback.format_exc())
        except: Qt.QMessageBox.warning(self,"Error",traceback.format_exc())
        
    def onImportPressed(self):
        try:
            print '%s: onImportPressed()'%time.ctime()
            self.loadFromFile(str(Qt.QFileDialog.getOpenFileName(self,'Load CSV File')))
        except: Qt.QMessageBox.warning(self,"Error",traceback.format_exc())   
        
    def onDelCtxPressed(self):
        try:
            cid = self.getCurrentContext()
            ctx = self.snapapi.contexts[cid]
            QMB = Qt.QMessageBox
            qmsg = QMB(QMB.Warning,"Delete Context","Are you sure that you want to delete this context?\n\n%s-%s"%(ctx.reason,ctx.name),
                QMB.Ok|QMB.Cancel)
            if qmsg.exec_()==QMB.Ok:
                try: self.snapapi.db.remove_context(cid)
                except: fandango.qt.QExceptionMessage(traceback.format_exc())
            #self.onContextChanged()
            self.onRefreshPressed()
        except: Qt.QMessageBox.warning(self,"Error",traceback.format_exc())
        
    def onDelSnapPressed(self):
        sid = self.getCurrentSnap()
        QMB = Qt.QMessageBox
        qmsg = QMB(QMB.Warning,"Delete Snapshot","Are you sure that you want to delete this snapshot?\n\n%s - %s"%tuple(self.snapshots[sid]),QMB.Ok|QMB.Cancel)
        if qmsg.exec_()==QMB.Ok:
            try: self.snapapi.db.remove_snapshot(sid)
            except: fandango.qt.QExceptionMessage(traceback.format_exc())
        #self.onContextChanged()
        self.onRefreshPressed()
    
    def onExportPressed(self):
        try:
            import csv, tkFileDialog, Tkinter, datetime
            sid1 = get_item_id(self.listWidget.currentItem())
            sid2 = get_as_int(self.comp._wi.diffComboBox.itemData(self.comp._wi.diffComboBox.currentIndex()))

            if get_as_int(self.viewComboBox.itemData(self.viewComboBox.currentIndex())) == 2:
                # EXPORTING A DIFF VIEW OF STORED vs CURRENT VALUES
                if self.comp._wi.diffComboBox.currentText()!='Actual values':
                    data=self.snapapi.db.get_diff_between_snapshots(sid1, sid2)
                else:
                    data=self.compare2Values(sid1)
                header=['attribute name', 'read value1', 'read value2', 'write value1', 'write value2', 'diff1', 'diff2']
                header.append(['attribute name', 'read value', 'write value', 'diff'])
            elif get_as_int(self.viewComboBox.itemData(self.viewComboBox.currentIndex())) == 0:
                # EXPORTING THE STORED VALUES TO CSV
                snapshot=self.context.get_snapshot(sid1)
                tmpdata=snapshot.items()
		data = []
		for r in tmpdata:
			if len(r[1]) == 2:
				data.append((r[0],r[1][0],r[1][1]))
			else:
				data.append((r[0],r[1][0],None))
                #data = [(r[0],r[1][0],r[1][1]) for r in data]
                header=['attribute name', 'read value', 'write value']

            root=Tkinter.Tk()
            root.withdraw()
            initname=str(self.contextComboBox.currentText().replace(' ', '_')+"."+str(datetime.date.today()))
            initname=initname.replace('(','')
            initname=initname.replace(')','')
            filename=tkFileDialog.asksaveasfilename(filetypes=[('all files', '.*'), ('csv files', '.csv')], initialfile=initname, title='Save CSV as...', parent=root)
            writer=csv.writer(open(filename+".csv", "wb"), delimiter="\t")
            writer.writerow(header)
            writer.writerows(data)
        except: Qt.QMessageBox.warning(self,"Error",traceback.format_exc())
        
    def loadSnapshot(self, sid):
        try:
            print '%s: loadSnapshot(%s)'%(time.ctime(),sid)
            attrs = [attr['full_name'] for attr in self.context.get_attributes().values() if 'WRITE' in str(attr['writable'])]
            snapshot=self.context.get_snapshot(sid)
            from snapdialogs import LoadValuesWidget
            from fandango.qt import QDialogWidget
            self.loadWidget = LoadValuesWidget()
            self.loadWidget.setModel(attrs)

            if self.loadWidget.loadValues(snapshot):
                qd = QDialogWidget(self)
                qd.setWidget(self.loadWidget,reject_signal='canceled')
                qd.setWindowTitle('Load Values from Snapshot (%s)'%sid)
                qd.show()
            else:
                pass #self.loadWidget.close()
        except:
            fandango.qt.QExceptionMessage(traceback.format_exc())
            
    def loadFromFile(self, filename):
        print '%s: loadFromFile(%s)'%(time.ctime(),filename)
        if not str(filename.strip()): return
        try:
            import PyTangoArchiving.files
            table = PyTangoArchiving.files.parse_raw_file(filename)
            attrs = table['attribute name']
	    data = {}
	    for i,a in enumerate(attrs):
		    
		    try:
		    	read_value = table['read value'][i]
		    except:
			read_value = None
		    try:
		    	write_value = table['write value'][i]
		    except:
			write_value = None
		    data[a] = (read_value, write_value)
	    
            #data = dict((a,(table['read value'][i],table['write value'][i])) for i,a in enumerate(attrs))
            from snapdialogs import LoadValuesWidget
            from fandango.qt import QDialogWidget
            self.loadWidget = LoadValuesWidget()
            self.loadWidget.setModel(attrs)

            if self.loadWidget.loadValues(data):
                qd = QDialogWidget(self)
                qd.setWidget(self.loadWidget,reject_signal='canceled')
                qd.setWindowTitle('Load from %s'%filename)
                qd.show()
            else: 
                pass #self.loadWidget.close()
        except:
            fandango.qt.QExceptionMessage(traceback.format_exc())

    def buildTable(self, snap):
        self.tableWidget.clear()
        self.tableWidget.setColumnCount(3)
        self.tableWidget.setRowCount(len(snap))
        self.tableWidget.setHorizontalHeaderLabels(["Attribute Name", "Read Value", "Write Value"])
        i=0
        for value in sorted(snap):
            item=QtGui.QTableWidgetItem("%s" % value[0])
            self.tableWidget.setItem(i, 0, item)
            if (type(value[1]) is not tuple):
                item1=QtGui.QTableWidgetItem("%s" % value[1])
                self.tableWidget.setItem(i, 1, item1)
                item2=QtGui.QTableWidgetItem("%s" %'None')
            else:
                item1=QtGui.QTableWidgetItem("%s" % value[1][0])
                self.tableWidget.setItem(i, 1, item1)
                item2=QtGui.QTableWidgetItem("%s" % value[1][1])
            self.tableWidget.setItem(i, 2, item2)
            if i%2==0:
                item.setBackgroundColor(QtGui.QColor(225,225,225))
                item1.setBackgroundColor(QtGui.QColor(225,225,225))
                item2.setBackgroundColor(QtGui.QColor(225,225,225))
            i=i+1
        self.tableWidget.resizeColumnsToContents()

    def tableView(self):
        self.taurusForm.hide()
        self.taurusForm.setModel([])
        self.comp.hide()
        #self.gridLayout.addWidget(self.tableWidget)
        self.tableWidget.show()

    def liveView(self):
        self.tableWidget.hide()
        self.comp.hide()
        names = sorted(t[0] for t in self.attributes)
        self.taurusForm.setModel(names)
        #self.gridLayout.addWidget(self.taurusForm)
        self.taurusForm.setWithButtons(False)
        self.taurusForm.show()

    def compareView(self):
        self.taurusForm.hide()
        self.taurusForm.setModel([])
        self.tableWidget.hide()
        self.comp._wi.tableWidget.clear()
        #self.gridLayout.addWidget(self.comp)
        self.comp._wi.tableWidget.setRowCount(0)
        self.comp._wi.tableWidget.setColumnCount(0)
        self.comp.show()
        
def SnapsTool(context=None,show=True):
    Form=QtGui.QWidget()
    ui=SnapForm()
    ui.setupUi(Form)
    taurus.changeDefaultPollingPeriod(3000)
    if context:
        print 'Setting Context ...'
        ui.filterComboBox2.setEditText(context)
        print 'Refresh ...'
        ui.onRefreshPressed()
    if show: Form.show()
    return ui

if __name__ == "__main__":
    from taurus.qt.qtgui.application import TaurusApplication
    app=TaurusApplication(sys.argv)
    ui = SnapsTool(sys.argv[-1] if sys.argv[1:] else None)
    sys.exit(app.exec_())
