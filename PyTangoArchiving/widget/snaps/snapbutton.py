#############################################################################
##
## file :       PyTangoArchiving/widget/snaps/snapbutton.py
##
## description : see below
##
## project :     Tango Control System
##
## $Author: Sebastien GARA (NEXEYA)
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

import sys, taurus
try:
    from taurus.external.qt import Qt, QtGui, QtCore
except:
    from PyQt4 import Qt, QtGui, QtCore
from taurus.qt.qtgui import container
from taurus.qt.qtgui.panel import TaurusForm
from PyTangoArchiving import SnapAPI
from snaps import *
import fandango, fandango.qt
from toolbar import snapWidget
import PyTango
import time

class SnapButtonContext(QtGui.QPushButton ):
    def __init__(self,contextid=None,useInputForComment=False,parent=None):
        QtGui.QPushButton .__init__(self,parent)
        self.setIconSize(Qt.QSize(30,30))
        self.setIcon(Qt.QIcon(":/devices/camera-photo.svg"))
        self.snapapi=SnapAPI()
        self.contextid = contextid
        self.comment = "AutoSnap" 
        self.useInputForComment = useInputForComment
        QtCore.QObject.connect(self, QtCore.SIGNAL("clicked()"), self.snap)
        self.setToolTip("Snapshot for contextid " + str(self.contextid))

    def getUserInput(self, val, title):
        input, ok=QtGui.QInputDialog.getText(self, 'Input dialog',
         'Do you want to change the value of ' + title + " ?", QtGui.QLineEdit.Normal, val)
        if ok:
            return str(input)
        else:
            return str(val)

    def snap(self):
        try:
            if self.useInputForComment == True:
                self.comment = self.getUserInput(self.comment, "Comment")
            ctx=self.snapapi.get_context(self.contextid)
            ctx.take_snapshot(self.comment)
            Qt.QMessageBox.information(None,"Snapshot","Done!")
        except Exception:
            fandango.qt.QExceptionMessage()
            return
snapButtonContext = SnapButtonContext

class SnapButton(QtGui.QPushButton):
    def __init__(self,useInputForComment=False,useWizardForContext=False,parent=None):
        QtGui.QPushButton .__init__(self,parent)
        self.setIconSize(Qt.QSize(30,30))
        self.setIcon(Qt.QIcon(":/devices/camera-photo.svg"))
        self.snapapi=SnapAPI()
        self.contextid = None
        self.model="" 
        self.author = self.getUserName()
        self.name = "AutoSnap_" + time.strftime("%Y_%m_%d_%H%M%S")
        self.reason = "Always a good reason to create an AutoSnap" 
        self.description = "Snap for all attributes of " + self.model
        self.comment = "AutoSnap" 
        self.useInputForComment = useInputForComment
        self.useWizardForContext = useWizardForContext
        QtCore.QObject.connect(self, QtCore.SIGNAL("clicked()"), self.snap)
        self.setToolTip("Snapshot for device " + self.model)

    def setModel(self, model):
        self.model = model
        self.name = self.model + "_" + time.strftime("%Y_%m_%d_%H%M%S")
        self.description = "Snap for all attributes of " + self.model
        self.setToolTip("Snapshot for device " + self.model)
        
    def setName(self, name):
        self.name = name
        
    def setAuthor(self, author):
        self.author = author
        
    def setReason(self, reason):
        self.reason = reason
        
    def setDescription(self, description):
        self.description = description
        
    def getUserName(self):
        for name in ('LOGNAME', 'USER', 'LNAME', 'USERNAME'):
            user = os.environ.get(name)
        if user:
            return user
        else:
            return "Unknown User" 

    def getUserInput(self, val, title):
        input, ok=QtGui.QInputDialog.getText(self, 'Input dialog',
         'Do you wan to change the value of ' + title + " ?", QtGui.QLineEdit.Normal, val)
        if ok:
            return str(input)
        else:
            return str(val)

    def snap(self):
        if self.contextid == None:
            try:
                attributes = []
                device = PyTango.DeviceProxy(self.model)
                attr_infos = device.attribute_list_query()
                for attr_info in attr_infos:
                    attributes.append(self.model + '/' + attr_info.name)
                if self.useWizardForContext:
                    self.author = self.getUserInput(self.author, "Author")
                    self.name = self.getUserInput(self.name, "Name")
                    self.reason = self.getUserInput(self.reason, "Reason")
                    self.description = self.getUserInput(self.description, "Description")
                self.snapapi.create_context(self.author,self.name,self.reason,self.description,attributes)
                print "context ok" 
                self.contextid = self.snapapi.contexts.keys()[len(self.snapapi.contexts.items())-1]
            except Exception:
                fandango.qt.QExceptionMessage()
                return
        try:
            if self.useInputForComment == True:
                self.comment = self.getUserInput(self.comment, "Comment")
            ctx=self.snapapi.get_context(self.contextid)
            ctx.take_snapshot(str(self.comment))
        except Exception:
            fandango.qt.QExceptionMessage()
            return

snapButton = SnapButton

if __name__ == "__main__":
    import sys
    qapp=Qt.QApplication([])
    panel = Qt.QWidget()
    layout = Qt.QHBoxLayout()
    panel.setLayout(layout)
    button=snapButtonContext(sys.argv[1:] and sys.argv[1] or 9)
    layout.addWidget(button)
    button2=snapButtonContext(sys.argv[2:] and sys.argv[2] or "BigContext")
    layout.addWidget(button2)
    button=snapButton(False, False)
    button.setModel("sys/tg_test/1")
    button2=snapButton(False, True)
    button2.setModel("sys/tg_test/1")
    button3=snapButton(True, True)
    button3.setModel("sys/tg_test/1")
    button4=snapButton(True, False)
    button4.setModel("sys/tg_test/1")
    layout.addWidget(button)
    layout.addWidget(button2)
    layout.addWidget(button3)
    layout.addWidget(button4)
    panel.show()
    sys.exit(qapp.exec_())