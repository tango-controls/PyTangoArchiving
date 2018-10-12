# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'test.ui'
#
# Created: Fri Jan 14 12:39:06 2011
#      by: PyQt4 UI code generator 4.4.3
#
# WARNING! All changes made in this file will be lost!

try:
    from taurus.external.qt import Qt, QtCore, QtGui
except:
    from PyQt4 import QtCore, QtGui, Qt
import taurus
from taurus.qt.qtgui.panel import TaurusForm

class Snap_Core_Ui_Form(object):
    def setupUi(self, Form):
        Form.setObjectName("Form")
        Form.setWindowModality(QtCore.Qt.WindowModal)
        Form.resize(600, 355)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.Maximum, QtGui.QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(100)
        sizePolicy.setVerticalStretch(100)
        sizePolicy.setHeightForWidth(Form.sizePolicy().hasHeightForWidth())
        Form.setSizePolicy(sizePolicy)
        Form.setMinimumSize(QtCore.QSize(1000, 500))
        Form.setSizeIncrement(QtCore.QSize(1, 1))
        Form.setBaseSize(QtCore.QSize(200, 200))
        self.mainLayout = QtGui.QHBoxLayout(Form)
        self.mainLayout.setObjectName("mainLayout")
        self.splitter = QtGui.QSplitter()
        self.splitter.setChildrenCollapsible(False)
        self.mainLayout.addWidget(self.splitter)
        
        ###################### Left Panel
        self.verticalLayout_2 = QtGui.QVBoxLayout()
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.frame = QtGui.QFrame(Form)
        self.frame.setFrameShape(QtGui.QFrame.StyledPanel)
        self.frame.setFrameShadow(QtGui.QFrame.Raised)
        self.frame.setObjectName("frame")
        self.verticalLayout_3 = QtGui.QVBoxLayout(self.frame)
        self.verticalLayout_3.setObjectName("verticalLayout_3")

        self.horizontalLayoutFilter = QtGui.QHBoxLayout()
        self.horizontalLayoutFilter.setObjectName("horizontalLayoutFilter")
        self.filterLabel = QtGui.QLabel(self.frame)
        self.filterLabel.setObjectName("filterLabel")
        self.filterLabel.setMaximumSize(QtCore.QSize(50, 30))
        self.horizontalLayoutFilter.addWidget(self.filterLabel)
        self.filterComboBox = QtGui.QComboBox(self.frame)
        self.filterComboBox.setObjectName("filterComboBox")
        self.horizontalLayoutFilter.addWidget(self.filterComboBox)
        self.filterComboBox2 = QtGui.QComboBox(self.frame)
        self.filterComboBox2.setObjectName("filterComboBox2")
        self.horizontalLayoutFilter.addWidget(self.filterComboBox2)		
        self.refreshButton = QtGui.QPushButton(self.frame)
        self.refreshButton.setObjectName("refreshButton")
        self.refreshButton.setMaximumSize(QtCore.QSize(50, 30))
        self.horizontalLayoutFilter.addWidget(self.refreshButton)
        self.verticalLayout_3.addLayout(self.horizontalLayoutFilter)

        self.horizontalLayout_4 = QtGui.QHBoxLayout()
        self.horizontalLayout_4.setObjectName("horizontalLayout_4")
        self.comboLabel = QtGui.QLabel(self.frame)
        self.comboLabel.setObjectName("comboLabel")
        self.comboLabel.setMaximumSize(QtCore.QSize(50, 30))
        self.comboLabel.hide()
        self.horizontalLayout_4.addWidget(self.comboLabel)
        self.contextComboBox = QtGui.QComboBox(self.frame)
        self.contextComboBox.setObjectName("contextComboBox")
        self.horizontalLayout_4.addWidget(self.contextComboBox)
        
        self.ctxbuttonsLayout = QtGui.QHBoxLayout()
        
        self.buttonNew = QtGui.QPushButton(self.frame)
        self.buttonNew.setObjectName("buttonNew")
        self.ctxbuttonsLayout.addWidget(self.buttonNew)
                
        self.buttonEditCtx = QtGui.QPushButton(self.frame)
        self.buttonEditCtx.setObjectName("buttonEditCtx")
        self.ctxbuttonsLayout.addWidget(self.buttonEditCtx)
        
        self.buttonDelCtx = QtGui.QPushButton(self.frame)
        self.buttonDelCtx.setObjectName("buttonDelCtx")
        self.ctxbuttonsLayout.addWidget(self.buttonDelCtx)
        
        self.verticalLayout_3.addLayout(self.horizontalLayout_4)
        self.verticalLayout_3.addLayout(self.ctxbuttonsLayout)

        self.formLayout = QtGui.QFormLayout()
        #self.formLayout.setContentsMargins(-1, 4, -1, -1)
        self.formLayout.setObjectName("formLayout")
        self.infoLabel0_1 = QtGui.QLabel(self.frame)
        self.infoLabel0_1.setObjectName("infoLabel0_1")
        self.formLayout.setWidget(0, QtGui.QFormLayout.LabelRole, self.infoLabel0_1)
        self.infoLabel0_1.hide()
        self.infoLabel1_1 = QtGui.QLabel(self.frame)
        self.infoLabel1_1.setObjectName("Author")
        self.formLayout.setWidget(1, QtGui.QFormLayout.LabelRole, self.infoLabel1_1)
        self.infoLabel1_1.hide()
        self.infoLabel1_2 = QtGui.QLabel(self.frame)
        self.infoLabel1_2.setObjectName("infoLabel1_2")
        self.formLayout.setWidget(1, QtGui.QFormLayout.FieldRole, self.infoLabel1_2)
        self.infoLabel1_2.hide()
        self.infoLabel2_1 = QtGui.QLabel(self.frame)
        self.infoLabel2_1.setObjectName("Reason")
        self.formLayout.setWidget(2, QtGui.QFormLayout.LabelRole, self.infoLabel2_1)
        self.infoLabel2_1.hide()
        self.infoLabel2_2 = QtGui.QLabel(self.frame)
        self.infoLabel2_2.setObjectName("infoLabel2_2")
        self.formLayout.setWidget(2, QtGui.QFormLayout.FieldRole, self.infoLabel2_2)
        self.infoLabel2_2.hide()
        self.infoLabel3_1 = QtGui.QLabel(self.frame)
        self.infoLabel3_1.setObjectName("Description")
        self.formLayout.setWidget(3, QtGui.QFormLayout.LabelRole, self.infoLabel3_1)
        self.infoLabel3_1.hide()
        self.infoLabel3_2 = QtGui.QLabel(self.frame)
        self.infoLabel3_2.setObjectName("infoLabel3_2")
        self.formLayout.setWidget(3, QtGui.QFormLayout.FieldRole, self.infoLabel3_2)
        self.infoLabel3_2.hide()
        self.infoLabel4_1 = QtGui.QLabel(self.frame)
        self.infoLabel4_1.setObjectName("Snapshots")
        self.formLayout.setWidget(4, QtGui.QFormLayout.LabelRole, self.infoLabel4_1)
        self.infoLabel4_1.hide()
        self.verticalLayout_3.addLayout(self.formLayout)
        self.listWidget = QtGui.QListWidget(self.frame)
        self.listWidget.setObjectName("listWidget")
        self.verticalLayout_3.addWidget(self.listWidget)
        self.verticalLayout_2.addWidget(self.frame)
        self.horizontalLayout_2 = QtGui.QHBoxLayout()
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")

        self.buttonTake = QtGui.QPushButton(Form)
        self.buttonTake.setObjectName("buttonTake")
        self.horizontalLayout_2.addWidget(self.buttonTake)
        self.buttonTake.hide()
        
        self.buttonImport = QtGui.QPushButton(Form)
        self.buttonImport.setObjectName("buttonImport")
        self.horizontalLayout_2.addWidget(self.buttonImport)
        self.buttonImport.hide()
        
        self.verticalLayout_2.addLayout(self.horizontalLayout_2)
        #self.mainLayout.addLayout(self.verticalLayout_2)
        
        self.leftWidget = QtGui.QWidget()
        self.leftWidget.setLayout(self.verticalLayout_2)
        self.splitter.addWidget(self.leftWidget)
        
        #################### End of Left Panel

        #################### Right Panel
        self.verticalLayout = QtGui.QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        
        self.horizontalLayout_5 = QtGui.QHBoxLayout()
        self.horizontalLayout_5.setObjectName("horizontalLayout_5")
        
        self.tableLabel = QtGui.QLabel(Form)
        self.tableLabel.setObjectName("tableLabel")
        self.horizontalLayout_5.addWidget(self.tableLabel)
        self.tableLabel.hide()
        
        self.buttonEditSnap = QtGui.QPushButton(Form)
        self.buttonEditSnap.setObjectName("buttonEditSnap")
        self.horizontalLayout_5.addWidget(self.buttonEditSnap)
        self.buttonEditSnap.hide()
        
        self.viewComboBox = QtGui.QComboBox(Form)
        self.viewComboBox.setObjectName("viewComboBox")
        self.horizontalLayout_5.addWidget(self.viewComboBox)
        self.viewComboBox.setLayoutDirection(QtCore.Qt.RightToLeft)
        self.viewComboBox.addItem("Table View")
        self.viewComboBox.addItem("Live View")
        self.viewComboBox.addItem("Compare View")
        self.viewComboBox.setMaximumWidth(115)
        self.viewComboBox.hide()
        
        self.verticalLayout.addLayout(self.horizontalLayout_5)

        self.frame_2 = QtGui.QFrame(Form)
        self.frame_2.setFrameShape(QtGui.QFrame.StyledPanel)
        self.frame_2.setFrameShadow(QtGui.QFrame.Raised)
        self.frame_2.setMinimumWidth(400)
        self.frame_2.setObjectName("frame_2")
        
        self.gridLayout = QtGui.QGridLayout(self.frame_2)
        self.gridLayout.setObjectName("gridLayout")
        self.tableWidget = QtGui.QTableWidget(self.frame_2)
        self.tableWidget.setObjectName("tableWidget")
        self.tableWidget.setColumnCount(0)
        self.tableWidget.setRowCount(0)
        self.gridLayout.addWidget(self.tableWidget)

        self.taurusForm = TaurusForm(self.frame_2)
        self.taurusForm.setObjectName("taurusForm")
        self.taurusForm.hide()
        self.verticalLayout.addWidget(self.frame_2)

        self.horizontalLayout = QtGui.QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        
        #self.customButton4 = QtGui.QPushButton(Form)
        #self.customButton4.setObjectName("customButton4")
        #self.horizontalLayout.addWidget(self.customButton4)
        #self.customButton4.hide()
        
        self.buttonExport = QtGui.QPushButton(Form)
        self.buttonExport.setObjectName("buttonExport")
        self.horizontalLayout.addWidget(self.buttonExport)
        self.buttonExport.hide()
        
        self.buttonLoad = QtGui.QPushButton(Form)
        self.buttonLoad.setObjectName("buttonLoad")
        self.horizontalLayout.addWidget(self.buttonLoad)
        self.buttonLoad.hide()
        
        self.buttonDelSnap = QtGui.QPushButton(Form)
        self.buttonDelSnap.setObjectName("buttonDelSnap")
        self.horizontalLayout.addWidget(self.buttonDelSnap)
        self.buttonDelSnap.hide()
        
        self.buttonClose = QtGui.QPushButton(Form)
        self.buttonClose.setObjectName("buttonClose")
        self.buttonClose.setText(QtGui.QApplication.translate("Form", "Close", None, QtGui.QApplication.UnicodeUTF8))
        self.buttonClose.setToolTip(QtGui.QApplication.translate("Form", "Close Application", None, QtGui.QApplication.UnicodeUTF8))
        icon_close = QtGui.QIcon(":/actions/process-stop.svg")
        self.buttonClose.setIcon(icon_close)
        
        self.horizontalLayout.addWidget(self.buttonClose)
        self.verticalLayout.addLayout(self.horizontalLayout)
        #self.mainLayout.addLayout(self.verticalLayout)
        self.rightWidget = QtGui.QWidget()
        self.rightWidget.setLayout(self.verticalLayout)
        self.splitter.addWidget(self.rightWidget)
        ##################### End of Right Panel

        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        Form.setWindowTitle(QtGui.QApplication.translate("Form", "Form", None, QtGui.QApplication.UnicodeUTF8))
        self.infoLabel0_1.setText(QtGui.QApplication.translate("Form", "Attribute", None, QtGui.QApplication.UnicodeUTF8))


if __name__ == "__main__":
    import sys
    app = QtGui.QApplication(sys.argv)
    Form = QtGui.QWidget()
    ui = Ui_Form()
    ui.setupUi(Form)
    Form.show()
    sys.exit(app.exec_())

