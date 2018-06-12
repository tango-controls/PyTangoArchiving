import taurus, sys, traceback
from taurus.external.qt import Qt, QtCore, QtGui
from PyTangoArchiving import SnapAPI
from PyTangoArchiving.widget.taurusattributechooser import TaurusAttributeChooser
#from taurus.qt.qtgui.panel.taurusmodelchooser import TaurusModelChooser as TaurusAttributeChooser

class ContextEditUi(object):
    def setupUi(self, Form):
        self._Form = Form
        Form.setObjectName("Form")
        Form.resize(550, 700)
        self.verticalLayout_2 = QtGui.QVBoxLayout(Form)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.gridLayout = QtGui.QGridLayout()
        self.gridLayout.setObjectName("gridLayout")
        self.label_name = QtGui.QLabel(Form)
        self.label_name.setObjectName("label_name")
        self.gridLayout.addWidget(self.label_name, 0, 0, 1, 1)
        self.line_name = QtGui.QLineEdit(Form)
        self.line_name.setObjectName("line_name")
        self.gridLayout.addWidget(self.line_name, 0, 1, 1, 1)
        self.label_author = QtGui.QLabel(Form)
        self.label_author.setObjectName("label_author")
        self.gridLayout.addWidget(self.label_author, 1, 0, 1, 1)
        self.line_author = QtGui.QLineEdit(Form)
        self.line_author.setObjectName("line_author")
        self.gridLayout.addWidget(self.line_author, 1, 1, 1, 1)
        self.label_reason = QtGui.QLabel(Form)
        self.label_reason.setObjectName("label_reason")
        self.gridLayout.addWidget(self.label_reason, 2, 0, 1, 1)
        self.line_reason = QtGui.QLineEdit(Form)
        self.line_reason.setObjectName("line_reason")
        self.gridLayout.addWidget(self.line_reason, 2, 1, 1, 1)
        self.label_description = QtGui.QLabel(Form)
        self.label_description.setObjectName("label_description")
        self.gridLayout.addWidget(self.label_description, 3, 0, 1, 1)
        self.line_description = QtGui.QLineEdit(Form)
        self.line_description.setObjectName("line_description")
        self.gridLayout.addWidget(self.line_description, 3, 1, 1, 1)

        self.verticalLayout_2.addLayout(self.gridLayout)
        self.verticalLayout = QtGui.QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")

        self.attch=TaurusAttributeChooser()
        self.wMainVerticalLayout = QtGui.QVBoxLayout()
        self.wMainVerticalLayout.setObjectName("wMainVerticalLayout")
        self.label_4=self.attch.ui.label_4
        self.wMainVerticalLayout.addWidget(self.label_4)
        self.final_List=self.attch.ui.final_List
        self.wMainVerticalLayout.addWidget(self.final_List)

        self.line = QtGui.QFrame(Form)
        self.line.setFrameShape(QtGui.QFrame.HLine)
        self.line.setFrameShadow(QtGui.QFrame.Sunken)
        self.line.setObjectName("line")
        self.wMainVerticalLayout.addWidget(self.line)

        self.att_label=QtGui.QLabel(Form)
        self.wMainVerticalLayout.addWidget(self.att_label)

        self.wHorizontalLayout1 = QtGui.QHBoxLayout()
        self.wHorizontalLayout1.setObjectName("wHorizontalLayout1")
        self.label_1=self.attch.ui.label_1
        self.wHorizontalLayout1.addWidget(self.label_1)
        self.lineEdit=self.attch.ui.lineEdit
        self.wHorizontalLayout1.addWidget(self.lineEdit)
        self.addButton=self.attch.ui.addButton
        icon_add = QtGui.QIcon(":/actions/list-add.svg")
        self.addButton.setIcon(icon_add)
        self.wHorizontalLayout1.addWidget(self.addButton)
        self.removeButton=self.attch.ui.removeButton
        icon_remove = QtGui.QIcon(":/actions/list-remove.svg")
        self.removeButton.setIcon(icon_remove)
        self.wHorizontalLayout1.addWidget(self.removeButton)
        self.cancelButton=self.attch.ui.cancelButton
        self.wHorizontalLayout1.addWidget(self.cancelButton)
        self.wMainVerticalLayout.addLayout(self.wHorizontalLayout1)
        self.wHorizontalLayout2 = QtGui.QHBoxLayout()
        self.wHorizontalLayout2.setObjectName("wHorizontalLayout2")
        self.label_2=self.attch.ui.label_2
        self.wHorizontalLayout2.addWidget(self.label_2)
        self.label_3=self.attch.ui.label_3
        self.wHorizontalLayout2.addWidget(self.label_3)
        self.wMainVerticalLayout.addLayout(self.wHorizontalLayout2)
        self.wHorizontalLayout3 = QtGui.QHBoxLayout()
        self.wHorizontalLayout3.setObjectName("wHorizontalLayout3")
        self.devList=self.attch.ui.devList
        self.wHorizontalLayout3.addWidget(self.devList)
        self.attrList=self.attch.ui.attrList
        self.wHorizontalLayout3.addWidget(self.attrList)
        self.wMainVerticalLayout.addLayout(self.wHorizontalLayout3)
        self.verticalLayout.addLayout(self.wMainVerticalLayout)

        self.verticalLayout_2.addLayout(self.verticalLayout)
        self.gridLayout_2 = QtGui.QGridLayout()
        self.gridLayout_2.setObjectName("gridLayout_2")
        spacerItem = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
        self.gridLayout_2.addItem(spacerItem, 0, 0, 1, 1)
        self.pushButtonCreate =  QtGui.QPushButton(Form)
        self.pushButtonCreate.setObjectName("pushButtonCreate")
        self.gridLayout_2.addWidget(self.pushButtonCreate, 0, 1, 1, 1)
        self.pushButtonCancel =  QtGui.QPushButton(Form)
        self.pushButtonCancel.setObjectName("pushButtonCancel")
        self.gridLayout_2.addWidget(self.pushButtonCancel, 0, 2, 1, 1)
        self.verticalLayout_2.addLayout(self.gridLayout_2)
        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        if Form.ctxID:
            Form.setWindowTitle(QtGui.QApplication.translate("Form", "Modify Context", None, QtGui.QApplication.UnicodeUTF8))
            self.pushButtonCreate.setText(QtGui.QApplication.translate("Form", "Modify", None, QtGui.QApplication.UnicodeUTF8))
            self.pushButtonCreate.setToolTip(QtGui.QApplication.translate("Form", "Modify Context", None, QtGui.QApplication.UnicodeUTF8))
        else:
            Form.setWindowTitle(QtGui.QApplication.translate("Form", "Create Context", None, QtGui.QApplication.UnicodeUTF8))
            self.pushButtonCreate.setText(QtGui.QApplication.translate("Form", "Create", None, QtGui.QApplication.UnicodeUTF8))
            self.pushButtonCreate.setToolTip(QtGui.QApplication.translate("Form", "Create Context", None, QtGui.QApplication.UnicodeUTF8))
        self.pushButtonCancel.setText(QtGui.QApplication.translate("Form", "Cancel", None, QtGui.QApplication.UnicodeUTF8))
        self.pushButtonCancel.setToolTip(QtGui.QApplication.translate("Form", "Cancel", None, QtGui.QApplication.UnicodeUTF8))
        self.label_name.setText(QtGui.QApplication.translate("Form", "Name:", None, QtGui.QApplication.UnicodeUTF8))
        self.label_author.setText(QtGui.QApplication.translate("Form", "Author:", None, QtGui.QApplication.UnicodeUTF8))
        self.label_reason.setText(QtGui.QApplication.translate("Form", "Reason:", None, QtGui.QApplication.UnicodeUTF8))
        self.label_description.setText(QtGui.QApplication.translate("Form", "Description:", None, QtGui.QApplication.UnicodeUTF8))
        self.att_label.setText(QtGui.QApplication.translate("Form", "Add / remove attributes:", None, QtGui.QApplication.UnicodeUTF8))

        QtCore.QObject.connect(self.pushButtonCreate,QtCore.SIGNAL("pressed()"), Form.onCreatePressed)
        QtCore.QObject.connect(self.pushButtonCancel,QtCore.SIGNAL("pressed()"), Form.onCancelPressed)
