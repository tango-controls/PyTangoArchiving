# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'diff.ui'
#
# Created: Thu Jan 27 14:26:56 2011
#      by: PyQt4 UI code generator 4.4.3
#
# WARNING! All changes made in this file will be lost!

try:
    from taurus.external.qt import Qt, QtCore, QtGui
except:
    from PyQt4 import QtCore, QtGui

class Diff_Ui_Form(object):
    def diffSetupUi(self, Form):
        Form.setObjectName("Form")
        self.diffGridLayout = QtGui.QGridLayout(Form)
        self.diffGridLayout.setObjectName("diffGridLayout")
        self.diffSnapLabel = QtGui.QLabel(Form)
        self.diffSnapLabel.setObjectName("diffSnapLabel")
        self.diffGridLayout.addWidget(self.diffSnapLabel, 0, 0, 1, 1)

        self.diffComboBox = QtGui.QComboBox(Form)
        self.diffComboBox.setObjectName("diffComboBox")
        self.diffComboBox.setToolTip(QtGui.QApplication.translate("Form", "Choose Second Snapshot", None, QtGui.QApplication.UnicodeUTF8))
        self.diffGridLayout.addWidget(self.diffComboBox, 0, 1, 1, 6)

        self.diffButtonCompare = QtGui.QPushButton(Form)
        self.diffButtonCompare.setObjectName("diffButtonCompare")
        self.diffGridLayout.addWidget(self.diffButtonCompare, 0, 7, 1, 1)

        self.tableWidget = QtGui.QTableWidget(Form)
        self.tableWidget.setObjectName("tableWidget")
        self.tableWidget.setColumnCount(0)
        self.tableWidget.setRowCount(0)
        self.diffGridLayout.addWidget(self.tableWidget, 2, 0, 1, 8)

        self.lowerHorizontalLayout = QtGui.QHBoxLayout()
        self.lowerHorizontalLayout.setObjectName("lowerHorizontalLayout")
        self.minLabel = QtGui.QLabel(Form)
        self.minLabel.setObjectName("minLabel")
        self.minLabel.setMaximumSize(QtCore.QSize(40, 30))
        self.lowerHorizontalLayout.addWidget(self.minLabel)
        self.minLogo = QtGui.QLabel(Form)
        self.minLogo.setObjectName("minLogo")
        self.lowerHorizontalLayout.addWidget(self.minLogo)
        self.maxLabel = QtGui.QLabel(Form)
        self.maxLabel.setObjectName("maxnLabel")
        self.maxLabel.setMaximumSize(QtCore.QSize(40, 30))
        self.lowerHorizontalLayout.addWidget(self.maxLabel)
        self.maxLogo = QtGui.QLabel(Form)
        self.maxLogo.setObjectName("maxLogo")
        self.lowerHorizontalLayout.addWidget(self.maxLogo)
        self.diffLabel = QtGui.QLabel(Form)
        self.diffLabel.setObjectName("diffLabel")
        self.diffLabel.setMaximumSize(QtCore.QSize(40, 30))
        self.lowerHorizontalLayout.addWidget(self.diffLabel)
        self.diffLogo = QtGui.QLabel(Form)
        self.diffLogo.setObjectName("diffLogo")
        self.lowerHorizontalLayout.addWidget(self.diffLogo)
        self.diffGridLayout.addLayout(self.lowerHorizontalLayout, 3, 0, 1, 8)

        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        Form.setWindowTitle(QtGui.QApplication.translate("Form", "Form", None, QtGui.QApplication.UnicodeUTF8))
        self.diffSnapLabel.setText(QtGui.QApplication.translate("Form", "Snapshot 2:", None, QtGui.QApplication.UnicodeUTF8))
        self.diffButtonCompare.setText(QtGui.QApplication.translate("Form", "Compare", None, QtGui.QApplication.UnicodeUTF8))
        self.diffButtonCompare.setToolTip(QtGui.QApplication.translate("Form", "Compare Snapshots", None, QtGui.QApplication.UnicodeUTF8))
        icon_compare = QtGui.QIcon(":/actions/view-refresh.svg")
        self.diffButtonCompare.setIcon(icon_compare)
        self.minLabel.setText(QtGui.QApplication.translate("Form", "min:", None, QtGui.QApplication.UnicodeUTF8))
        self.maxLabel.setText(QtGui.QApplication.translate("Form", "max:", None, QtGui.QApplication.UnicodeUTF8))
        self.diffLabel.setText(QtGui.QApplication.translate("Form", "diff:", None, QtGui.QApplication.UnicodeUTF8))
        #self.minLogo.setPixmap(QtGui.QPixmap("img/min.gif"))
        #self.maxLogo.setPixmap(QtGui.QPixmap("img/max.gif"))
        #self.diffLogo.setPixmap(QtGui.QPixmap("img/diff.gif"))
        self.minLogo.setStyleSheet("QLabel { background-color: rgb(255,255,0) }")
        self.maxLogo.setStyleSheet("QLabel { background-color: rgb(255,0,0) }")
        self.diffLogo.setStyleSheet("QLabel { background-color: rgb(45,150,255) }")

if __name__ == "__main__":
    import sys
    app = QtGui.QApplication(sys.argv)
    Form = QtGui.QWidget()
    ui = Diff_Ui_Form()
    ui.diffSetupUi(Form)
    Form.show()
    sys.exit(app.exec_())