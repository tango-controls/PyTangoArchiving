import sys, taurus
try:
    from taurus.external.qt import Qt, QtGui, QtCore
except:
    from PyQt4 import Qt, QtGui, QtCore
from taurus.qt.qtgui import container
from taurus.qt.qtgui.panel import TaurusForm
from PyTangoArchiving import SnapAPI
from snaps import *

class snapWidget(QtGui.QWidget):
    def __init__(self,parent=None,container=None):
        QtGui.QWidget.__init__(self,parent)
        self._swi=SnapForm()
        self._swi.setupUi(self,load=False)
        self._kontainer=container

    def initContexts(self,attrlist=[], sid=None):
        self._swi.initContexts(attrlist, sid)

    def show(self):
        QtGui.QWidget.show(self)

class snapToolbar(QtGui.QToolBar):
    def __init__(self,parent=None):
        QtGui.QToolBar.__init__(self,parent)
        self.setIconSize(Qt.QSize(30,30))
        self.SnapApp=snapWidget()
        self.att_table=[]
        self.factory = taurus.Factory()
        self.refresh()    
        self.refreshTimer = QtCore.QTimer()
        QtCore.QObject.connect(self.refreshTimer, QtCore.SIGNAL("timeout()"), self.refresh)
        self.refreshTimer.start(5000)

        snap=self.SnapApp.show
        self.addAction(Qt.QIcon(":/devices/camera-photo.svg"),"Snapshot Widget", snap)
        self.setMovable(True)
        self.setFloatable(True)
        self.setToolTip("Snapshot Toolbar")

    def refresh(self):
        new_table = []
        for a in self.factory.getExistingAttributes():
            new_table.append(str(a).split('/',1)[1])
        new_table = sorted(set(s.lower() for s in new_table))
        if not self.att_table or new_table!=self.att_table:
            self.att_table = new_table
            if self.att_table: self.SnapApp.initContexts(self.att_table)
            else: self.SnapApp.initContexts()

    def setRefreshTime(self, refTime):
        self.refreshTimer.start(refTime*1000)

if __name__ == "__main__":
    qapp=Qt.QApplication([])
    snapapi=SnapAPI()
    context=snapapi.get_context(0)
    tmw=container.TaurusMainWindow()
    tmw.setFixedWidth(280)
    #SnapApp=snapWidget(container=tmw)
    #snap=SnapApp.show
    #tmw.fileMenu.addAction(Qt.QIcon(":/actions/media-record.svg"),"SnapApp",snap)
    #dupa2=tmw.fileMenu.addMenu('Dupa2')
    #dupa2.addAction(Qt.QIcon(":/actions/media-record.svg"),"SnapApp",snap)

    #toolbar=Qt.QToolBar(tmw)
    #tmw.addToolBar(toolbar)
    #toolbar.setIconSize(Qt.QSize(30,30))
    #toolbar.addAction(Qt.QIcon(":/actions/media-record.svg"),"SnapApp",snap)
    #toolbar.setMovable(True)
    #toolbar.setFloatable(True)
    #toolbar.setToolTip("ToolBarrrrrrrr")

    #menubar=tmw.menuBar()
    #dupa=menubar.addMenu('&Dupa')
    #dupa.addAction(Qt.QIcon(":/actions/media-record.svg"),"SnapApp",snap)

    contextAttributes=[attr['full_name'] for attr in context.get_attributes().values()]
    taurusForm=TaurusForm(tmw)
    taurusForm.setModel(contextAttributes)
    tmw.setCentralWidget(taurusForm)

    tmw.statusBar().showMessage('Ready')
    s=tmw.splashScreen()
    s.finish(tmw)
    tmw.show()

    toolbar2=snapToolbar(tmw)
    toolbar2.setRefreshTime(5)
    tmw.addToolBar(toolbar2)

    #widgets=tmw.findChildren(taurus.qt.qtgui.base.TaurusBaseComponent)
    #att_table=[]
    #for w in widgets:
      #if type(w.getModelObj()).__name__ == 'TangoAttribute':
        #if not w.getModelObj().getFullName().split('/',1)[1] in att_table:
          #att_table.append(w.getModelObj().getFullName().split('/',1)[1])
        ##print w.getModelObj().getFullName()
      #if type(w.getModelObj()).__name__ == 'TaurusConfigurationProxy':
        #if not w.getModelObj().getParentObj().getFullName().split('/',1)[1] in att_table:
          #att_table.append(w.getModelObj().getParentObj().getFullName().split('/',1)[1])

    #print att_table
    #if att_table: SnapApp.initContexts(att_table)
    #else: SnapApp.initContexts()
    sys.exit(qapp.exec_())