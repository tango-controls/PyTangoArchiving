try:
    from taurus.external.qt import Qt
except:
    from PyQt4 import Qt
from snapdialogs import SnapSaver, SnapLoader


class ContextToolBar(Qt.QToolBar):
    
    def __init__(self, user=None, host=None, password=None, name = "ContextToolBar", models=None, parent=None):
        Qt.QToolBar.__init__(self, name, parent)
        self.setObjectName(name)
        if models == None: models = []
        self.setSorter(None)
        self.setModels(models)
        self.setUser(user)
        self.setHost(host)
        self.setPassword(password)
        self.setDefaultContextID(None)
        self.saveSnapAction = self.createAction(text="Save snapshot",
                                          slot=self.onSave,
                                          shortcut="Ctrl+S",
                                          icon=":/actions/media-record.svg", 
                                          tip="") 
        self.loadSnapAction = self.createAction(text="Load snapshot",
                                      slot=self.onLoad,
                                      shortcut="Ctrl+L",
                                      icon=":/actions/edit-redo.svg", 
                                      tip="")
        self.addAction(self.saveSnapAction)
        self.addAction(self.loadSnapAction)
                
    def user(self):
        return self._user
    
    def setUser(self, user):
        self._user = user
        
    def host(self):
        return self._host
    
    def setHost(self, host):
        self._host = host
        
    def password(self):
        return self._password 
    
    def setPassword(self, password):
        self._password = password
        
    def setModels(self, models):
        self._models = models
        
    def getModels(self):
        return self._models
    
    def sorter(self):
        return self._sorter
    
    def setSorter(self, sorter):
        self._sorter = sorter
        
    def setDefaultContextID(self, defaultContextID):
        self.setDefaultLoadContextID(defaultContextID)
        self.setDefaultSaveContextID(defaultContextID)
        
    def defaultLoadContextID(self):
        return self._defaultLoadContextID
    
    def setDefaultLoadContextID(self, defaultLoadContextID):
        self._defaultLoadContextID = defaultLoadContextID
        
    def defaultSaveContextID(self):
        return self._defaultSaveContextID
    
    def setDefaultSaveContextID(self, defaultSaveContextID):
        self._defaultSaveContextID = defaultSaveContextID
    
    def onSave(self):
        if self.user() is None or self.host is None or self.password() is None:
            snapSaver = SnapSaver(parent=self, defaultContextID=self.defaultSaveContextID(), sorter=self.sorter())
            snapSaver.setStartupModels(self.getModels()) 
#            snapSaver.setSorter(self.sorter())
        else:
            credentials = (self.user(), self.host(), self.password())
            snapSaver = SnapSaver(credentials, parent=self, defaultContextID=self.defaultSaveContextID())
            snapSaver.setStartupModels(self.getModels())
#            snapSaver.setSorter(self.sorter())
        snapSaver.setWindowTitle("SnapSaver")
        snapSaver.setWindowIcon(Qt.QIcon(":/actions/media-record.svg"))
        snapSaver.exec_()
    
    def onLoad(self):
        if self.user() is None or self.host is None or self.password() is None:
            snapLoader = SnapLoader(parent=self, defaultContextID=self.defaultLoadContextID(), sorter=self.sorter())
#            snapLoader.setSorter(self.sorter())
        else:
            credentials = (self.user(), self.host(), self.password())
            snapLoader = SnapLoader(credentials, parent=self, defaultContextID=self.defaultLoadContextID(), sorter=self.sorter())
            snapLoader.setSorter(self.sorter())
        snapLoader.setWindowTitle("SnapLoader")
        snapLoader.setWindowIcon(Qt.QIcon(":/actions/edit-redo.svg"))
        snapLoader.exec_()
        
    def createAction(self, text="", 
                             slot=None, 
                             shortcut=None, 
                             icon=None, 
                             tip=None,
                             checkable=False,
                             signal="triggered()"):
        action = Qt.QAction(text, self)
        if icon is not None:
            action.setIcon(Qt.QIcon(icon))
        if shortcut is not None:
            action.setShortcut(shortcut)
        if tip is not None:
            action.setToolTip(tip)
            action.setStatusTip(tip)
        if slot is not None:
            Qt.QObject.connect(action, Qt.SIGNAL(signal), slot)
        if checkable:
            action.setCheckable(True)
        return action