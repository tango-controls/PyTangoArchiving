import traceback,time,os,re
import fandango as fun,PyTango as pt,PyTangoArchiving as pta
from fandango.qt import Qt

class ReaderProgress(Qt.QWidget):
    
    def setModel(self,reader):
        self.reader = reader
        
    def setInterval(self,reader):
        pass
