import sys

from fandango.qt import Qt
from taurus.qt.qtgui.container import TaurusWidget, TaurusMainWindow
from PyTangoArchiving.widget import ContextToolBar
from PyTangoArchiving.widget.snapdialogs import LoadForm 

BO_DEVICES = ['BO/PC/BEND',                                                                                  

 'BO/PC/QH01',                                                                                  

 'BO/PC/QH02',                                                                                  

 'BO/PC/QV01',                                                                                  

 'BO/PC/QV02',                                                                                  

 'BO/PC/SH',                                                                                    

 'BO/PC/SV',                                                                                    

 'BO01/PC/CORH-01',                                                                             

 'BO01/PC/CORH-02',                                                                             

 'BO01/PC/CORH-03',                                                                             

 'BO01/PC/CORH-04',                                                                             

 'BO01/PC/CORH-05',                                                                             

 'BO01/PC/CORH-06',                                                                             

 'BO01/PC/CORH-07',                                                                             

 'BO01/PC/CORH-08',                                                                             

 'BO01/PC/CORH-09',                                                                             

 'BO01/PC/CORH-10',                                                                             

 'BO01/PC/CORH-11',                                                                             

 'BO01/PC/CORV-01',                                                                             

 'BO01/PC/CORV-02',                                                                             

 'BO01/PC/CORV-03',                                                                             

 'BO01/PC/CORV-04',                                                                             

 'BO01/PC/CORV-05',                                                                             

 'BO01/PC/CORV-06',                                                                             

 'BO01/PC/CORV-07',                                                                             

 'BO01/PC/CORV-09',                                                                             

 'BO01/PC/CORV-11',                                                                             

 'BO02/PC/CORH-01',                                                                             

 'BO02/PC/CORH-02',                                                                             

 'BO02/PC/CORH-03',                                                                             

 'BO02/PC/CORH-04',                                                                             

 'BO02/PC/CORH-05',                                                                             

 'BO02/PC/CORH-06',                                                                             

 'BO02/PC/CORH-07',                                                                             

 'BO02/PC/CORH-08',                                                                             

 'BO02/PC/CORH-09',                                                                             

 'BO02/PC/CORH-10',                                                                             

 'BO02/PC/CORH-11',                                                                             

 'BO02/PC/CORV-01',                                                                             

 'BO02/PC/CORV-02',                                                                             

 'BO02/PC/CORV-03',                                                                             

 'BO02/PC/CORV-04',                                                                             

 'BO02/PC/CORV-05',                                                                             

 'BO02/PC/CORV-06',                                                                             

 'BO02/PC/CORV-07',                                                                             

 'BO02/PC/CORV-09',                                                                             

 'BO02/PC/CORV-11',                                                                             

 'BO03/PC/CORH-01',                                                                             

 'BO03/PC/CORH-02',                                                                             

 'BO03/PC/CORH-03',                                                                             

 'BO03/PC/CORH-04',                                                                             

 'BO03/PC/CORH-05',                                                                             

 'BO03/PC/CORH-06',                                                                             

 'BO03/PC/CORH-07',                                                                             

 'BO03/PC/CORH-08',                                                                             

 'BO03/PC/CORH-09',                                                                             

 'BO03/PC/CORH-10',                                                                             

 'BO03/PC/CORH-11',                                                                             

 'BO03/PC/CORV-01',                                                                             

 'BO03/PC/CORV-02',                                                                             

 'BO03/PC/CORV-03',                                                                             

 'BO03/PC/CORV-04',                                                                             

 'BO03/PC/CORV-05',                                                                             

 'BO03/PC/CORV-06',                                                                             

 'BO03/PC/CORV-07',                                                                             

 'BO03/PC/CORV-09',                                                                             

 'BO03/PC/CORV-11',                                                                             

 'BO04/PC/CORH-01',                                                                             

 'BO04/PC/CORH-02',                                                                             

 'BO04/PC/CORH-03',                                                                             

 'BO04/PC/CORH-04',                                                                             

 'BO04/PC/CORH-05',                                                                             

 'BO04/PC/CORH-06',                                                                             

 'BO04/PC/CORH-07',                                                                             

 'BO04/PC/CORH-08',                                                                             

 'BO04/PC/CORH-09',                                                                             

 'BO04/PC/CORH-10',                                                                             

 'BO04/PC/CORH-11',                                                                             

 'BO04/PC/CORV-01',                                                                             

 'BO04/PC/CORV-02',                                                                             

 'BO04/PC/CORV-03',                                                                             

 'BO04/PC/CORV-04',                                                                             

 'BO04/PC/CORV-05',                                                                             

 'BO04/PC/CORV-06',                                                                             

 'BO04/PC/CORV-07',                                                                             

 'BO04/PC/CORV-09',                                                                             

 'BO04/PC/CORV-11'] 

def create_comparison(device_names):
    lookup = {}
    for idx,d in enumerate(device_names):
        lookup[d.lower()] = idx

    def comparison(a,b):
        a = a.lower()
        b = b.lower()
        adev,sep,aname = a.rpartition('/')
        bdev,sep,bname = b.rpartition('/')
        if adev==bdev:
            return cmp(aname,bname)
        else:
            return cmp(lookup[adev],lookup[bdev])
        return comparison
    
    return comparison

my_cmp = create_comparison(BO_DEVICES)

class Gui(TaurusMainWindow):
    
    def __init__(self, parent=None):
        TaurusMainWindow.__init__(self, parent)
        contextToolBar = ContextToolBar()
        contextToolBar.setDefaultContextID(85)
        contextToolBar.setSorter(cmp)
        self.addToolBar(contextToolBar)
        
class Dummy(TaurusWidget):
    def __init__(self, parent=None):
        TaurusWidget.__init__(self, parent)
        self.setLayout(Qt.QHBoxLayout())
        form = LoadForm(self)
        form.setModel(["simumotor/zreszela/1/position", "simumotor/zreszela/2/position", "simumotor/zreszela/3/position", "simumotor/zreszela/4/position"])
        self.layout().addWidget(form)
         
        

def main():    
    app = Qt.QApplication(sys.argv)
    gui = Gui()
    
    gui.show() 
    app.exec_()

if __name__ == "__main__":
    main()
