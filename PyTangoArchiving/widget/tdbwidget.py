import PyTangoArchiving, traceback, taurus, time, re

from fandango.qt import QtCore, QtGui, Qt, Qwt5
from PyTangoArchiving.widget.taurusattributechooser import TaurusAttributeChooser
from taurus.qt.qtgui.panel import TaurusForm
from taurus.qt.qtgui.resource import getThemeIcon

class attributeChooser(TaurusAttributeChooser):
    def __init__(self):
        TaurusAttributeChooser.__init__(self, parent=None, designMode=False)
        self.tdb = PyTangoArchiving.Reader('tdb')
        self.beingArchived=[]
        self.beingArchived=[a.lower() for a in self.tdb.get_attributes() if self.tdb.is_attribute_archived(a.lower())] #already archived attributes
        self.connect(self.ui.lineEdit, Qt.SIGNAL("textChanged (const QString&)"), self.setNewDevName)
        self.setNewDevName()

    def setAttributes(self):
        """Fill the attributes list"""
        import PyTango
        self.ui.attrList.clear()
        self.dev_name = str(self.ui.devList.currentItem().text())
        try:
            items=[str(a.name) for a in PyTango.DeviceProxy(self.dev_name).attribute_list_query()]
        except Exception,e:
            self.warning('Unable to contact with device %s: %s'%(self.dev_name,str(e)))
            items=[]
        items.sort(key=lambda x:x.lower()) #sort the attributes (case insensitive!)
        
        for i in range(len(items)): 
            fullname=str(self.dev_name+'/'+items[i]).lower()
            item=Qt.QListWidgetItem()
            item.setText(str(items[i]))
            if fullname in self.beingArchived: 
                item.setIcon(getThemeIcon("appointment-new"))
            self.ui.attrList.addItem(item)

    def setNewDevName(self):
        """Fill the devices list"""
        device= str(self.ui.lineEdit.text())
        device += '*'
        try:
            items = list(self.getDb().get_device_exported(device))
        except Exception,e:
            self.warning('Unable to contact with device %s: %s'%(device,str(e)))
            items=[]
        self.ui.devList.clear()
        devs=set([d.rsplit('/',1)[0].lower() for d in self.beingArchived])
        for i in items:
          item=Qt.QListWidgetItem()
          item.setText(str(i))
          if str(i).lower() in devs:
              item.setIcon(getThemeIcon("appointment-new"))
          self.ui.devList.addItem(item)
        self.connect(self.ui.devList, Qt.SIGNAL("itemSelectionChanged ()"), self.setAttributes)

class historyButton(Qt.QPushButton):
    def __init__(self):
        Qt.QPushButton.__init__(self)
        self.setMaximumWidth(40)
        self.setIcon(getThemeIcon("appointment-new"))
        self.setToolTip('show history')
        self.connect(self, Qt.SIGNAL("clicked()"), self.onButtonClicked)

    def setModel(self, model):
        self.model=model

    def getModel(self):
        return self.model

    def onButtonClicked(self):
        self.show_history(self.getModel())

    def show_history(self, attribute):
        TABS=[]
        print 'getting archiving readers ...'
        from PyTangoArchiving import Reader
        hdb = Reader(db='hdb',schema='hdb')
        tdb = Reader(db='tdb',schema='tdb')
        tformat = '%Y-%m-%d %H:%M:%S'
        str2epoch = lambda s: time.mktime(time.strptime(s,tformat))
        epoch2str = lambda f: time.strftime(tformat,time.localtime(f))
        attribute = attribute.lower()

        if attribute in hdb.get_attributes() or attribute in tdb.get_attributes():
            print '%s is being archived' % attribute
            di = Qt.QDialog()
            wi = di #QtGui.QWidget(di)
            wi.setLayout(Qt.QGridLayout())
            begin = Qt.QLineEdit()
            begin.setText(epoch2str(time.time()-3600))
            end = Qt.QLineEdit()
            end.setText(epoch2str(time.time()))
            wi.setWindowTitle('Show Archiving')
            wi.layout().addWidget(Qt.QLabel('Enter Begin and End dates in %s format'%tformat),0,0,1,2)
            wi.layout().addWidget(Qt.QLabel('Begin:'),1,0,1,1)
            wi.layout().addWidget(Qt.QLabel('End:'),2,0,1,1)
            wi.layout().addWidget(begin,1,1,1,1)
            wi.layout().addWidget(end,2,1,1,1)
            buttons = Qt.QDialogButtonBox(Qt.QDialogButtonBox.Ok|Qt.QDialogButtonBox.Cancel)
            wi.connect(buttons,Qt.SIGNAL('accepted()'),wi.accept)
            wi.connect(buttons,Qt.SIGNAL('rejected()'),wi.reject)
            wi.layout().addWidget(buttons,3,0,1,2)

            def check_values():
                di.exec_()
                if di.result():
                    print 'checking result ...'
                    start,stop = str(begin.text()),str(end.text())
                    if not all(re.match('[0-9]+-[0-9]+-[0-9]+ [0-9]+:[0-9]+:[0-9]+',str(s).strip()) for s in (start,stop)):
                        print 'dates are wrong ...'
                        Qt.QMessageBox.warning(None,'Show archiving', 'Dates seem not in %s format'%(tformat), Qt.QMessageBox.Ok)
                        return check_values()
                    else:
                        print 'getting values ...'
                        reader = tdb if str2epoch(start)>(time.time()-5*24*3600.) and attribute in tdb.get_attributes() else hdb
                        print 'using %s reader' % reader.schema
                        values = reader.get_attribute_values(attribute,str2epoch(start),str2epoch(stop))
                        if not len(values) and reader is tdb and attribute in hdb.get_attributes():
                            print 'tdb failed, retrying with hdb'
                            values = hdb.get_attribute_values(attribute,str2epoch(start),str2epoch(stop))
                        print 'drawing table from %d values' % len(values)
                        tab = Qt.QTableWidget()
                        tab.setWindowTitle('%s: %s to %s' % (attribute,start,stop))
                        tab.setRowCount(len(values))
                        tab.setColumnCount(2)
                        tab.setHorizontalHeaderLabels(['TIME','VALUE'])
                        for i,tup in enumerate(values):
                            date,value = tup
                            tab.setItem(i,0,Qt.QTableWidgetItem(epoch2str(date)))
                            tab.setItem(i,1,Qt.QTableWidgetItem(str(value)))
                        tab.show()
                        tab.resizeColumnsToContents()
                        tab.horizontalHeader().setStretchLastSection(True)
                        TABS.append(tab)
                        tab.connect(tab,Qt.SIGNAL('close()'),lambda o=tab: TABS.remove(o))
                        print 'show_history done ...'
                        return tab
                else:
                    print 'dialog closed'
                    return None
            print 'asking for dates ...'
            return check_values()
        else:
            Qt.QMessageBox.warning(None,'Show archiving', 'Attribute %s is not being archived'%attribute, Qt.QMessageBox.Ok) 

class Ui_Form(object):
    def setupUi(self, Form):
        Form.setObjectName("Form")
        self._Form=Form
        self.font=QtGui.QFont()
        self.font.setBold(True)
        self.gridLayout_2 = QtGui.QGridLayout(Form)
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.gridLayout = QtGui.QGridLayout()
        self.gridLayout.setObjectName("gridLayout")

        self.chooseLabel=Qt.QLabel()
        self.chooseLabel.setObjectName("chooseLabel")
        self.chooseLabel.setFont(self.font)
        self.chooseLabel.setAlignment(QtCore.Qt.AlignCenter);
        self.gridLayout.addWidget(self.chooseLabel, 0, 0, 1, 1)

        self.tac=attributeChooser()
        self.tac.setObjectName("taurusAttributeChooser")
        self.gridLayout.addWidget(self.tac, 1, 0, 6, 1)
        
        self.selectedLabel=Qt.QLabel()
        self.selectedLabel.setObjectName("selectedLabel")
        self.selectedLabel.setFont(self.font)
        self.selectedLabel.setAlignment(QtCore.Qt.AlignCenter);
        self.selectedLabel.hide()
        self.gridLayout.addWidget(self.selectedLabel, 0, 1, 1, 6)

        self.tf=TaurusForm()
        self.tf.setWithButtons(False)
        self.tf.setObjectName("taurusForm")
        self.tf.hide()
        self.gridLayout.addWidget(self.tf, 1, 1, 1, 6)        
        self.label=Qt.QLabel()
        self.label.setObjectName("label")
        self.label.setFont(self.font)
        self.label.setAlignment(QtCore.Qt.AlignCenter);
        self.gridLayout.addWidget(self.label, 1, 1, 1, 6)
                
        spacerItem = QtGui.QSpacerItem(1, 1, QtGui.QSizePolicy.Minimum, QtGui.QSizePolicy.Expanding)
        self.gridLayout.addItem(spacerItem, 1, 1, 1, 1)
        
        self.modepLabel = QtGui.QLabel(Form)
        self.modepLabel.setObjectName("modepLabel")
        self.modepLabel.setText('Period [s]:')
        self.gridLayout.addWidget(self.modepLabel, 2, 1, 1, 1)        
        self.modepLineEdit = QtGui.QLineEdit(Form)
        self.modepLineEdit.setObjectName("modepLineEdit")
        self.modepLineEdit.setValidator(Qt.QDoubleValidator(1, 14400, 1, Form))
        self.modepLineEdit.setMaxLength(7)
        self.modepLineEdit.setMaximumWidth(60)
        self.modepLineEdit.setText('60.0')
        self.gridLayout.addWidget(self.modepLineEdit, 2, 2, 1, 1)                
        
        self.modeaLabel = QtGui.QLabel(Form)
        self.modeaLabel.setObjectName("modeaLabel")
        self.modeaLabel.setText('Absolute Period [s]:')
        self.gridLayout.addWidget(self.modeaLabel, 3, 1, 1, 1)
        self.modeaLineEdit = QtGui.QLineEdit(Form)
        self.modeaLineEdit.setObjectName("modeaLineEdit")
        self.modeaLineEdit.setValidator(Qt.QDoubleValidator(1, 14400, 1, Form))
        self.modeaLineEdit.setMaxLength(7)        
        self.modeaLineEdit.setMaximumWidth(60)
        self.modeaLineEdit.setText('15.0')        
        self.gridLayout.addWidget(self.modeaLineEdit, 3, 2, 1, 1)        
        self.modeaLowerLimitLabel = QtGui.QLabel(Form)
        self.modeaLowerLimitLabel.setObjectName("modeaLowerLimitLabel")
        self.modeaLowerLimitLabel.setText('Lower Limit (decr.):')
        self.gridLayout.addWidget(self.modeaLowerLimitLabel, 3, 3, 1, 1)                
        self.modeaLowerLimitLineEdit = QtGui.QLineEdit(Form)
        self.modeaLowerLimitLineEdit.setObjectName("modeaLowerLimitLineEdit")
        self.modeaLowerLimitLineEdit.setValidator(Qt.QDoubleValidator(1, 14400, 2, Form))
        self.modeaLowerLimitLineEdit.setMaxLength(7)        
        self.modeaLowerLimitLineEdit.setMaximumWidth(60)
        self.gridLayout.addWidget(self.modeaLowerLimitLineEdit, 3, 4, 1, 1)                
        self.modeaUpperLimitLabel = QtGui.QLabel(Form)
        self.modeaUpperLimitLabel.setObjectName("modeaUpperLimitLabel")
        self.modeaUpperLimitLabel.setText('Upper Limit (incr.):')
        self.gridLayout.addWidget(self.modeaUpperLimitLabel, 3, 5, 1, 1)                
        self.modeaUpperLimitLineEdit = QtGui.QLineEdit(Form)
        self.modeaUpperLimitLineEdit.setObjectName("modeaUpperLimitLineEdit")
        self.modeaUpperLimitLineEdit.setValidator(Qt.QDoubleValidator(1, 14400, 2, Form))
        self.modeaUpperLimitLineEdit.setMaxLength(7)        
        self.modeaUpperLimitLineEdit.setMaximumWidth(60)
        self.gridLayout.addWidget(self.modeaUpperLimitLineEdit, 3, 6, 1, 1)        
        
        self.moderLabel = QtGui.QLabel(Form)
        self.moderLabel.setObjectName("moderLabel")
        self.moderLabel.setText('Relative Period [s]:')
        self.gridLayout.addWidget(self.moderLabel, 4, 1, 1, 1)                        
        self.moderLineEdit = QtGui.QLineEdit(Form)
        self.moderLineEdit.setObjectName("moderLineEdit")
        self.moderLineEdit.setValidator(Qt.QDoubleValidator(1, 14400, 1, Form))
        self.moderLineEdit.setMaxLength(7)
        self.moderLineEdit.setMaximumWidth(60)
        self.moderLineEdit.setText('15.0')        
        self.gridLayout.addWidget(self.moderLineEdit, 4, 2, 1, 1)        
        self.moderLowerLimitPercentLabel = QtGui.QLabel(Form)
        self.moderLowerLimitPercentLabel.setObjectName("moderLowerLimitPercentLabel")
        self.moderLowerLimitPercentLabel.setText('Lower % Limit (decr.):')
        self.gridLayout.addWidget(self.moderLowerLimitPercentLabel, 4, 3, 1, 1)                        
        self.moderLowerLimitPercentLineEdit = QtGui.QLineEdit(Form)
        self.moderLowerLimitPercentLineEdit.setObjectName("moderLowerLimitPercentLineEdit")
        self.moderLowerLimitPercentLineEdit.setValidator(QtGui.QDoubleValidator(0, 1, 2, Form))
        self.moderLowerLimitPercentLineEdit.setMaxLength(7)
        self.moderLowerLimitPercentLineEdit.setMaximumWidth(60)
        self.gridLayout.addWidget(self.moderLowerLimitPercentLineEdit, 4, 4, 1, 1)     
        self.moderUpperLimitPercentLabel = QtGui.QLabel(Form)
        self.moderUpperLimitPercentLabel.setObjectName("moderUpperLimitPercentLabel")
        self.moderUpperLimitPercentLabel.setText('Upper % Limit (incr.):')
        self.gridLayout.addWidget(self.moderUpperLimitPercentLabel, 4, 5, 1, 1)                        
        self.moderUpperLimitPercentLineEdit = QtGui.QLineEdit(Form)
        self.moderUpperLimitPercentLineEdit.setObjectName("moderUpperLimitPercentLineEdit")
        self.moderUpperLimitPercentLineEdit.setValidator(Qt.QDoubleValidator(0, 1, 2, Form))
        self.moderUpperLimitPercentLineEdit.setMaxLength(7)
        self.moderUpperLimitPercentLineEdit.setMaximumWidth(60)
        self.gridLayout.addWidget(self.moderUpperLimitPercentLineEdit, 4, 6, 1, 1)             

        self.gridButtonLayout = QtGui.QGridLayout()
        self.gridButtonLayout.setObjectName("gridButtonLayout")               
        spacerItem = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
        self.gridButtonLayout.addItem(spacerItem, 0, 1, 1, 1)
        self.pushButtonStart = QtGui.QPushButton(Form)
        self.pushButtonStart.setObjectName("pushButtonStart")
        self.pushButtonStart.setEnabled(False)
        self.gridButtonLayout.addWidget(self.pushButtonStart, 0, 2, 1, 1)
        self.pushButtonStop = QtGui.QPushButton(Form)
        self.pushButtonStop.setObjectName("pushButtonStop")
        self.pushButtonStop.setEnabled(False)
        self.gridButtonLayout.addWidget(self.pushButtonStop, 0, 3, 1, 1)
        self.pushButtonCancel = QtGui.QPushButton(Form)
        self.pushButtonCancel.setObjectName("pushButtonCancel")
        self.gridButtonLayout.addWidget(self.pushButtonCancel, 0, 4, 1, 1)
        self.gridLayout.addLayout(self.gridButtonLayout, 5, 1, 1, 6)
        self.gridLayout_2.addLayout(self.gridLayout, 1, 0, 1, 1)
        
        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        Form.setWindowTitle(QtGui.QApplication.translate("Form", "Archiving Widget", None, QtGui.QApplication.UnicodeUTF8))
        self.pushButtonStart.setText(QtGui.QApplication.translate("Form", "Start Archiving", None, QtGui.QApplication.UnicodeUTF8))
        self.pushButtonStop.setText(QtGui.QApplication.translate("Form", "Stop", None, QtGui.QApplication.UnicodeUTF8))
        self.pushButtonCancel.setText(QtGui.QApplication.translate("Form", "Close", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("Form", "Nothing to display", None, QtGui.QApplication.UnicodeUTF8))
        self.selectedLabel.setText(QtGui.QApplication.translate("Form", "Press start to start archiving selected attributes:", None, QtGui.QApplication.UnicodeUTF8))
        self.chooseLabel.setText(QtGui.QApplication.translate("Form", "Choose attributes to archive and hit apply to move attributes to selection", None, QtGui.QApplication.UnicodeUTF8))
        self.tf.connect(self.tac, Qt.SIGNAL("UpdateAttrs"), self.onUpdate)
        QtCore.QObject.connect(self.pushButtonStart, Qt.SIGNAL("clicked()"), self.onStart)
        QtCore.QObject.connect(self.pushButtonStop, Qt.SIGNAL("clicked()"), self.onStop)
        QtCore.QObject.connect(self.pushButtonCancel, Qt.SIGNAL("clicked()"), self.onCancel)
        QtCore.QObject.connect(self.modepLineEdit, Qt.SIGNAL('textChanged(const QString &)'), self.modepTextChanged)
        QtCore.QObject.connect(self.modeaLineEdit, Qt.SIGNAL('textChanged(const QString &)'), self.modeaTextChanged)
        QtCore.QObject.connect(self.moderLineEdit, Qt.SIGNAL('textChanged(const QString &)'), self.moderTextChanged)
        #QtCore.QObject.connect(self.moderUpperLimitPercentLineEdit, Qt.SIGNAL('editingFinished()'), self.moderUpperLimitPercentTextChanged)        
        #QtCore.QObject.connect(self.moderLowerLimitPercentLineEdit, Qt.SIGNAL('editingFinished()'), self.moderLowerLimitPercentTextChanged)                
        #QtCore.QObject.connect(self.modeaUpperLimitLineEdit, Qt.SIGNAL('editingFinished()'), self.modeaUpperLimitTextChanged)        
        #QtCore.QObject.connect(self.modeaLowerLimitLineEdit, Qt.SIGNAL('editingFinished()'), self.modeaLowerLimitTextChanged)                

    def modepTextChanged(self):
        if self.modepLineEdit.text():
            if (float(self.modepLineEdit.text()) < 1 or float(self.modepLineEdit.text()) > 14400):
                Qt.QMessageBox.critical(self._Form,'Error','Value ranges between 1 and 14400', QtGui.QMessageBox.AcceptRole, QtGui.QMessageBox.AcceptRole)
                if float(self.modepLineEdit.text()) > 14400: self.modepLineEdit.setText('14400') 
                else: self.modepLineEdit.setText('1') 
        else:
            Qt.QMessageBox.critical(self._Form,'Error','Value ranges between 1 and 14400', QtGui.QMessageBox.AcceptRole, QtGui.QMessageBox.AcceptRole)
            self.modepLineEdit.setText('1')
              
    def modeaTextChanged(self):
        if self.modeaLineEdit.text():
            if (float(self.modeaLineEdit.text()) > float(self.modepLineEdit.text())):
                Qt.QMessageBox.critical(self._Form,'Error','Value cannot be higher than '+self.modepLineEdit.text(), QtGui.QMessageBox.AcceptRole, QtGui.QMessageBox.AcceptRole)
                self.modeaLineEdit.setText('0') 

    def moderTextChanged(self):
        if self.moderLineEdit.text():
            if (float(self.moderLineEdit.text()) > float(self.modepLineEdit.text())):
                Qt.QMessageBox.critical(self._Form,'Error','Value cannot be higher than '+self.modepLineEdit.text(), QtGui.QMessageBox.AcceptRole, QtGui.QMessageBox.AcceptRole)
                self.moderLineEdit.setText('0') 
       
    def validate(self, modep, modea, moder):
        if (modep < modea or modep < moder):
            Qt.QMessageBox.critical(self._Form,'Error','Period value has to be higher than the others', QtGui.QMessageBox.AcceptRole, QtGui.QMessageBox.AcceptRole)
            self.modepLineEdit.setText(str(10*max(int(self.moderLineEdit.text()), int(self.modeaLineEdit.text())))) 
            return False
        return True

    def floatValidation(self, line):
        val = line.text()
        try:
            float(val)
            return True
        except:
            Qt.QMessageBox.critical(self._Form,'Error in '+str(line.objectName()),'Wrong value !\nSetting value to 0!', QtGui.QMessageBox.AcceptRole, QtGui.QMessageBox.AcceptRole)
            line.setText('0') 
            return False        

    def onStart(self):

        self.floatValidation(self.moderUpperLimitPercentLineEdit)
        self.floatValidation(self.moderLowerLimitPercentLineEdit)
        self.floatValidation(self.modeaUpperLimitLineEdit)
        self.floatValidation(self.modeaLowerLimitLineEdit)
        try:
            self.modep=int(1000*self.modepLineEdit.text())
            self.modea=(int(1000*self.modeaLineEdit.text()) if self.modeaLineEdit.text() else None)
            self.modeaLowerLimit=(float(self.modeaLowerLimitLineEdit.text()) if self.modeaLowerLimitLineEdit.text() else None)
            self.modeaUpperLimit=(float(self.modeaUpperLimitLineEdit.text()) if self.modeaUpperLimitLineEdit.text() else None)
            self.moder=(int(1000*self.moderLineEdit.text()) if self.moderLineEdit.text() else None)
            self.moderLowerLimit=(float(self.moderLowerLimitPercentLineEdit.text()) if self.moderLowerLimitPercentLineEdit.text() else None)
            self.moderUpperLimit=(float(self.moderUpperLimitPercentLineEdit.text()) if self.moderUpperLimitPercentLineEdit.text() else None)
            if self.validate(self.modep, self.modea, self.moder):
                reply=Qt.QMessageBox.question(self._Form,"Warning","Do you want to start archiving selected attributes?", QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.Yes)
                if reply == QtGui.QMessageBox.Yes:
                    command={'MODE_P':[self.modep]}                    
                    if (self.modea and self.modeaUpperLimit and self.modeaLowerLimit): command['MODE_A']=[self.modea, self.modeaUpperLimit, self.modeaLowerLimit]
                    if (self.moder and self.moderUpperLimit and self.moderLowerLimit): command['MODE_R']=[self.moder, self.moderUpperLimit, self.moderLowerLimit]
                    attrs=[a for a in self.tf.getModel()]                    
                    cmd=self.tac.tdb.check_modes('tdb', command)
                    toStop=[a for a in attrs if a in self.tac.beingArchived]
                    try:
                        if toStop: self.tac.tdb.api.start_archiving(attrs, cmd, kill=True)
                        else: self.tac.tdb.api.start_archiving(attrs, cmd)
                    except:
                        Qt.QMessageBox.critical(self._Form,"Error",'Cannot start archiving process.\nCheck the state of archiving managers.', QtGui.QMessageBox.AcceptRole, QtGui.QMessageBox.AcceptRole)
                        print(traceback.format_exc())

                    self.tac.beingArchived=[a.lower() for a in self.tac.tdb if self.tac.tdb.is_attribute_archived(a.lower())] #update archived list
                    self.onUpdate(attrs)
                    self.tac.setNewDevName()
        except:
            print(traceback.format_exc())

    def onStop(self):
        toStop=[att.lower() for att in self.tf.getModel() if att.lower() in self.tac.beingArchived]
        reply=Qt.QMessageBox.question(self._Form,"Warning","Do you want to stop archiving "+str(len(toStop))+" attributes?", QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.Yes)
        if reply == QtGui.QMessageBox.Yes:
            self.tac.tdb.api.stop_archiving(toStop)
            self.tac.beingArchived=[a.lower() for a in self.tac.tdb if self.tac.tdb.is_attribute_archived(a.lower())] #update archived list
            self.pushButtonStop.setEnabled(False)
            self.pushButtonStop.setText('Stop')
            self.tac.setNewDevName()
            self.clearParams()

    def onUpdate(self, attrs):
        self.clearParams()
        if not attrs:
            self.tf.hide()
            self.label.show()
            self.selectedLabel.hide()
            self.pushButtonStart.setEnabled(False)
            self.pushButtonStop.setEnabled(False)
            self.pushButtonStop.setText('Stop')
        else: 
            self.label.hide()
            self.selectedLabel.show()
            self.tf.show()
            self.tf.setModel(attrs)
            
            for i in range(len(attrs)):
                item=self.tf.getItemByIndex(i)
                if item.getModel().lower() in self.tac.beingArchived:
                    item.setExtraWidgetClass(historyButton)
                    historyButton().setModel(item.getModel())

            self.pushButtonStart.setEnabled(True)
            toStop=[att.lower() for att in attrs if att.lower() in self.tac.beingArchived]
            if toStop: 
                self.pushButtonStop.setEnabled(True)
                self.pushButtonStop.setText('Stop ('+str(len(toStop))+')')
                modes=self.tac.tdb.get(toStop[0]).modes
                if 'MODE_P' in modes.keys():
                    self.modepLineEdit.setText(str(int(modes['MODE_P'][0]/1000)))
                if 'MODE_R' in modes.keys():
                    if len(modes['MODE_R']) == 3: 
                        self.moderLineEdit.setText(str(int(modes['MODE_R'][0]/1000)))
                        self.moderLowerLimitPercentLineEdit.setText(str(float(modes['MODE_R'][1])))
                        self.moderUpperLimitPercentLineEdit.setText(str(float(modes['MODE_R'][2])))
                if 'MODE_A' in modes.keys(): 
                    if len(modes['MODE_A']) == 3: 
                        self.modeaLineEdit.setText(str(int(modes['MODE_A'][0]/1000)))
                        self.modeaLowerLimitLineEdit.setText(str(int(modes['MODE_A'][1])))
                        self.modeaUpperLimitLineEdit.setText(str(int(modes['MODE_A'][2])))
            else:
                self.pushButtonStop.setEnabled(False)
                self.pushButtonStop.setText('Stop')

    def clearParams(self):
        self.modepLineEdit.setText('60')
        self.moderLineEdit.setText('')
        self.moderLowerLimitPercentLineEdit.setText('')
        self.moderUpperLimitPercentLineEdit.setText('')
        self.modeaLineEdit.setText('')
        self.modeaLowerLimitLineEdit.setText('')
        self.modeaUpperLimitLineEdit.setText('')

    def onCancel(self):
        self._Form.close()

if __name__ == "__main__":
    import sys
    app = QtGui.QApplication(sys.argv)
    Form = QtGui.QWidget()
    ui = Ui_Form()
    ui.setupUi(Form)
    Form.show()
    sys.exit(app.exec_())
