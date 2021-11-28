"""
    Author: Eric Whalls
    Simulation Environment for IO Cube Capstone

    Launch Arguments: 
    python ./simulator [configuration file.json]

    Load a parameter file from configurations. 
    See them simulated. 

    TODO: 
    ADD UDP Output streams for webapp connectivity 

"""
import os,sys,time
#
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from multiprocessing import Event
#
from Parameters import Parameters
Parameters = Parameters.instance()

from Configurations import Configuration
from TableContainer import Table
import Utilities
import NetworkDistribute
#

class Interface(QMainWindow) :

    configurations = []

    class tableUpdater(QThread) : 

        def __init__(self, table) :
            QThread.__init__(self)
            self.shutdown = Event()
            #
            self.pars = [] # Parameter Objects
            self.data = [] # Serialized Parameters (table data structure)
            self.table = table 
            #
            self.checkPars() 
            #
            self.start()

        def checkPars(self) :
            parameters = Parameters.get()
            for p in parameters : 
                par = getattr(Parameters, p)
                if hasattr(par, "toList") : 
                    if not par in self.pars : 
                        self.pars.append(par)
            ##
            tmpdata = []
            for p in self.pars : 
                tmpdata.append(p.toList())
            #
            self.data = tmpdata

        def run(self) :
            while not self.shutdown.is_set() : 
                time.sleep(0.1)
                #
                if len(self.pars) > 0 and len(self.data) > 0 : 
                    for i,p in enumerate(self.pars) : 
                        self.data[i][1] = p.value
                
                # update table at 10hz 
                self.table.update(self.data)

    def __init__(self) :
        QMainWindow.__init__(self)
        ##
        self.setWindowTitle("IO Cube Simulator")
        # mdi (multi document interface)
        self.setFixedSize(600, 800)
    
        # menu
        menu = self.menuBar()
        menu_file = QMenu("File", self)
        menu_file_load = QAction("Load Parameters...", self, triggered=self.getConfigurationToLoad) # add triggered function 
        menu_file.addActions([menu_file_load])

        menu_tools = QMenu("Tools", self)
        #menu_udp_out = QAction("Add UDP Stream", self, triggered=None)
        #menu_udp_stop = QAction("Stop UDP Streams", self, triggered=None)
        #menu_file.addActions([menu_udp_out])
        

        # add this for use later with unloading parameters
        self.file_menu = menu_file 
        menu.addMenu(menu_file)
    
        # Table
        self.table = Table(self, ["Parameter", "Value", "Unit", "Description"])
        self.tableView = QTableView(self)
        self.tableView.move(0,20)
        self.tableView.setFixedSize(600, 780)
        self.tableView.setModel(self.table)
        # headers
        table_header = self.tableView.horizontalHeader()
        table_header.setSectionResizeMode(0,QHeaderView.ResizeToContents)
        #table_header.setSectionResizeMode(1,QHeaderView.ResizeToContents)
        table_header.setSectionResizeMode(2,QHeaderView.ResizeToContents)
        table_header.setSectionResizeMode(3,QHeaderView.Stretch)

        # table updater
        self.updater = self.tableUpdater(self.table)
        self.netjob = NetworkDistribute.JobHandler()

        ## Start UI 
        self.show()

        
    def getConfigurationToLoad(self) : 
        self.loadConfiguration(Utilities.chooseConfigFile(self)) 

    def loadConfiguration(self, file_path) :
        print('Loading configuration')
        # getFile returns tuple (file path, file type)
        try : 
            config = Configuration(configuration_file=file_path[0], simulator=True, sim_rate=0.05)
        except Exception as e :
            pass 
        else :
            self.configurations.append(config)
            self.updater.checkPars()
            #### 
            cfg_action = QAction("Unload "+os.path.basename(file_path[0]), self, triggered=config.unLoad) 
            self.file_menu.addActions([cfg_action])

    def closeEvent(self, event) :
        print('Close event')
        #
        self.updater.shutdown.set() 
        self.netjob.stop() 
        time.sleep(1)
        print("Threads should be stopped")
        # accept close event 
        event.accept()


app = QApplication(sys.argv)
app.setStyle('Fusion')
interface = Interface()
sys.exit(app.exec_())
