import os,sys
#
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *

def chooseConfigFile(parent_widget) :
    return QFileDialog.getOpenFileName(parent_widget, 'Open Configuration', os.path.dirname(__file__)+'/configurations/',"JSON File (*.json)")
