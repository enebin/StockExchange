import sys
from PyQt5.QtWidgets import *
from PyQt5 import uic

form_class = uic.loadUiType("login2.ui")[0]


class WindowClass(QDialog, form_class):
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.temp = 0
        self.buttonBox.accepted.connect(self.pushButtonClicked)
        self.radioButton.clicked.connect(self.radioButtonClicked)

    def pushButtonClicked(self):
        print(self.lineEdit_2.text())

    def radioButtonClicked(self):
        self.temp = not self.temp
        print(self.temp)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    myWindow = WindowClass()
    myWindow.show()
    app.exec_()
