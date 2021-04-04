
from PyQt5 import QtCore, QtGui, QtWidgets
from pyqtgraph import PlotWidget, mkPen
import numpy as np
import pyqtgraph
from flask import Flask
from flask_restful import Api, Resource, reqparse
import threading
from support_code_for_model import *


class Sber:
    def __init__(self):
        df0203_1118 = load_dataset(3, 3, 6, 39).reset_index(drop=True)
        self.X_train = df0203_1118.iloc[0:1, :]

    def get(self):
        if self.X_train.shape[0] > 1:
            result = self.X_train.iloc[0:1, :]
            self.X_train = self.X_train.drop(self.X_train.index[0])
            return result
        return None

    def put(self, data):
        self.X_train = pd.concat([self.X_train, data])


LOCK = threading.Lock()
SB = Sber()


try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s

try:
    _encoding = QtGui.QApplication.UnicodeUTF8

    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)


class Ui_MainWindow(object):
    def setupUi(self, MainWindow, data):
        pyqtgraph.setConfigOption('background', 'w')
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(800, 600)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.graphicsView = PlotWidget(self.centralwidget)
        self.graphicsView.setGeometry(QtCore.QRect(90, 10, 611, 431))
        self.graphicsView.setObjectName("graphicsView")
        self.splitter = QtWidgets.QSplitter(self.centralwidget)
        self.splitter.setGeometry(QtCore.QRect(140, 470, 501, 71))
        self.splitter.setOrientation(QtCore.Qt.Horizontal)
        self.splitter.setObjectName("splitter")
        self.pushButton = QtWidgets.QPushButton(self.splitter)
        self.pushButton.setObjectName("pushButton")
        self.pushButton_2 = QtWidgets.QPushButton(self.splitter)
        self.pushButton_2.setObjectName("pushButton_2")
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 800, 21))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.chkMore = QtGui.QCheckBox(self.centralwidget)
        self.chkMore.setObjectName(_fromUtf8("chkMore"))

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        self.y_data = np.zeros(100)
        self.y_data1 = np.zeros(100)

        self.ml_model = load("first_model.joblib")

        self.pushButton.clicked.connect(self.update)
        self.pushButton_2.clicked.connect(lambda: self.clear())
        self.graphicsView.plotItem.showGrid(True, True, 0.7)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.pushButton.setText(_translate("MainWindow", "Draw"))
        self.pushButton_2.setText(_translate("MainWindow", "Clean"))
        self.chkMore.setText(_translate("MainWindow", "keep updating", None))

    def update(self):
        LOCK.acquire()
        data = SB.get()
        LOCK.release()
        self.y_data = np.concatenate((self.y_data[1:], np.array([0])), axis=None)
        if data is not None:
            self.y_data[70] = data["latency"]
            self.y_data1[:-1] = self.y_data1[1:]
            self.y_data1[-1] = self.ml_model.predict(data)[0]
        else:
            self.y_data[70] = 0
            self.y_data1[:-1] = self.y_data1[1:]
            self.y_data1[-1] = 0

        self.graphicsView.plot(range(len(self.y_data)),self.y_data,pen=mkPen('b', width=2), clear=True)
        self.graphicsView.plot(range(len(self.y_data1)), self.y_data1, pen=mkPen('r', width=2))
        time.sleep(0.01)

        if self.chkMore.isChecked():
            QtCore.QTimer.singleShot(1, self.update) # QUICKLY repeat

    def clear(self):
        self.graphicsView.clear()


class Data(Resource):
    def put(self):
        parser = reqparse.RequestParser()
        parser.add_argument('data')
        params = parser.parse_args()
        print(params)
        LOCK.acquire()
        SB.put(pd.read_json(params['data']))
        LOCK.release()
        return "Ok", 201


def flask_proc():
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(Data, "/sb", "/sb/")
    app.run(debug=False, )


def ui_procc():
    import sys
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow, Sber())
    MainWindow.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    t1 = threading.Thread(target=flask_proc, args=())
    t2 = threading.Thread(target=ui_procc, args=())

    t1.start()
    t2.start()

    t1.join()
    t2.join()