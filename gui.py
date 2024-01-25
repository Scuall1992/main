from PyQt5.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QPushButton,
    QCheckBox,
    QMessageBox,
    QTextEdit,
)
from PyQt5.QtCore import QTimer
import json

from lib import run, FOLDER, result, save_rest_df

from PyQt5.QtCore import QThread, pyqtSignal

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


class SaveRest(QThread):
    finished = pyqtSignal()

    def __init__(self):
        super().__init__()

    def run(self):
        save_rest_df()
        self.finished.emit()
        
    def stop(self):
        self.quit()  # Останавливает цикл событий потока
        self.wait()  # Дожидается завершения потока



class RunCalc(QThread):
    finished = pyqtSignal(int, str, str)

    def __init__(self, text, row_num):
        super().__init__()
        self.text = text
        self.row_num = row_num

    def run(self):
        row_count, sum_all = run(self.text)
        if all(k is not None for k in [row_count, sum_all]):
            self.finished.emit(self.row_num, str(row_count), str(sum_all))
        else:
            self.finished.emit(self.row_num, None, None)
            
    def stop(self):
        self.quit()  # Останавливает цикл событий потока
        self.wait()  # Дожидается завершения потока


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        
        with open("config.json", "r", encoding="utf-8") as f:
            config = json.loads(f.read())

        self.workers = []
        self.save_rest = []
        self.fileLoaded = False
        

        self.logViewer = QTextEdit(self)
        self.logViewer.setReadOnly(True)
        self.logViewer.append(f"Загрузка файла {config['filename']}.  Пожалуйста подождите")

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.checkDownload)
        self.timer.start(1000)

        self.check_windows = []
        self.resize(1200, 800)
        # имя контрагента, кол-во строк в екселе, сумма до процента, сумма после процента
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Путь к вычислениям", "Количество строк", "Сумма итоговых выплат"])

        from pathlib import Path

        folder_path = Path(FOLDER)

        all_files = [file for file in folder_path.glob("*")]

        for file in all_files:
            if file.is_file() and file.suffix == ".txt":
                self.addRow(str(file))

        for file in all_files:
            if file.is_dir():
                self.addRow(str(file))

        self.table.resizeColumnsToContents()

        self.evaluate_cases = QPushButton("Рассчитать")
        self.evaluate_cases.clicked.connect(self.on_evaluate_cases_clicked)

        self.checkbox = QCheckBox("Создать файл с необработанными строками")

        layout_v = QVBoxLayout()
        layout_v.addWidget(self.evaluate_cases)
        layout_v.addWidget(self.checkbox)

        layout_h = QHBoxLayout()
        layout_h.addWidget(self.table)
        layout_h.addLayout(layout_v)

        main_layout = QVBoxLayout()

        main_layout.addLayout(layout_h)
        main_layout.addWidget(self.logViewer)

        self.setLayout(main_layout)

        self.showMaximized()

    def on_evaluate_cases_clicked(self):
        if not self.fileLoaded:
            QMessageBox.information(self, 'Ошибка', 'Файл еще не загружен')
            return
        
        self.active_workers = 0
        if self.table.selectedItems():
            for item in self.table.selectedItems():
                worker = RunCalc(item.text(), int(item.row()))
                self.logViewer.append(f"Начинаем формировать отчет {self.table.item(int(item.row()), 0).text()}")
                worker.finished.connect(self.update_table)
                worker.finished.connect(self.check_all_workers_finished)
                self.workers.append(worker)
                worker.start()
                
        else:
            for i in range(self.table.rowCount()):
                worker = RunCalc(self.table.item(i, 0).text(), i)
                self.logViewer.append(f"Начинаем формировать отчет {self.table.item(i, 0).text()}")
                worker.finished.connect(self.update_table)
                worker.finished.connect(self.check_all_workers_finished)
                self.workers.append(worker)
                worker.start()

    def update_table(self, row_num, row_count, sum_all):
        if row_count == "":
            return

        self.logViewer.append(f"Данные посчитаны {self.table.item(row_num, 0).text()}")

        self.table.setItem(row_num, 1, QTableWidgetItem(row_count))
        self.table.setItem(row_num, 2, QTableWidgetItem(sum_all))

    def check_all_workers_finished(self, row_num, row_count, sum_all):
        self.workers = [worker for worker in self.workers if worker.isRunning()]
        if len(self.workers) == 0:
            if row_count == "":
                return
            QMessageBox.information(self, 'Завершение', 'Все задачи завершены')
            if self.checkbox.isChecked():
                self.logViewer.append(f"Сохранение необработанных строк")
                
                save = SaveRest()
                save.finished.connect(self.save_finish)
                self.save_rest.append(save)
                save.start()
    
    def save_finish(self):
        self.save_rest = []
        self.logViewer.append(f"Необработанных строки сохранены")



    def addRow(self, case):
        rowCount = self.table.rowCount()
        self.table.insertRow(rowCount)
        self.table.setItem(rowCount, 0, QTableWidgetItem(f"{case}"))
        self.table.setItem(rowCount, 1, QTableWidgetItem(f"x"))
        self.table.setItem(rowCount, 2, QTableWidgetItem(f"x"))
        self.table.setItem(rowCount, 3, QTableWidgetItem(f"x"))

    def checkDownload(self):
        row_count, _, _ = run("")
        if row_count == 0:
            self.logViewer.append(f"Файл загружен")
            self.fileLoaded = True
            self.timer.stop()
            
    def stop_all_threads(self):
        for thread in self.workers:
            if thread.isRunning():
                thread.stop()
        if self.save_rest:
            if self.save_rest[0].isRunning():
                self.save_rest[0].stop()

app = QApplication([])

window = MainWindow()
window.show()

app.aboutToQuit.connect(window.stop_all_threads)

app.exec()
