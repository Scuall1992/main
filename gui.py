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
)
from PyQt5.QtCore import QTimer
import json

from lib import run, FOLDER, result, save_rest_df

class CheckWindow(QWidget):
    def __init__(self, case):
        super().__init__()
        self.initUI(case)

    def initUI(self, case):
        self.setWindowTitle(f"Проверка {case}")
        self.setGeometry(100, 100, 400, 300)
        layout = QVBoxLayout()
        self.setLayout(layout)
        layout.addWidget(QPushButton("Кнопка"))


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        
        with open("config.json", "r", encoding="utf-8") as f:
            config = json.loads(f.read())

        self.check_windows = []
        self.resize(1200, 800)
        # имя контрагента, кол-во строк в екселе, сумма до процента, сумма после процента
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Путь к вычислениям", "Количество строк", "Сумма поступлений", "Сумма выплат"])

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

        self.setLayout(layout_h)

    def on_evaluate_cases_clicked(self):
        def eval_row(text, row_num):
            row_count, sum_all, sum_after = run(text)
            if all(k is not None for k in [row_count, sum_all, sum_after]):
                self.table.setItem(row_num, 1, QTableWidgetItem(f"{row_count}"))
                self.table.setItem(row_num, 2, QTableWidgetItem(f"{sum_all}"))
                self.table.setItem(row_num, 3, QTableWidgetItem(f"{sum_after}"))
            else:
                QMessageBox.information(self, 'Информация', 'Файл еще не загружен')
                return 1

        if self.table.selectedItems():
            for item in self.table.selectedItems():
                if eval_row(item.text(), item.row()):
                    break
        else:
            for i in range(self.table.rowCount()):
                if eval_row(self.table.item(i, 0).text(), i):
                    break

        if self.checkbox.isChecked():
            save_rest_df()

    def addRow(self, case):
        rowCount = self.table.rowCount()
        self.table.insertRow(rowCount)
        self.table.setItem(rowCount, 0, QTableWidgetItem(f"{case}"))
        self.table.setItem(rowCount, 1, QTableWidgetItem(f"x"))
        self.table.setItem(rowCount, 2, QTableWidgetItem(f"x"))
        self.table.setItem(rowCount, 3, QTableWidgetItem(f"x"))

app = QApplication([])

window = MainWindow()
window.show()

app.exec()
