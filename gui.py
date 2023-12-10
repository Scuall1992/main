from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QListWidget, QPushButton, QCheckBox

from main import run

class CheckWindow(QWidget):
    def __init__(self, case):
        super().__init__()
        self.initUI(case)

    def initUI(self, case):
        self.setWindowTitle(f"Проверка {case}")
        self.setGeometry(100, 100, 200, 100)
        layout = QVBoxLayout()
        self.setLayout(layout)
        layout.addWidget(QPushButton("Кнопка"))


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.check_windows = []
        self.resize(1000, 800)
        # Создание списка
        self.list_widget_flat = QListWidget()

        self.list_widget_flat.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        # Добавление элементов в список
        
        from pathlib import Path

        folder_path = Path("cases")

        all_files = [file for file in folder_path.glob("*")]

        for file in all_files:
            if file.is_file() and file.suffix == ".txt":
                self.list_widget_flat.addItem(str(file))
        
        for file in all_files:
            if file.is_dir():
                self.list_widget_flat.addItem(str(file))

        self.evaluate_cases = QPushButton("Рассчитать")
        self.evaluate_cases.clicked.connect(self.on_evaluate_cases_clicked)
        
        self.check_cases = QPushButton("Проверить рассчеты")
        self.check_cases.clicked.connect(self.on_check_cases_clicked)
        
        checkbox = QCheckBox("Создать файл с необработанными строками")

        layout_v = QVBoxLayout()
        layout_v.addWidget(self.evaluate_cases)
        layout_v.addWidget(self.check_cases)
        layout_v.addWidget(checkbox)
        
        layout_h = QHBoxLayout()
        layout_h.addWidget(self.list_widget_flat)
        layout_h.addLayout(layout_v)

        self.setLayout(layout_h)

    def on_evaluate_cases_clicked(self):
        selected_items = self.list_widget_flat.selectedItems()
        selected_texts = [item.text() for item in selected_items]
        print("Выбранные элементы:", selected_texts)

    def on_check_cases_clicked(self):
        selected_items = self.list_widget_flat.selectedItems()
        selected_texts = [item.text() for item in selected_items]
        for text in selected_texts:
            c = CheckWindow(text)
            c.show()
            self.check_windows.append(c)



# Создание приложения
app = QApplication([])

# Создание и отображение окна
window = MainWindow()
window.show()

# Запуск приложения
app.exec()
