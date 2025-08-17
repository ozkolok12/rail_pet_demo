import pandas as pd
import os
from glob import glob

# Папка с файлами
file_path = 'data/raw_demo/'

# Список колонок для перевода
cols_to_translate = [
    "Категория отправки","Тоннажность","Вид перевозки","Груз","Государство отправления",
    "Станция отправления СНГ","Область отправления","Дорога отправления","Станция отправления РФ",
    "Грузоотправитель","Государство назначения","Область назначения","Дорога назначения",
    "Станция назначения РФ","Станция назначения СНГ","Грузополучатель","Род вагона",
    "Тип вагона","Плательщик","Собственник","Арендатор","Оператор"
]

# Собираем все xlsb файлы в папке
files = glob(os.path.join(file_path, "*.xlsb"))

print(f"Found {len(files)} files")

# Словарь с уникальными значениями
unique_values = {col: set() for col in cols_to_translate}

# Обработка каждого файла
for f in files:
    print(f"Processing {os.path.basename(f)} ...")
    df = pd.read_excel(f, engine="pyxlsb")

    for col in cols_to_translate:
        if col not in df.columns:
            continue
        s = df[col].dropna()
        # всё → строки, чистим пробелы
        vals = {str(v).strip() for v in s if str(v).strip() != ""}
        unique_values[col].update(vals)

# Переводим множества в отсортированные списки
for col in unique_values:
    unique_values[col] = sorted(unique_values[col])

print("Unique dictionary was created")

# В один лист Excel (две колонки: Column / Value)
records = [{"Column": col, "Value": val}
           for col, values in unique_values.items()
           for val in values]

pd.DataFrame(records).to_excel("unique_values.xlsx", index=False)

print("Unique dictionary was saved successfully")