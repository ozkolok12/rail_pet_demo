import pandas as pd
import os
from glob import glob

# Путь к файлам и словарю
data_path = "data/raw_demo/"
dict_path = "unique_values.xlsx"

# Чтение словаря
df_dict = pd.read_excel(dict_path)

# Преобразуем в словарь словарей
translation_dict = {}
for _, row in df_dict.iterrows():
    col = row["Column"]
    original = str(row["Value"]).strip()
    translated = str(row["Translation"]).strip()
    if col not in translation_dict:
        translation_dict[col] = {}
    translation_dict[col][original] = translated

# Переводимые колонки
cols_to_translate = list(translation_dict.keys())

# Обработка всех файлов .xlsb
files = glob(os.path.join(data_path, "*.xlsb"))

for file in files:
    print(f"Processing {os.path.basename(file)}")
    df = pd.read_excel(file, engine="pyxlsb")

    # Применим переводы
    for col in cols_to_translate:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .map(translation_dict[col])
                .fillna(df[col])  # если нет перевода — оставить оригинал
            )

    # Сохраняем в новый Excel-файл
    base = os.path.splitext(os.path.basename(file))[0]
    df.to_excel(f"{base}_translated.xlsx", index=False)

print("✅ Перевод завершён.")