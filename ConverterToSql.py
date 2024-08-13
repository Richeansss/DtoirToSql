import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, Float
import os

# Чтение Excel файла
file_path = '/home/alexey/Документы/ДТОиР/Инфа от ПО ЭКС 12.08.2024/МТР подрядчика 2024.xlsx'  # Замените на путь к вашему Excel файлу
sheet_name = '2024'  # Укажите имя листа, если нужно
df = pd.read_excel(file_path, sheet_name=sheet_name)

# Удаление апострофов из всех строковых данных
df = df.apply(lambda col: col.map(lambda x: x.replace("'", "") if isinstance(x, str) else x))

# Определение столбцов с датами и числами
def identify_column_types(df):
    date_columns = []
    numeric_columns = []

    for col in df.columns:
        # Попытка преобразовать в datetime
        try:
            converted_dates = pd.to_datetime(df[col], format='%d.%m.%Y', errors='coerce')
            if not converted_dates.isna().all():  # Проверяем, есть ли успешные преобразования
                date_columns.append(col)
                df[col] = converted_dates.dt.strftime('%Y-%m-%d')  # Преобразуем даты в строку формата 'YYYY-MM-DD'
        except Exception as e:
            print(f"Error processing column {col} for date: {e}")

        # Попытка преобразовать в числовой тип
        try:
            converted_numbers = pd.to_numeric(df[col], errors='coerce')
            if not converted_numbers.isna().all():  # Проверяем, есть ли успешные преобразования
                numeric_columns.append(col)
                df[col] = converted_numbers
        except Exception as e:
            print(f"Error processing column {col} for number: {e}")

    return date_columns, numeric_columns

date_columns, numeric_columns = identify_column_types(df)

# Вывод найденных столбцов
print("Найденные столбцы с датами:")
print(date_columns)

print("\nНайденные столбцы с числами:")
print(numeric_columns)

# Создание SQLAlchemy engine
db_url = 'sqlite:///your_database.db'  # Замените на ваш путь к базе данных или строку подключения
engine = create_engine(db_url)

# Определение типа данных для столбцов SQLAlchemy
def get_sql_types(df, date_columns):
    sql_types = {}
    for col in df.columns:
        if col in date_columns:
            sql_types[col] = String()  # Используем String для столбцов с датами
        elif pd.api.types.is_integer_dtype(df[col]):
            sql_types[col] = Integer()
        elif pd.api.types.is_float_dtype(df[col]):
            sql_types[col] = Float()
        else:
            sql_types[col] = String()
    return sql_types

sql_types = get_sql_types(df, date_columns)

# Извлечение имени файла без расширения и преобразование его в название таблицы
table_name = os.path.splitext(os.path.basename(file_path))[0].replace(" ", "_").lower()

# Создание таблицы с нужными типами данных
metadata = MetaData()
table = Table(
    table_name, metadata,  # Используем table_name, полученное из имени файла
    *[Column(col, sql_types[col]) for col in df.columns]
)

# Создание таблицы в базе данных
metadata.create_all(engine)

# Запись данных в базу данных
df.to_sql(table_name, con=engine, if_exists='replace', index=False)

print(f"Данные успешно сохранены в таблицу '{table_name}'")
