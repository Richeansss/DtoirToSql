import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Numeric, Date, MetaData, Table, Text
import os
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Чтение Excel файла
file_path = 'C:\\Users\\Alexey\\Desktop\\ДТОиР\\Инфа от ПО ЭКС 12.08.2024\\Анализ мероприятий ДТОиР КС.xlsx'
sheet_name = 'Отфильтрован'

# Проверка наличия файла
if not os.path.exists(file_path):
    logger.error(f"Файл {file_path} не найден.")
    raise FileNotFoundError(f"Файл {file_path} не найден.")

# Предварительное чтение первых строк для нахождения строки заголовков
try:
    preview_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=10)
except Exception as e:
    logger.error(f"Ошибка при чтении файла {file_path}: {e}")
    raise

# Функция для поиска строки заголовков
def find_header_row(df):
    for i, row in df.iterrows():
        if row.dropna().apply(lambda x: isinstance(x, str) and x.strip() != '').count() > 2:
            return i
    return None

header_row_index = find_header_row(preview_df)

if header_row_index is None:
    logger.error("Не удалось найти строку с заголовками.")
    raise ValueError("Не удалось найти строку с заголовками.")

logger.info(f"Заголовки найдены на строке {header_row_index + 1}.")

# Чтение файла снова с использованием найденных заголовков
try:
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row_index)
except Exception as e:
    logger.error(f"Ошибка при чтении файла {file_path} с заголовками: {e}")
    raise

# Удаление ненужных столбцов и очистка данных
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df = df.dropna(axis=1, how='all')
df = df.apply(lambda col: col.map(lambda x: x.replace("'", "") if isinstance(x, str) else x))

# Определение типов столбцов и обработка дат
def identify_column_types(df):
    date_columns = []
    numeric_columns = []
    for col in df.columns:
        try:
            converted_dates = pd.to_datetime(df[col], format='%d.%m.%Y', errors='coerce')
            if not converted_dates.isna().all():
                date_columns.append(col)
                # Преобразуем даты в строки в формате гггг-мм-дд
                df[col] = converted_dates.dt.strftime('%Y-%m-%d').where(converted_dates.notna(), None)
        except Exception as e:
            logger.warning(f"Ошибка при обработке столбца {col} как даты: {e}")

        try:
            converted_numbers = pd.to_numeric(df[col], errors='coerce')
            if not converted_numbers.isna().all():
                numeric_columns.append(col)
                df[col] = converted_numbers
        except Exception as e:
            logger.warning(f"Ошибка при обработке столбца {col} как числа: {e}")

    return date_columns, numeric_columns

date_columns, numeric_columns = identify_column_types(df)

logger.info(f"Найденные столбцы с датами: {date_columns}")
logger.info(f"Найденные столбцы с числами: {numeric_columns}")

# Создание SQLAlchemy engine для подключения к PostgreSQL
db_url = 'postgresql://postgres:root@localhost:5432/dtoir'  # Замените на ваши параметры подключения
engine = create_engine(db_url)

with engine.connect() as connection:
    metadata = MetaData()
    table_name = os.path.splitext(os.path.basename(file_path))[0].replace(" ", "_").lower()

    # Определение типов столбцов с учетом PostgreSQL
    sql_types = {
        col: Date() if col in date_columns else
        Numeric() if pd.api.types.is_numeric_dtype(df[col]) else
        Text() for col in df.columns
    }

    table = Table(
        table_name, metadata,
        *[Column(col, sql_types[col]) for col in df.columns]
    )
    metadata.create_all(connection)

    # Вставка данных
    try:
        df.to_sql(table_name, con=connection, if_exists='replace', index=False, dtype=sql_types)
        logger.info(f"Данные успешно сохранены в таблицу '{table_name}'")
        connection.commit()  # Явный коммит изменений
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в таблицу '{table_name}': {e}")
        raise
