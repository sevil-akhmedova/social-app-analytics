"""Простая локальная проверка сохранённой выгрузки ETL."""

import pandas as pd


# CSV с результатом ETL не публикуется в репозитории.
file_path = input('Укажи путь к CSV-выгрузке: ')
df = pd.read_csv(file_path)

# Один срез за одну дату должен встречаться один раз.
key = ['event_date', 'dimension', 'dimension_value']
duplicates = df.duplicated(subset=key).sum()

print('Строк в выгрузке:', len(df))
print('Дней с данными:', df['event_date'].nunique())
print('Повторяющихся ключей:', duplicates)
