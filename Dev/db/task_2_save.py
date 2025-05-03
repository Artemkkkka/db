import glob
import os
import re

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.types import Integer, Numeric, Date, DateTime, Text

from config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER


DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(bind=engine)

BaseModel = declarative_base()
FOLDER = 'spimex_xls'

frames = []
for pattern in (os.path.join(FOLDER, '*.xls'), os.path.join(FOLDER, '*.xlsx')):
    for path in glob.glob(pattern):
        m = re.search(r'(\d{4}-\d{2}-\d{2})', os.path.basename(path))
        file_date = pd.to_datetime(m.group(1)).date() if m else pd.NaT
        sheets = pd.read_excel(
            path, sheet_name=None,
            header=6, engine='xlrd' if path.lower().endswith('.xls') else None
        )
        for df in sheets.values():
            df.columns = df.columns.str.replace(
                r'\s+', ' ', regex=True
            ).str.strip()
            mapping = {}
            for col in df.columns:
                if 'Код Инструмента' in col:
                    mapping[col] = 'exchange_product_id'
                elif 'Наименование Инструмента' in col:
                    mapping[col] = 'exchange_product_name'
                elif 'Базис поставки' in col:
                    mapping[col] = 'delivery_basis_name'
                elif 'Объем Договоров в единицах измерения' in col:
                    mapping[col] = 'volume'
                elif 'Обьем Договоров' in col or (
                    'Объем Договоров' in col and 'руб' in col.lower()
                ):
                    mapping[col] = 'total'
                elif 'Количество Договоров' in col:
                    mapping[col] = 'count'
            df = df.rename(columns=mapping)

            if 'count' not in df.columns:
                continue
            df['count'] = (
                df['count'].astype(str)
                .str.replace(r'\s', '', regex=True)
                .str.replace(',', '.', regex=False)
            )
            df['count'] = pd.to_numeric(df['count'], errors='coerce').fillna(0)
            df = df[df['count'] > 0]
            if df.empty:
                continue
            want = ['exchange_product_id', 'exchange_product_name',
                    'delivery_basis_name', 'volume', 'total', 'count']
            have = [c for c in want if c in df.columns]
            df2 = df[have].copy()
            df2['oil_id'] = df2['exchange_product_id'].str[:4]
            df2['delivery_basis_id'] = df2['exchange_product_id'].str[4:7]
            df2['delivery_type_id'] = df2['exchange_product_id'].str[-1]
            df2['date'] = file_date

            frames.append(df2)

final_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
    columns=['exchange_product_id', 'exchange_product_name',
             'delivery_basis_name', 'volume', 'total', 'count', 'oil_id',
             'delivery_basis_id', 'delivery_type_id', 'date']
)

now = pd.Timestamp.now(tz='America/Denver').tz_convert(None)
final_df['created_on'] = now
final_df['updated_on'] = now

create_sql = """
CREATE TABLE IF NOT EXISTS spimex_trading_results (
    id                  SERIAL PRIMARY KEY,
    exchange_product_id TEXT      NOT NULL,
    exchange_product_name TEXT,
    oil_id              TEXT,
    delivery_basis_id   TEXT,
    delivery_basis_name TEXT,
    delivery_type_id    TEXT,
    volume              NUMERIC,
    total               NUMERIC,
    count               INTEGER,
    date                DATE,
    created_on          TIMESTAMP WITHOUT TIME ZONE,
    updated_on          TIMESTAMP WITHOUT TIME ZONE
);
"""

with engine.begin() as conn:
    conn.execute(text(create_sql))

final_df.to_sql(
    'spimex_trading_results',
    engine,
    index=False,
    if_exists='append',
    method='multi',
    dtype={
        'exchange_product_id': Text(),
        'exchange_product_name': Text(),
        'oil_id': Text(),
        'delivery_basis_id': Text(),
        'delivery_basis_name': Text(),
        'delivery_type_id': Text(),
        'volume': Numeric(),
        'total': Numeric(),
        'count': Integer(),
        'date': Date(),
        'created_on': DateTime(),
        'updated_on': DateTime(),
    }
)

print("Все данные сохранены в spimex_trading_results")
