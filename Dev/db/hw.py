import os
import re
import glob
import asyncio
import time
import datetime as dt
import sys
from dotenv import load_dotenv

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
import aiofiles
import pandas as pd
from bs4 import BeautifulSoup

from sqlalchemy import Column, Integer, Numeric, Text, Date, DateTime, insert
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession


load_dotenv()
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

BASE_URL = os.getenv(
    "SPIMEX_BASE_URL",
    "https://spimex.com/markets/oil_products/trades/results/"
)
OUT_DIR = os.getenv("OUT_DIR", "./data")
START_DATE = dt.date.fromisoformat(
    os.getenv("START_DATE", "2025-04-02")
)

ASYNC_DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

if not ASYNC_DATABASE_URL:
    raise RuntimeError(
        "ASYNC_DATABASE_URL is not defined."
    )

Base = declarative_base()


class TradingResult(Base):
    __tablename__ = 'trading_results'
    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_product_id = Column(Text)
    exchange_product_name = Column(Text)
    delivery_basis_name = Column(Text)
    count = Column(Integer)
    volume = Column(Numeric)
    total = Column(Numeric)
    price = Column(Numeric)
    oil_id = Column(Text)
    delivery_basis_id = Column(Text)
    delivery_type_id = Column(Text)
    date = Column(Date)
    created_on = Column(DateTime)
    updated_on = Column(DateTime)


engine = create_async_engine(
    ASYNC_DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"ssl": False},  # отключаем SSL на Windows
)
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def download_file(
        session: aiohttp.ClientSession, url: str, out_path: str
        ):
    async with session.get(url) as resp:
        resp.raise_for_status()
        async with aiofiles.open(out_path, mode="wb") as f:
            await f.write(await resp.read())
    print(f"[OK] {out_path}")


async def fetch_download_links(session: aiohttp.ClientSession):
    os.makedirs(OUT_DIR, exist_ok=True)
    page = 1
    next_url = BASE_URL
    sem = asyncio.Semaphore(5)
    tasks = []

    while next_url:
        print(f"Fetching page {page}: {next_url}")
        async with session.get(next_url) as resp:
            resp.raise_for_status()
            html = await resp.text()
        soup = BeautifulSoup(html, "html.parser")
        links = soup.find_all(
            "a", href=re.compile(r"oil_xls_\d{8}\.xls", re.IGNORECASE)
        )
        if not links:
            break

        for a in links:
            href = a["href"]
            m = re.search(r"oil_xls_(\d{8})", href)
            if not m:
                continue
            file_date = dt.datetime.strptime(m.group(1), "%Y%m%d").date()
            if file_date < START_DATE:
                return
            full_url = aiohttp.client.URL(href, encoded=True).join(BASE_URL)
            fname = f"{file_date.isoformat()}_{os.path.basename(full_url.path)}"
            out_path = os.path.join(OUT_DIR, fname)
            if not os.path.exists(out_path):
                async def sem_task(u, p):
                    async with sem:
                        await download_file(session, u, p)
                tasks.append(asyncio.create_task(sem_task(full_url, out_path)))
            else:
                print(f"[SKIP] {out_path} exists")

        page += 1
        next_url = f"{BASE_URL}?page={page}"

    if tasks:
        await asyncio.gather(*tasks)


def prepare_df(path: str) -> pd.DataFrame:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(path))
    file_date = pd.to_datetime(m.group(1)).date() if m else None
    sheets = pd.read_excel(
        path, sheet_name=None, header=6, engine="xlrd"
    )
    frames = []
    for df in sheets.values():
        df.columns = (
            df.columns
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        mapping = {
            "Код Инструмента": "exchange_product_id",
            "Наименование Инструмента": "exchange_product_name",
            "Базис поставки": "delivery_basis_name",
            "Объем Договоров в единицах измерения": "volume",
            "Обьем Договоров": "total",
            "Объем Договоров": "total",
            "Количество Договоров": "count",
        }
        rename_map = {
            col: new
            for pat, new in mapping.items()
            for col in df.columns
            if pat in col
        }
        df = df.rename(columns=rename_map)
        if 'count' not in df.columns or 'exchange_product_id' not in df.columns:
            continue
        df = df[
            df['exchange_product_id'].astype(str).str.match(r'^[A-Za-z0-9]')
        ]
        if df.empty:
            continue
        df['count'] = (
            pd.to_numeric(
                df['count'].astype(str).str.replace(r"\s+", "", regex=True).str.replace(',', '.', regex=False),
                errors='coerce'
            )
            .fillna(0)
            .astype(int)
        )
        cols = [v for v in rename_map.values() if v in df.columns]
        df = df[cols]
        df['oil_id'] = df['exchange_product_id'].str[:4]
        df['delivery_basis_id'] = df['exchange_product_id'].str[4:7]
        df['delivery_type_id'] = df['exchange_product_id'].str[-1]
        df['date'] = file_date
        now = pd.Timestamp.now(tz='UTC').tz_convert(None)
        df['created_on'] = now
        df['updated_on'] = now
        frames.append(df)
    if frames:
        df = pd.concat(frames, ignore_index=True)
        df = df.loc[:, ~df.columns.duplicated()]
        return df
    return pd.DataFrame()


async def save_to_db():
    files = (
        glob.glob(os.path.join(OUT_DIR, '*.xls')) +
        glob.glob(os.path.join(OUT_DIR, '*.xlsx'))
    )
    async with AsyncSessionLocal() as session:
        async with session.begin():
            for path in files:
                df = await asyncio.to_thread(prepare_df, path)
                if not df.empty:
                    df = df.where(pd.notnull(df), None)
                    await session.execute(
                        insert(TradingResult),
                        df.to_dict(orient='records')
                    )
    print("All data saved")


def sync_run():
    import subprocess, sys
    start = time.perf_counter()
    subprocess.run([sys.executable, 'task_2_pars.py'], check=True)
    subprocess.run([sys.executable, 'task_2_save.py'], check=True)
    print(f"[SYNC] elapsed: {time.perf_counter() - start:.2f} sec")


async def async_run():
    start = time.perf_counter()
    await init_db()
    async with aiohttp.ClientSession(
        headers={"User-Agent": "async-spimex/1.0"}
    ) as session:
        await fetch_download_links(session)
    await save_to_db()
    print(f"[ASYNC] elapsed: {time.perf_counter() - start:.2f} sec")

if __name__ == '__main__':
    import sys
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else 'async'
    if mode == 'sync':
        sync_run()
    else:
        asyncio.run(async_run())