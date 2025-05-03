from urllib.parse import urljoin, urlparse
import re
import datetime as dt
import os
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://spimex.com/markets/oil_products/trades/results/"
OUT_DIR = "spimex_xls"
START_DATE = dt.date(2023, 1, 1)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (parser; +https://github.com/your-repo)"
})

os.makedirs(OUT_DIR, exist_ok=True)


def download_file(url, out_path):
    r = session.get(url)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)
    print(f"[OK] {out_path}")


def main():
    page_number = 1
    next_url = BASE_URL
    done_earlier = False

    while next_url:
        print(f"Страница {page_number}: {next_url}")
        resp = session.get(next_url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        links = soup.find_all("a", href=re.compile(r"xls", re.IGNORECASE))
        if links:
            for a in links:
                href = a["href"]
                full_url = urljoin(BASE_URL, href)

                m = re.search(r"oil_xls_(\d{8})", href)
                if not m:
                    print("Не удалось распознать дату из href:", href)
                    continue
                date = dt.datetime.strptime(m.group(1), "%Y%m%d").date()
                if date < START_DATE:
                    done_earlier = True
                    break

                clean_path = urlparse(href).path
                fname_only = os.path.basename(clean_path)
                fname = f"{date.isoformat()}_{fname_only}"
                out_path = os.path.join(OUT_DIR, fname)

                if not os.path.exists(out_path):
                    try:
                        download_file(full_url, out_path)
                    except Exception as e:
                        print("Ошибка при скачивании", full_url, e)
                else:
                    print(f"[SKIP] {out_path} уже есть")

        if done_earlier:
            print("Дошли до 2023-01-01")
            break

        next_url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{page_number + 1}"
        page_number += 1
        if not links:
            print("Нет ссылок на файлы на текущей странице.")
            break

    print("Готово")


if __name__ == "__main__":
    main()
