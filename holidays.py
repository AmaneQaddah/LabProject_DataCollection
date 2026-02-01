import time
import csv
import re
import requests
from bs4 import BeautifulSoup

# ================== CONFIG ==================
BASE = "https://www.timeanddate.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
}

START_YEAR = 2018
END_YEAR = 2026

SLEEP_SECONDS = 0.5
MAX_RETRIES = 3

# slugs שלא מייצגים מדינות אמיתיות
SKIP_SLUGS = {"world", "un", "fun"}

# סוגי חגים שנחשבים "רשמיים"
ACCEPT_TYPE_KEYWORDS = [
    "public holiday",
    "national holiday",
    "bank holiday",
    "government holiday",
    "statutory holiday",
    "national/legal holiday",
    "gazetted holiday",
    "legal holiday",
    "federal holiday",
    "official holiday",
    "regular holiday",
    "special non-working holiday",
]


# ================== HELPERS ==================
def get_html(url: str) -> str:
    last_err = None
    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(1.0)
    raise RuntimeError(f"Failed fetching {url}: {last_err}")


def extract_country_slugs() -> list:
    html = get_html(f"{BASE}/holidays/")
    soup = BeautifulSoup(html, "html.parser")

    slugs = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m1 = re.match(r"^/holidays/([a-z0-9\-]+)/?$", href)
        m2 = re.match(r"^/holidays/([a-z0-9\-]+)/\d{4}$", href)
        if m1:
            slugs.add(m1.group(1))
        if m2:
            slugs.add(m2.group(1))

    return sorted(s for s in slugs if s not in SKIP_SLUGS)


def is_accepted_type(t: str) -> bool:
    t = (t or "").lower()
    return any(k in t for k in ACCEPT_TYPE_KEYWORDS)


def find_type_column_index(table):
    thead = table.find("thead")
    if not thead:
        return None

    headers = [
        h.get_text(" ", strip=True).lower()
        for h in thead.find_all(["th", "td"])
    ]

    if "type" not in headers:
        return None

    return headers.index("type") - 1  # מינוס th של תאריך


# ================== SCRAPER ==================
def scrape_country_year(country: str, year: int) -> list:
    url = f"{BASE}/holidays/{country}/{year}"

    try:
        html = get_html(url)
    except Exception as e:
        if "404" in str(e):
            return []
        raise

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="holidays-table")
    if not table:
        return []

    type_idx = find_type_column_index(table)
    tbody = table.find("tbody")
    if not tbody:
        return []

    rows_out = []

    for r in tbody.find_all("tr"):
        name_a = r.find("a")
        date_th = r.find("th")
        tds = r.find_all("td")

        if not name_a or not date_th or len(tds) < 2:
            continue

        holiday_name = name_a.get_text(" ", strip=True)
        date_md = date_th.get_text(" ", strip=True).replace("\xa0", " ")

        holiday_type = None
        if type_idx is not None and type_idx < len(tds):
            holiday_type = tds[type_idx].get_text(" ", strip=True)
        else:
            for td in tds:
                txt = td.get_text(" ", strip=True)
                if "holiday" in txt.lower():
                    holiday_type = txt
                    break

        if not holiday_type or not is_accepted_type(holiday_type):
            continue

        full_date = f"{year} {date_md}"
        rows_out.append([full_date, country, holiday_name, holiday_type])

    return rows_out


# ================== MAIN ==================
def main():
    countries = extract_country_slugs()
    print("Countries:", len(countries))

    out_file = f"holidays_{START_YEAR}_{END_YEAR}.csv"
    total = 0

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "country", "holiday_name", "holiday_type"])

        for i, c in enumerate(countries, start=1):
            for y in range(START_YEAR, END_YEAR + 1):
                rows = scrape_country_year(c, y)
                for r in rows:
                    writer.writerow(r)
                total += len(rows)

                print(f"[{i}/{len(countries)}] {c} {y}: {len(rows)} rows (total={total})")
                time.sleep(SLEEP_SECONDS)

    print("DONE.")
    print("Saved to:", out_file)
    print("TOTAL rows:", total)


if __name__ == "__main__":
    main()
