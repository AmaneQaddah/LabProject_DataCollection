import csv
import requests
from datetime import datetime, date, timedelta
from collections import defaultdict

# =========================
# CONFIG
# =========================
START_YEAR = 2010
END_YEAR = 2026

OUT_CSV = "worldcup_daily_features.csv"

# אם שמות המדינות אצלכם שונים (למשל "USA" במקום "United States"),
# הוסיפי פה מיפוי
COUNTRY_REMAP = {
    # "United States": "USA",
    # "United States of America": "USA",
}

# GitHub raw CSVs from Fjelstul World Cup Database
TOURNAMENTS_URL = "https://raw.githubusercontent.com/jfjelstul/worldcup/master/data-csv/tournaments.csv"
MATCHES_URL     = "https://raw.githubusercontent.com/jfjelstul/worldcup/master/data-csv/matches.csv"


def parse_csv_from_url(url: str):
    headers = {
        "User-Agent": "AmaniQaddah-WorldCupDaily/1.0 (course project)",
        "Accept": "text/csv"
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    text = r.text
    reader = csv.DictReader(text.splitlines())
    return list(reader)


def parse_date(s: str) -> date:
    # Fjelstul datasets typically use YYYY-MM-DD
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def main():
    tournaments = parse_csv_from_url(TOURNAMENTS_URL)
    matches = parse_csv_from_url(MATCHES_URL)

    # Build: tournament_id -> host country name (string)
    # tournaments.csv has fields like:
    # - tournament_id
    # - year
    # - host_country (or similar)
    #
    # Because schemas can vary slightly, we handle a few common column names.
    def get_field(row, options):
        for k in options:
            if k in row and row[k]:
                return row[k]
        return ""

    # Collect relevant tournaments (men's FIFA World Cup)
    # Fjelstul DB includes many tournaments; tournaments.csv includes "tournament_name"
    # We'll filter by name containing "FIFA World Cup" and (men's) years.
    tournament_hosts = {}
    tournament_year = {}

    for t in tournaments:
        name = get_field(t, ["tournament_name", "name", "tournament"])
        year_s = get_field(t, ["year"])
        tid = get_field(t, ["tournament_id", "id"])

        if not tid or not year_s:
            continue

        try:
            y = int(year_s)
        except:
            continue

        if y < START_YEAR or y > END_YEAR:
            continue

        # keep men's FIFA World Cup editions
        if "fifa world cup" not in (name or "").lower():
            continue

        host = get_field(t, ["host_country", "host_countries", "country", "host"])
        # Some editions have multiple hosts; keep as raw string.
        if not host:
            continue

        tournament_hosts[tid] = host
        tournament_year[tid] = y

    if not tournament_hosts:
        raise RuntimeError("No tournaments found. Check tournaments.csv schema/filters.")

    # Build match counts per (host_country, date) and tournament date ranges per host
    match_count = defaultdict(int)
    host_min_date = {}
    host_max_date = {}

    for m in matches:
        tid = m.get("tournament_id", "") or m.get("tournament", "") or ""
        if tid not in tournament_hosts:
            continue

        # match date column name can be "match_date" or "date"
        ds = m.get("match_date") or m.get("date") or ""
        if not ds:
            continue

        d = parse_date(ds)
        y = d.year
        if y < START_YEAR or y > END_YEAR:
            continue

        host_raw = tournament_hosts[tid]
        # Normalize multi-hosts: split by common separators if present
        # We'll record match day for EACH host in multi-host tournaments
        host_list = [h.strip() for h in host_raw.replace("&", ",").replace(" and ", ",").split(",") if h.strip()]

        for host in host_list:
            host_clean = COUNTRY_REMAP.get(host, host)
            match_count[(host_clean, d)] += 1

            if host_clean not in host_min_date or d < host_min_date[host_clean]:
                host_min_date[host_clean] = d
            if host_clean not in host_max_date or d > host_max_date[host_clean]:
                host_max_date[host_clean] = d

    # Emit daily rows per host, within tournament window (min..max match date)
    rows_out = []
    for host in sorted(host_min_date.keys()):
        d0 = host_min_date[host]
        d1 = host_max_date[host]
        for d in daterange(d0, d1):
            rows_out.append({
                "country": host,
                "date": d.isoformat(),
                "year": d.year,
                "month": d.strftime("%B"),
                "is_world_cup_day": 1,
                "is_match_day": 1 if match_count.get((host, d), 0) > 0 else 0,
                "matches_that_day": match_count.get((host, d), 0)
            })

    # Write CSV
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["country","date","year","month","is_world_cup_day","is_match_day","matches_that_day"]
        )
        writer.writeheader()
        writer.writerows(rows_out)

    print("DONE. saved:", OUT_CSV)
    print("Hosts:", len(host_min_date))
    print("Rows:", len(rows_out))
    # quick sanity example
    sample = rows_out[:5]
    print("Sample:", sample)


if __name__ == "__main__":
    main()
