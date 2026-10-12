"""
Scrape The Current's (89.3 FM, Minnesota Public Radio) full playlist history.

Each day's playlist lives at https://www.thecurrent.org/playlist/the-current/YYYY-MM-DD
and the whole day is embedded in the page's Next.js payload, so one request = one day.

Usage (terminal, recommended for the full ~12 hour run):
    caffeinate -i python scraper.py                       # everything, 2005-12-22 → yesterday
    python scraper.py --start 2010-01-01 --end 2010-12-31  # one year
    python scraper.py --retry-failed                      # re-try dates that failed
    python scraper.py --combine-only                      # just rebuild data/plays.parquet
    python scraper.py --update                            # last saved date → yesterday (monthly GitHub Action)

The scraper is resumable: days already saved in data/raw/ are skipped, so you can
Ctrl-C at any time and re-run the same command later.
"""

import argparse
import csv
import json
import logging
import random
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

try:
    # Use the macOS keychain for SSL certificates. Needed on networks that inspect
    # HTTPS traffic (e.g. Zscaler), where Python's bundled certificates fail.
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
BASE_URL = "https://www.thecurrent.org/playlist/the-current/{date}"
START_DATE = date(2005, 12, 22)  # first day of broadcasting
DELAY_SECONDS = 5.0  # pause between requests (plus up to JITTER_SECONDS random)
JITTER_SECONDS = 1.0
TIMEOUT_SECONDS = 30
MAX_RETRIES = 4
BACKOFF_SECONDS = [30, 60, 120, 240]  # wait before retry 1, 2, 3, 4
MAX_CONSECUTIVE_FAILURES = 3  # circuit breaker: stop if the site seems to be blocking us
USER_AGENT = (
    "the-current-playlist-research/1.0 "
    "(Star Tribune data project; contact: ellie.lin@startribune.com)"
)

DATA_DIR = Path(__file__).resolve().parent / "data"
RAW_DIR = DATA_DIR / "raw"
FAILED_FILE = DATA_DIR / "failed_dates.txt"
LOG_FILE = DATA_DIR / "scrape.log"
YEARLY_DIR = DATA_DIR / "plays"  # data/plays/YYYY.parquet, committed to git

# Column order for the output. The first five are the core fields; the rest are
# every other per-play field the page provides (useful for later enrichment,
# e.g. album_mbid → MusicBrainz release year/genre, spotify_id → Spotify API).
COLUMNS = [
    "played_at",
    "title",
    "artist",
    "album",
    "art_url",
    "play_id",
    "song_id",
    "duration",
    "album_mbid",
    "spotify_id",
    "artist_ulid",
    "album_ulid",
    "song_type",
    "additional_text",
    "service_id",
]

PUSH_RE = re.compile(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)')

log = logging.getLogger("scraper")


class ParseError(Exception):
    """Page loaded but the playlist data couldn't be found (site layout may have changed)."""


class BlockedError(Exception):
    """The server returned 403 — we may be blocked. Stop and wait."""


# ----------------------------------------------------------------------------
# Setup helpers
# ----------------------------------------------------------------------------
def setup_logging():
    if log.handlers:
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(fmt)
    log.addHandler(fh)
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.WARNING)  # keep the console quiet; tqdm shows progress
    sh.setFormatter(fmt)
    log.addHandler(sh)


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html"})
    return s


def to_date(d) -> date:
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()


def day_path(d: date) -> Path:
    return RAW_DIR / f"{d.year}" / f"{d.isoformat()}.csv"


# ----------------------------------------------------------------------------
# Fetch + parse
# ----------------------------------------------------------------------------
def fetch_html(d: date, session: requests.Session) -> str | None:
    """Download one day's playlist page. Returns None if the day doesn't exist (404)."""
    url = BASE_URL.format(date=d.isoformat())
    for attempt in range(MAX_RETRIES + 1):
        wait = None
        try:
            r = session.get(url, timeout=TIMEOUT_SECONDS)
        except requests.exceptions.SSLError:
            raise  # certificate problems won't fix themselves by retrying
        except (requests.Timeout, requests.ConnectionError) as e:
            reason = f"{type(e).__name__}"
        else:
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return None
            if r.status_code == 403:
                raise BlockedError(f"403 Forbidden for {url}")
            if r.status_code != 429 and r.status_code < 500:
                r.raise_for_status()
            reason = f"HTTP {r.status_code}"
            retry_after = r.headers.get("Retry-After", "")
            if retry_after.isdigit():
                wait = int(retry_after)

        if attempt == MAX_RETRIES:
            raise RuntimeError(f"{d}: gave up after {MAX_RETRIES} retries ({reason})")
        sleep_for = wait if wait is not None else BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
        log.warning("%s: %s, retrying in %ss (attempt %d/%d)", d, reason, sleep_for, attempt + 1, MAX_RETRIES)
        time.sleep(sleep_for)


def parse_plays(html: str) -> list[dict]:
    """Pull the list of plays out of the Next.js payload embedded in the page."""
    chunks = [json.loads(f'"{m}"') for m in PUSH_RE.findall(html)]
    payload = "".join(chunks)
    i = payload.find('"plays":[')
    if i == -1:
        raise ParseError("no 'plays' array found in page payload")
    plays, _ = json.JSONDecoder().raw_decode(payload, i + len('"plays":'))

    rows = []
    for p in plays:
        song = p.get("song") or {}
        row = {
            "played_at": p.get("played_at"),
            "title": song.get("title"),
            "artist": song.get("artist"),
            "album": song.get("album"),
            "art_url": song.get("art_url"),
            "play_id": p.get("play_id"),
            "song_id": song.get("song_id"),
            "duration": song.get("duration"),
            "album_mbid": song.get("album_mbid"),
            "spotify_id": song.get("spotify_id"),
            "artist_ulid": song.get("artist_ulid"),
            "album_ulid": song.get("album_ulid"),
            "song_type": song.get("song_type"),
            "additional_text": p.get("additional_text"),
            "service_id": p.get("service_id"),
        }
        rows.append(row)
    return rows


# ----------------------------------------------------------------------------
# Save
# ----------------------------------------------------------------------------
def write_day(d: date, rows: list[dict]):
    """Write atomically (tmp file → rename) so an interrupted run never leaves half a file."""
    path = day_path(d)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)


def scrape_day(d, session: requests.Session | None = None) -> int:
    """Fetch, parse and save one day. Returns the number of plays saved."""
    d = to_date(d)
    session = session or make_session()
    html = fetch_html(d, session)
    rows = [] if html is None else parse_plays(html)
    write_day(d, rows)  # empty days still get a header-only file so they aren't refetched
    log.info("%s: %d plays", d, len(rows))
    return len(rows)


def record_failure(d: date, err: Exception):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(FAILED_FILE, "a") as f:
        f.write(f"{d.isoformat()}\t{type(err).__name__}: {err}\n")


# ----------------------------------------------------------------------------
# Main loops
# ----------------------------------------------------------------------------
def date_list(start, end, newest_first=False) -> list[date]:
    start, end = to_date(start), to_date(end)
    days = [start + timedelta(n) for n in range((end - start).days + 1)]
    return days[::-1] if newest_first else days


def scrape_dates(days: list[date], delay: float = DELAY_SECONDS) -> dict:
    """Scrape a list of dates, skipping ones already saved. Returns a summary dict."""
    from tqdm.auto import tqdm

    setup_logging()
    session = make_session()
    todo = [d for d in days if not day_path(d).exists()]
    skipped = len(days) - len(todo)
    if skipped:
        print(f"Skipping {skipped:,} days already saved.")
    if not todo:
        print("Nothing to do.")
        return {"scraped": 0, "plays": 0, "failed": 0, "skipped": skipped}

    est_hours = len(todo) * (delay + JITTER_SECONDS / 2 + 0.5) / 3600
    print(f"Scraping {len(todo):,} days (~{est_hours:.1f} hours at {delay}s delay).")

    total_plays = scraped = failed = consecutive = 0
    bar = tqdm(todo, unit="day")
    for i, d in enumerate(bar):
        bar.set_postfix(date=d.isoformat(), plays=f"{total_plays:,}")
        try:
            total_plays += scrape_day(d, session)
            scraped += 1
            consecutive = 0
        except BlockedError as e:
            log.error("%s: %s", d, e)
            record_failure(d, e)
            print(f"\nGot 403 Forbidden on {d}. Stopping so we don't get blocked further. "
                  "Wait a few hours, then re-run the same command to resume.")
            break
        except Exception as e:
            log.error("%s: %s: %s", d, type(e).__name__, e)
            record_failure(d, e)
            failed += 1
            consecutive += 1
            if consecutive >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n{consecutive} days in a row failed (last: {d}: {e}). Stopping. "
                      "Check data/scrape.log, then re-run to resume.")
                break
        if i < len(todo) - 1:
            time.sleep(delay + random.uniform(0, JITTER_SECONDS))
    bar.close()

    summary = {"scraped": scraped, "plays": total_plays, "failed": failed, "skipped": skipped}
    print(f"Done: {scraped:,} days scraped, {total_plays:,} plays, {failed} failed.")
    if failed:
        print(f"Failed dates are listed in {FAILED_FILE}. Run retry_failed() / --retry-failed.")
    return summary


def scrape_range(start=START_DATE, end=None, delay: float = DELAY_SECONDS, newest_first: bool = False) -> dict:
    """Scrape every day from start to end (inclusive). end defaults to yesterday."""
    end = end or (date.today() - timedelta(days=1))
    return scrape_dates(date_list(start, end, newest_first), delay)


def retry_failed(delay: float = DELAY_SECONDS) -> dict:
    """Re-try every date listed in data/failed_dates.txt that still isn't saved."""
    if not FAILED_FILE.exists():
        print("No failed dates.")
        return {}
    days = sorted({to_date(line.split("\t")[0]) for line in FAILED_FILE.read_text().splitlines() if line.strip()})
    FAILED_FILE.rename(FAILED_FILE.with_suffix(".previous.txt"))  # fresh list for this attempt
    return scrape_dates(days, delay)


# ----------------------------------------------------------------------------
# Combine
# ----------------------------------------------------------------------------
def read_daily(files: list[Path]):
    """Read daily CSVs into one typed DataFrame. Returns (df, number of days with 0 plays)."""
    import pandas as pd

    frames = [pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""]) for f in files]
    empty_days = sum(len(f) == 0 for f in frames)
    df = pd.concat(frames, ignore_index=True)

    # played_at strings carry -05:00/-06:00 offsets; normalize to Central time
    df["played_at"] = pd.to_datetime(df["played_at"], utc=True).dt.tz_convert("America/Chicago")
    for col in ["play_id", "song_id", "service_id"]:
        df[col] = pd.to_numeric(df[col]).astype("Int64")

    df = df.drop_duplicates("play_id").sort_values("played_at").reset_index(drop=True)
    return df, empty_days


def combine(write: bool = True):
    """Merge all daily CSVs into one DataFrame and save data/plays.parquet + data/plays.csv."""
    files = sorted(RAW_DIR.glob("*/*.csv"))
    if not files:
        raise FileNotFoundError(f"No daily files in {RAW_DIR}. Run the scraper first.")

    df, empty_days = read_daily(files)

    if write:
        df.to_parquet(DATA_DIR / "plays.parquet", index=False)
        df.to_csv(DATA_DIR / "plays.csv", index=False)

    print(f"{len(df):,} plays from {len(files):,} days "
          f"({df['played_at'].min():%Y-%m-%d} → {df['played_at'].max():%Y-%m-%d}); "
          f"{empty_days} days with 0 plays.")
    print(f"art_url present on {df['art_url'].notna().mean():.0%} of plays.")
    if write:
        print(f"Saved {DATA_DIR / 'plays.parquet'} and {DATA_DIR / 'plays.csv'}")
    return df


# ----------------------------------------------------------------------------
# Yearly files (committed to git; what the monthly GitHub Action updates)
# ----------------------------------------------------------------------------
def year_path(year: int) -> Path:
    return YEARLY_DIR / f"{year}.parquet"


def merge_into_yearly(df):
    """Add plays to data/plays/YYYY.parquet, one file per year (Central time), deduped on play_id."""
    import pandas as pd

    YEARLY_DIR.mkdir(parents=True, exist_ok=True)
    for year, new in df.groupby(df["played_at"].dt.year):
        path = year_path(year)
        if path.exists():
            new = pd.concat([pd.read_parquet(path), new], ignore_index=True)
        new = new.drop_duplicates("play_id").sort_values("played_at").reset_index(drop=True)
        new.to_parquet(path, index=False)


def write_yearly():
    """One-time setup: split data/plays.parquet into data/plays/YYYY.parquet."""
    import pandas as pd

    df = pd.read_parquet(DATA_DIR / "plays.parquet")
    merge_into_yearly(df)
    print(f"Wrote {df['played_at'].dt.year.nunique()} yearly files to {YEARLY_DIR}")


def last_saved_date() -> date:
    """Latest play date in the yearly files."""
    import pandas as pd

    files = sorted(YEARLY_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No yearly files in {YEARLY_DIR}. Run write_yearly() first.")
    return pd.read_parquet(files[-1], columns=["played_at"])["played_at"].max().date()


def update(delay: float = DELAY_SECONDS) -> dict:
    """Scrape from the day after the last saved play through yesterday, and merge into the yearly files.

    Only an unbroken run of days is merged, so a day that fails can't leave a gap:
    the next update simply starts again from the first missing day.
    """
    start = last_saved_date() + timedelta(days=1)
    end = date.today() - timedelta(days=1)
    if start > end:
        print(f"Already up to date (through {start - timedelta(days=1)}).")
        return {"complete": True, "new_plays": 0}

    print(f"Updating {start} → {end}")
    scrape_range(start, end, delay)

    days = []
    for d in date_list(start, end):
        if not day_path(d).exists():
            break
        days.append(d)

    new_plays = 0
    if days:
        df, _ = read_daily([day_path(d) for d in days])
        new_plays = len(df)
        if new_plays:
            merge_into_yearly(df)
        print(f"Merged {new_plays:,} plays ({days[0]} → {days[-1]}) into {YEARLY_DIR}")
    complete = len(days) == (end - start).days + 1
    if not complete:
        print(f"Stopped before {end}; the next update will resume from the first missing day.")
    return {"complete": complete, "new_plays": new_plays, "through": days[-1] if days else None}


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Scrape The Current's playlist history.")
    ap.add_argument("--start", default=START_DATE.isoformat(), help="YYYY-MM-DD (default: 2005-12-22)")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--delay", type=float, default=DELAY_SECONDS, help="seconds between requests (default: 5)")
    ap.add_argument("--newest-first", action="store_true", help="scrape from end date backwards")
    ap.add_argument("--retry-failed", action="store_true", help="only re-try dates in data/failed_dates.txt")
    ap.add_argument("--combine-only", action="store_true", help="skip scraping; just build data/plays.parquet")
    ap.add_argument("--no-combine", action="store_true", help="don't build data/plays.parquet after scraping")
    ap.add_argument("--update", action="store_true",
                    help="scrape from the last date in data/plays/ through yesterday and merge (used by the monthly GitHub Action)")
    ap.add_argument("--write-yearly", action="store_true", help="split data/plays.parquet into data/plays/YYYY.parquet")
    args = ap.parse_args()

    if args.update:
        result = update(args.delay)
        sys.exit(0 if result["complete"] else 1)
    if args.write_yearly:
        write_yearly()
        return

    try:
        if args.retry_failed:
            retry_failed(args.delay)
        elif not args.combine_only:
            scrape_range(args.start, args.end, args.delay, args.newest_first)
    except KeyboardInterrupt:
        print("\nStopped. Re-run the same command to pick up where you left off.")
        return

    if not args.no_combine:
        combine()


if __name__ == "__main__":
    main()
