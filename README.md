# The Current playlist scraper

Scrapes every song played on [The Current](https://www.thecurrent.org) (89.3 FM, MPR) since its first
broadcast on 2005-12-22, using the daily playlist pages
(`https://www.thecurrent.org/playlist/the-current/YYYY-MM-DD`).

## Setup (one time)

```bash
cd the-current-scraper
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

`truststore` is included so Python trusts the macOS keychain. Without it, requests fail with
`CERTIFICATE_VERIFY_FAILED` behind the Strib network's Zscaler proxy.

## Run the full scrape (~12 hours)

```bash
caffeinate -i .venv/bin/python scraper.py
```

`caffeinate -i` keeps the Mac from sleeping while the scraper runs. When it finishes, the scraper builds `data/plays.parquet` automatically.

Other options:

| Command | What it does |
|---|---|
| `.venv/bin/python scraper.py --start 2010-01-01 --end 2010-12-31` | scrape just one range |
| `.venv/bin/python scraper.py --newest-first` | go backwards from yesterday |
| `.venv/bin/python scraper.py --retry-failed` | retry dates listed in `data/failed_dates.txt` |
| `.venv/bin/python scraper.py --combine-only` | rebuild `data/plays.parquet` from the daily files |
| `.venv/bin/python scraper.py --delay 8` | slow down further |

**Stopping and resuming:** press Ctrl-C (or close the laptop) whenever you like, then rerun the same command.
Days already saved in `data/raw/` are skipped.

**If it stops by itself:** it does that after a `403 Forbidden` or 3 failed days in a row, to avoid getting blocked.
Check `data/scrape.log`, wait a while, then rerun.

`01_scrape.ipynb` calls the same functions. It's handy for small ranges and for inspecting the combined data.

## Output

- `data/raw/YYYY/YYYY-MM-DD.csv`: one file per day (the raw, resumable cache)
- `data/plays.parquet`: everything combined and deduped. Load it with `pd.read_parquet("data/plays.parquet")`.
- `data/plays.csv`: the same data as CSV, for Excel or Sheets
- `data/failed_dates.txt`, `data/scrape.log`

| column | notes |
|---|---|
| `played_at` | timestamp, America/Chicago |
| `title`, `artist`, `album` | |
| `art_url` | album art image (missing for some older plays) |
| `play_id` | unique per play (dedupe key) |
| `song_id` | The Current's internal song ID (the same song shares an ID across plays) |
| `duration` | `m:ss` |
| `album_mbid` | MusicBrainz release ID, for future release-year/genre lookups |
| `spotify_id` | Spotify track ID (~30% filled) |
| `artist_ulid`, `album_ulid` | The Current's internal IDs |
| `song_type`, `additional_text`, `service_id` | rarely useful, kept for completeness |
