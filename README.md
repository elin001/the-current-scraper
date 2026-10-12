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

## Monthly updates (GitHub Action)

[`.github/workflows/monthly-scrape.yml`](.github/workflows/monthly-scrape.yml) runs at 12:00 UTC on the 1st of
every month. It runs `python scraper.py --update`, which:

1. finds the last play date in `data/plays/`,
2. scrapes from the next day through yesterday (the whole previous month), and
3. merges the new plays into that year's `data/plays/YYYY.parquet` and commits it.

If a day fails, the Action merges the unbroken run of days before it and keeps that progress.
The run shows as failed (red), and the next run starts again from the missing day, so the data never has gaps.
To run it early, open the **Actions** tab, choose **Monthly playlist scrape**, then click **Run workflow**.
The scrape log for each run is saved as a downloadable artifact.

After a run, `git pull` to get the new data. You can run the same update locally with `.venv/bin/python scraper.py --update`.

## Analysis notebooks

Each notebook is a list of questions, with one `##` heading per question. They all load the data through
[`plays.py`](plays.py) (`load_plays()`), which reads `data/plays/` and adds shared helper columns:
`seconds`, `song_key`, `artist_key`, and `is_music`, which filters out shows, segments, interviews and clips.
Run the notebooks with the project's `.venv` kernel.

| notebook | what it covers |
|---|---|
| [`01_scrape.ipynb`](01_scrape.ipynb) | Runs the scraper. Ends with a **data dictionary**: what each column means, how complete each field is by year, and what in the log isn't a song. |
| [`02_songs.ipynb`](02_songs.ipynb) | **Songs:** total and unique songs played, the longest and shortest songs in rotation, the most-played songs overall and by year, and a Billboard-style **running chart**. The chart has a weekly top 100 (ranked on a rolling 4-week window), `hot10("any date")`, every #1 run with the song that replaced it, weeks in the top 10, and a heatmap of one year's top 10. |
| [`03_artists.ipynb`](03_artists.ipynb) | **Artists:** unique artists, new artists per year, the most-played artists overall and by year, and **89.3 in-studio performances**. That last section estimates who visited the studio most, plus whose session recordings were replayed most. |

## Output

- `data/plays/YYYY.parquet`: **the canonical dataset**. It's committed to git, with one file per year (Central time).
  Load everything with `pd.read_parquet("data/plays")`.
- `data/raw/YYYY/YYYY-MM-DD.csv`: one file per day, a local-only resumable cache (not in git)
- `data/plays.parquet` / `data/plays.csv`: everything combined from `data/raw/` (local only).
  It's rebuilt by the full scrape or `--combine-only`. It won't include months added by the Action.
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
