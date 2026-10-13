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

[`.github/workflows/monthly-scrape.yml`](.github/workflows/monthly-scrape.yml) runs at 12:37 UTC on the 1st of
every month. It runs `python scraper.py --update`, which:

1. finds the last play date in `data/plays/`,
2. scrapes from the next day through yesterday (the whole previous month), and
3. merges the new plays into that year's `data/plays/YYYY.parquet` and commits it.

Then it runs `python enrich.py update --jitter`, which looks up only the albums and artists that aren't cached yet
and rebuilds `data/songs.parquet` and `data/artists.parquet` (see below). If MusicBrainz is down, the new plays
are still committed.

If a day fails, the Action merges the unbroken run of days before it and keeps that progress.
The run shows as failed (red), and the next run starts again from the missing day, so the data never has gaps.
To run it early, open the **Actions** tab, choose **Monthly playlist scrape**, then click **Run workflow**.
The scrape log for each run is saved as a downloadable artifact.

After a run, `git pull` to get the new data. You can run the same update locally with `.venv/bin/python scraper.py --update`.

## MusicBrainz enrichment (songs + artists tables)

[`enrich.py`](enrich.py) adds facts from [MusicBrainz](https://musicbrainz.org), with
[Wikidata](https://www.wikidata.org) filling gaps, and builds two tables:

- `data/songs.parquet`: one row per song, with original release date, genres and play stats
- `data/artists.parquet`: one row per station artist, with gender, birth/formation date and place, genres and play stats

```bash
caffeinate -i .venv/bin/python enrich.py all              # first run: every step, then build (a couple of hours)
.venv/bin/python enrich.py all --limit 300                # quick sample: the 300 most-played albums only
.venv/bin/python enrich.py build                          # rebuild the tables from the cache (seconds, no network)
```

Steps run in order: `albums` → `artists` → `wikidata` → `areas` → `genres` → `build`. Each one can be run on its own
(`enrich.py areas`). Raw API results are cached in `data/musicbrainz/` (committed), so every step is
resumable. Press Ctrl-C at any time, rerun the same command, and only uncached IDs are fetched. The log is `data/enrich.log`.

It follows the [MusicBrainz API rules](https://musicbrainz.org/doc/MusicBrainz_API/Rate_Limiting):
- At most one request every 1.1s, counted from the previous response, through one shared throttle.
- A User-Agent with contact info.
- JSON responses.
- Back off and retry on `503`.
- Known IDs are fetched 100 at a time through search, with a single lookup for IDs search misses.

Most of the time goes to `areas`: turning a birth area like "Minneapolis" into "Minneapolis, Minnesota, United
States" takes one request per new area.

How the tables are built:
- **Station artist → MusicBrainz artist:** taken from the artist credits on the albums each artist's plays point
  to. `match_method` is `exact`, `contained` (e.g. "X feat. Y"), or `single_credit` (the album's only credited
  artist, even though the name differs). Check rows where `mb_name` differs from `artist`.
  Artists whose plays have no album ID (~1% of plays) are left blank.
- **Release date:** the earliest `first-release-date` among the albums a song was played from, so compilations
  and reissues don't count. A song that was only ever a single, and that the station logs from a later compilation,
  gets the compilation's date (e.g. Joy Division's "Transmission" → *Substance*, 1988).
  `release_album_matches` flags songs where the station's album name differs from MusicBrainz's title, which
  catches wrong album IDs.
- **Genres:** MusicBrainz's official genres, picked out of its user tags and ordered by votes. They come from the
  release album, or from the artist when the album has none (`genres_source`). `tags` holds the raw tags.
- **Groups:** `birth_date` and `birth_location` are the formation date and place, and `gender` is empty.
- **Wikidata** only fills empty `gender` / `birth_date` / `birth_location` fields, looked up by MusicBrainz ID
  (never by name). `filled_from_wikidata` lists which fields it filled.

Load the tables with `from plays import load_songs, load_artists`.

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
| [`04_musicbrainz.ipynb`](04_musicbrainz.ipynb) | **Release years, genres and artists** (from the MusicBrainz enrichment): how much of the playlist could be matched, how old the songs are and how much is new, top genres and how they've shifted, plays by solo men vs. solo women vs. groups, Minnesota artists' share, where artists are from, and data checks. |

## Output

- `data/plays/YYYY.parquet`: **the canonical dataset**. It's committed to git, with one file per year (Central time).
  Load everything with `pd.read_parquet("data/plays")`.
- `data/raw/YYYY/YYYY-MM-DD.csv`: one file per day, a local-only resumable cache (not in git)
- `data/plays.parquet` / `data/plays.csv`: everything combined from `data/raw/` (local only).
  It's rebuilt by the full scrape or `--combine-only`. It won't include months added by the Action.
- `data/failed_dates.txt`, `data/scrape.log`
- `data/songs.parquet`, `data/artists.parquet`, `data/musicbrainz/`: MusicBrainz enrichment (see above), committed

| column | notes |
|---|---|
| `played_at` | timestamp, America/Chicago |
| `title`, `artist`, `album` | |
| `art_url` | album art image (missing for some older plays) |
| `play_id` | unique per play (dedupe key) |
| `song_id` | The Current's internal song ID (the same song shares an ID across plays) |
| `duration` | `m:ss` |
| `album_mbid` | MusicBrainz **release group** (album) ID, used by `enrich.py` |
| `spotify_id` | Spotify track ID (~30% filled) |
| `artist_ulid`, `album_ulid` | The Current's internal IDs |
| `song_type`, `additional_text`, `service_id` | rarely useful, kept for completeness |
