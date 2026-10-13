"""
Enrich The Current's plays with MusicBrainz (and Wikidata) facts, then build two tables:

    data/songs.parquet    one row per song: original release date, genres/tags, play stats
    data/artists.parquet  one row per station artist: gender, birth date/place, genres/tags, play stats

Raw API results are cached in data/musicbrainz/ so every step is resumable: re-running a
step only fetches IDs that aren't cached yet.

Usage:
    python enrich.py all                # every step, then build (first run: ~1-1.5 hours)
    python enrich.py all --limit 300    # only the 300 most-played albums (quick sample)
    python enrich.py update --jitter    # monthly GitHub Action: fetch what's new, then build
    python enrich.py albums | artists | wikidata | areas | genres | build

MusicBrainz API rules followed here (https://musicbrainz.org/doc/MusicBrainz_API):
- every request to musicbrainz.org goes through one shared throttle, at most one every 1.1s
- a descriptive User-Agent with contact info (reused from scraper.py)
- JSON via fmt=json; 503 means "slow down", so back off and retry
- known IDs are fetched 100 at a time through search (rgid:/arid:), with a single lookup
  as fallback for IDs search doesn't return (merged or not yet indexed)
"""

import argparse
import json
import logging
import random
import re
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

from plays import display_names, load_plays, normalize
from scraper import make_session  # importing scraper also sets up truststore (Zscaler)

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
MB_ROOT = "https://musicbrainz.org/ws/2/"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
MB_INTERVAL_SECONDS = 1.1  # MusicBrainz allows 1 request/second per IP
WIKIDATA_INTERVAL_SECONDS = 1.0
SEARCH_BATCH = 100  # MusicBrainz search returns at most 100 results
WIKIDATA_BATCH = 200
TIMEOUT_SECONDS = 30
MAX_RETRIES = 4
BACKOFF_SECONDS = [5, 15, 45, 120]  # wait before retry 1, 2, 3, 4
JITTER_MAX_SECONDS = 15 * 60  # --jitter: don't hit the API at the same minute every month
SAVE_EVERY = 50  # area lookups between cache saves

VARIOUS_ARTISTS_ID = "89ad4ac3-39f7-470e-963a-56509c546377"
GROUP_TYPES = {"Group", "Orchestra", "Choir"}  # no gender; begin date/area = formation
MATCH_RANK = {"exact": 0, "contained": 1, "single_credit": 2}

DATA_DIR = Path(__file__).resolve().parent / "data"
MB_DIR = DATA_DIR / "musicbrainz"
RG_FILE = MB_DIR / "release_groups.parquet"
ARTIST_FILE = MB_DIR / "artists.parquet"
AREA_FILE = MB_DIR / "areas.parquet"
GENRE_FILE = MB_DIR / "genres.txt"
SONGS_FILE = DATA_DIR / "songs.parquet"
ARTISTS_FILE = DATA_DIR / "artists.parquet"
LOG_FILE = DATA_DIR / "enrich.log"

RG_COLUMNS = ["album_mbid", "rg_id", "title", "release_type", "first_release_date", "tags",
              "artist_credit", "status"]
WD_COLUMNS = ["wd_gender", "wd_begin", "wd_place", "wd_state", "wd_country"]
ARTIST_COLUMNS = ["mb_artist_id", "id", "name", "type", "gender", "begin", "end", "begin_area_id",
                  "begin_area_name", "country", "tags", "status", "wd_checked", *WD_COLUMNS]
AREA_COLUMNS = ["area_id", "name", "type", "parent_id", "status"]

log = logging.getLogger("enrich")


class MusicBrainzError(Exception):
    """A request still failed after all retries (or failed in a way retrying won't fix)."""


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
    sh.setLevel(logging.WARNING)
    sh.setFormatter(fmt)
    log.addHandler(sh)


# ----------------------------------------------------------------------------
# HTTP
# ----------------------------------------------------------------------------
class Throttle:
    """Keeps calls at least `interval` seconds apart."""

    def __init__(self, interval: float, clock=time.monotonic, sleep=time.sleep):
        self.interval = interval
        self.clock = clock
        self.sleep = sleep
        self.last = None

    def wait(self):
        if self.last is not None:
            remaining = self.interval - (self.clock() - self.last)
            if remaining > 0:
                self.sleep(remaining)
        self.last = self.clock()


def _retry_get(session, url, params, headers, throttle, sleep, label):
    """GET with the shared throttle. Returns the response (200) or None (404); retries 429/5xx/network errors."""
    for attempt in range(MAX_RETRIES + 1):
        throttle.wait()
        try:
            r = session.get(url, params=params, headers=headers, timeout=TIMEOUT_SECONDS)
            status = r.status_code
        except requests.exceptions.RequestException as e:
            r, status = None, f"error ({e.__class__.__name__})"
        log.info("GET %s -> %s", label, status)
        if status == 200:
            return r
        if status == 404:
            return None
        retryable = not isinstance(status, int) or status == 429 or status >= 500
        if not retryable:
            raise MusicBrainzError(f"{status} for {label}")
        if attempt < MAX_RETRIES:
            wait = BACKOFF_SECONDS[attempt]
            log.warning("%s for %s, retrying in %ss", status, label, wait)
            sleep(wait)
    raise MusicBrainzError(f"gave up on {label} after {MAX_RETRIES + 1} tries (last: {status})")


class MusicBrainz:
    """musicbrainz.org/ws/2 client. Every request (search or lookup) shares one throttle."""

    def __init__(self, session=None, throttle=None, sleep=time.sleep):
        self.session = session or make_session()
        self.throttle = throttle or Throttle(MB_INTERVAL_SECONDS)
        self.sleep = sleep

    def get(self, path: str, fmt: str = "json", **params):
        """JSON (or text, with fmt="txt") for ws/2/<path>, or None if it doesn't exist."""
        params = {**params, "fmt": fmt}
        accept = "application/json" if fmt == "json" else "text/plain"
        label = path + (f" [{params['query'][:60]}…]" if "query" in params else "")
        r = _retry_get(self.session, MB_ROOT + path, params, {"Accept": accept}, self.throttle, self.sleep, label)
        if r is None:
            return None
        return r.json() if fmt == "json" else r.text

    def search(self, entity: str, field: str, ids: list[str]) -> list[dict]:
        """Up to 100 entities by ID in one request, e.g. search("release-group", "rgid", ids)."""
        d = self.get(entity + "/", query=id_query(field, ids), limit=SEARCH_BATCH)
        return (d or {}).get(entity + "s", [])


def chunks(seq, n):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def id_query(field: str, ids) -> str:
    return " OR ".join(f"{field}:{i}" for i in ids)


# ----------------------------------------------------------------------------
# Small text helpers
# ----------------------------------------------------------------------------
def tag_string(tags) -> str:
    """MusicBrainz tags → "most votes, next, …" (downvoted tags dropped)."""
    tags = [t for t in (tags or []) if t.get("count", 0) > 0]
    tags.sort(key=lambda t: (-t["count"], t["name"]))
    return ", ".join(t["name"] for t in tags)


def split_list(s) -> list[str]:
    return [x for x in s.split(", ") if x] if isinstance(s, str) else []


def genres_from_tags(tags, genres: set[str]) -> str:
    """MusicBrainz genres are the tags whose name is on its official genre list."""
    return ", ".join(t for t in split_list(tags) if t in genres)


def wikidata_date(value, precision) -> str:
    """Wikidata time + precision (11 day, 10 month, 9 year) → "1958-06-07" / "1958-06" / "1958"."""
    if not value:
        return ""
    d = value.lstrip("+")[:10]
    prec = int(precision or 11)
    return d if prec >= 11 else d[:7] if prec == 10 else d[:4]


def loose(s) -> str:
    """Looser than plays.normalize, for comparing names: also drops punctuation, "&" → "and", leading "the"."""
    s = unicodedata.normalize("NFKD", s if isinstance(s, str) else "")
    s = "".join(c for c in s if not unicodedata.combining(c)).casefold().replace("&", " and ")
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return re.sub(r"^the ", "", s)


def titles_match(a, b) -> bool:
    """Same album title, ignoring case, punctuation and "(Deluxe Edition)"-style suffixes."""
    strip = lambda s: re.sub(r"\s*[(\[].*?[)\]]", "", s if isinstance(s, str) else "")
    return loose(strip(a)) == loose(strip(b))


# ----------------------------------------------------------------------------
# Parsing API responses into cache rows
# ----------------------------------------------------------------------------
def parse_release_group(album_mbid: str, d: dict) -> dict:
    types = [d.get("primary-type"), *(d.get("secondary-types") or [])]
    credit = [{"id": ac["artist"]["id"], "name": ac["artist"]["name"],
               "credited": ac.get("name") or ac["artist"]["name"]}
              for ac in d.get("artist-credit") or []]
    return {
        "album_mbid": album_mbid,
        "rg_id": d["id"],
        "title": d.get("title") or "",
        "release_type": " + ".join(t for t in types if t),
        "first_release_date": d.get("first-release-date") or "",
        "tags": tag_string(d.get("tags")),
        "artist_credit": json.dumps(credit),
        "status": "ok",
    }


def parse_artist(mb_artist_id: str, d: dict) -> dict:
    life = d.get("life-span") or {}
    begin_area = d.get("begin-area") or {}
    return {
        "mb_artist_id": mb_artist_id,
        "id": d["id"],
        "name": d.get("name") or "",
        "type": d.get("type") or "",
        "gender": (d.get("gender") or "").capitalize(),
        "begin": life.get("begin") or "",
        "end": life.get("end") or "",
        "begin_area_id": begin_area.get("id") or "",
        "begin_area_name": begin_area.get("name") or "",
        "country": d.get("country") or "",
        "tags": tag_string(d.get("tags")),
        "status": "ok",
        "wd_checked": False,
        **{c: "" for c in WD_COLUMNS},
    }


def parse_area(area_id: str, d: dict) -> dict:
    parent = next((r["area"]["id"] for r in d.get("relations") or []
                   if r.get("type") == "part of" and r.get("direction") == "backward"), None)
    return {"area_id": area_id, "name": d.get("name") or "", "type": d.get("type") or "",
            "parent_id": parent, "status": "ok"}


def parse_wikidata(bindings: list[dict]) -> dict[str, dict]:
    """SPARQL rows → {mb_artist_id: wd_* fields}. The first row per artist wins."""
    out = {}
    for b in bindings:
        v = lambda k: b.get(k, {}).get("value", "")
        mbid = v("mbid")
        if mbid in out:
            continue
        gender = v("genderLabel")
        begin = wikidata_date(v("dob"), v("dobPrec")) or wikidata_date(v("inc"), v("incPrec"))
        out[mbid] = {
            # an unlabeled item comes back as its URL/Q-id; treat that as missing
            "wd_gender": "" if re.match(r"^(Q\d+|https?:)", gender) else gender[:1].upper() + gender[1:],
            "wd_begin": begin,
            "wd_place": v("placeLabel"),
            "wd_state": v("stateLabel"),
            "wd_country": v("countryLabel"),
        }
    return out


# ----------------------------------------------------------------------------
# Locations
# ----------------------------------------------------------------------------
def location_parts(area_id, areas: dict) -> tuple[str, str, str]:
    """(place, state, country) by walking up MusicBrainz "part of" areas.

    state is the subdivision just below the country (Minnesota, not Hennepin County)."""
    place = state = country = ""
    seen, cur, first = set(), area_id, True
    while isinstance(cur, str) and cur in areas and cur not in seen:
        seen.add(cur)
        a = areas[cur]
        if a["type"] == "Country":
            country = a["name"]
            break
        if a["type"] == "Subdivision":
            state = a["name"]
        elif first:
            place = a["name"]
        first = False
        cur = a.get("parent_id")
    return place, state, country


def format_location(parts) -> str:
    out = []
    for p in parts:
        if p and (not out or out[-1] != p):
            out.append(p)
    return ", ".join(out)


# ----------------------------------------------------------------------------
# Matching station artists to MusicBrainz artists
# ----------------------------------------------------------------------------
def pick_credit(station_artist: str, credit: list[dict]):
    """Which credited MusicBrainz artist is this station artist? → (id, name, method) or None."""
    credit = [c for c in credit if c["id"] != VARIOUS_ARTISTS_ID]
    if not credit:
        return None
    station = loose(station_artist)
    tests = {
        "exact": lambda n: n == station,
        "contained": lambda n: bool(n) and re.search(rf"\b{re.escape(n)}\b", station) is not None,
    }
    for method, test in tests.items():
        for c in credit:
            if test(loose(c["name"])) or test(loose(c["credited"])):
                return c["id"], c["name"], method
    if len(credit) == 1:
        return credit[0]["id"], credit[0]["name"], "single_credit"
    return None


def match_artists(plays: pd.DataFrame, rgs: pd.DataFrame) -> pd.DataFrame:
    """artist_key → mb_artist_id, mb_name, match_method: the MusicBrainz artist most of its plays point to."""
    credits = {r.album_mbid: json.loads(r.artist_credit)
               for r in rgs[rgs["status"] == "ok"].itertuples()}
    pairs = plays.dropna(subset=["album_mbid"]).groupby(["artist_key", "album_mbid"]).size()
    rows = []
    for (artist_key, album_mbid), n in pairs.items():
        picked = pick_credit(artist_key, credits[album_mbid]) if album_mbid in credits else None
        if picked:
            rows.append((artist_key, *picked, MATCH_RANK[picked[2]], n))
    cols = ["artist_key", "mb_artist_id", "mb_name", "match_method", "rank", "n"]
    df = pd.DataFrame(rows, columns=cols)
    best = (df.groupby(["artist_key", "mb_artist_id"])
            .agg(n=("n", "sum"), rank=("rank", "min"), mb_name=("mb_name", "first"))
            .reset_index()
            .sort_values(["n", "rank"], ascending=[False, True])
            .drop_duplicates("artist_key"))
    best["match_method"] = best["rank"].map({v: k for k, v in MATCH_RANK.items()})
    return best.set_index("artist_key")[["mb_artist_id", "mb_name", "match_method"]]


# ----------------------------------------------------------------------------
# Building the tables
# ----------------------------------------------------------------------------
def _music(plays: pd.DataFrame) -> pd.DataFrame:
    return plays[plays["is_music"]]


def build_artists(plays, rgs, artists_cache, areas, genres: set[str]) -> pd.DataFrame:
    music = _music(plays)
    music = music.assign(album_key=normalize(music["album"].fillna("")).replace("", pd.NA))
    stats = music.groupby("artist_key").agg(
        plays=("played_at", "size"), songs=("song_key", "nunique"), albums=("album_key", "nunique"),
        first_played=("played_at", "min"), last_played=("played_at", "max"))
    out = display_names(music, "artist_key", ["artist"]).join(match_artists(music, rgs)).join(stats)

    area_map = areas[areas["status"] == "ok"].set_index("area_id")[["name", "type", "parent_id"]].to_dict("index")
    facts = artists_cache[artists_cache["status"] == "ok"].set_index("mb_artist_id")

    def describe(mb_artist_id):
        if not isinstance(mb_artist_id, str) or mb_artist_id not in facts.index:
            return {}
        a = facts.loc[mb_artist_id]
        is_group = a["type"] in GROUP_TYPES
        gender = "" if is_group else a["gender"]
        birth_date = a["begin"]
        parts = location_parts(a["begin_area_id"], area_map) if a["begin_area_id"] else ("", "", "")
        filled = []
        if not gender and not is_group and a["wd_gender"]:
            gender = a["wd_gender"]
            filled.append("gender")
        if not birth_date and a["wd_begin"]:
            birth_date = a["wd_begin"]
            filled.append("birth_date")
        if not any(parts) and (a["wd_place"] or a["wd_state"] or a["wd_country"]):
            parts = (a["wd_place"], a["wd_state"], a["wd_country"])
            filled.append("birth_location")
        return {"type": a["type"], "gender": gender, "birth_date": birth_date,
                "birth_location": format_location(parts), "birth_state": parts[1], "birth_country": parts[2],
                "country": a["country"], "genres": genres_from_tags(a["tags"], genres), "tags": a["tags"],
                "filled_from_wikidata": ", ".join(filled)}

    described = pd.DataFrame([describe(i) for i in out["mb_artist_id"]], index=out.index)
    out = out.join(described).reset_index()
    cols = ["artist", "artist_key", "mb_artist_id", "mb_name", "match_method", "type", "gender", "birth_date",
            "birth_location", "birth_state", "birth_country", "country", "genres", "tags",
            "filled_from_wikidata", "plays", "songs", "albums", "first_played", "last_played"]
    out = out.reindex(columns=cols)
    return out.sort_values("plays", ascending=False).reset_index(drop=True)


def build_songs(plays, rgs, artist_table, genres: set[str]) -> pd.DataFrame:
    music = _music(plays)
    stats = music.groupby("song_key").agg(
        artist_key=("artist_key", "first"), plays=("played_at", "size"),
        first_played=("played_at", "min"), last_played=("played_at", "max"))
    out = display_names(music, "song_key", ["title", "artist"]).join(stats)

    # the station's album name for each (song, album ID), joined to what MusicBrainz says about that album
    ok = rgs[rgs["status"] == "ok"].set_index("album_mbid")
    albums = (music.dropna(subset=["album_mbid"])
              .groupby(["song_key", "album_mbid", "album"], dropna=False).size().reset_index(name="n")
              .sort_values("n", ascending=False).drop_duplicates(["song_key", "album_mbid"]))
    albums = albums[albums["album_mbid"].isin(ok.index)].join(
        ok[["title", "first_release_date", "release_type", "tags", "artist_credit"]], on="album_mbid")
    out["album_ids_checked"] = albums.groupby("song_key").size().reindex(out.index, fill_value=0)

    # release album = earliest-dated album, preferring ones credited to the song's matched artist
    artist_ids = artist_table.set_index("artist_key")["mb_artist_id"]
    song_artist = albums["song_key"].map(out["artist_key"]).map(artist_ids)
    albums["credited"] = [isinstance(a, str) and a in {c["id"] for c in json.loads(ac)}
                          for a, ac in zip(song_artist, albums["artist_credit"])]
    release = (albums[albums["first_release_date"] != ""]
               .sort_values(["credited", "first_release_date"], ascending=[False, True])
               .drop_duplicates("song_key").set_index("song_key"))
    out["release_date"] = release["first_release_date"]
    out["release_year"] = pd.to_numeric(out["release_date"].str[:4], errors="coerce").astype("Int64")
    out["release_album"] = release["title"]
    out["release_album_type"] = release["release_type"]
    out["release_album_matches"] = pd.Series(
        [titles_match(a, t) for a, t in zip(release["album"], release["title"])],
        index=release.index, dtype="boolean")

    # genres/tags: the release album's, else the artist's
    artist_info = artist_table.set_index("artist_key")
    for col in ["genres", "tags"]:
        album_vals = release["tags"].map(lambda t: genres_from_tags(t, genres)) if col == "genres" else release["tags"]
        album_vals = album_vals.reindex(out.index).fillna("")
        artist_vals = out["artist_key"].map(artist_info[col]).fillna("")
        out[col] = album_vals.where(album_vals != "", artist_vals)
        out[f"{col}_source"] = pd.Series("album", index=out.index).where(album_vals != "", "artist")
        out.loc[out[col] == "", f"{col}_source"] = ""

    out = out.reset_index()
    cols = ["title", "artist", "song_key", "artist_key", "release_date", "release_year", "release_album",
            "release_album_type", "release_album_matches", "genres", "genres_source", "tags", "tags_source",
            "plays", "first_played", "last_played", "album_ids_checked"]
    return out[cols].sort_values("plays", ascending=False).reset_index(drop=True)


# ----------------------------------------------------------------------------
# Caches
# ----------------------------------------------------------------------------
def read_cache(path: Path, columns: list[str]) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame(columns=columns)


def write_atomic(df: pd.DataFrame, path: Path):
    """tmp file → rename, so an interrupted run never leaves half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


def upsert(cache: pd.DataFrame, rows: list[dict], key: str, columns: list[str]) -> pd.DataFrame:
    if not rows:
        return cache
    new = pd.DataFrame(rows).reindex(columns=columns)
    if cache.empty:
        return new
    return pd.concat([cache[~cache[key].isin(new[key])], new], ignore_index=True)


def missing_row(key: str, value: str, columns: list[str]) -> dict:
    row = {c: "" for c in columns}
    row.update({key: value, "status": "not_found"})
    if "wd_checked" in row:
        row["wd_checked"] = False
    return row


# ----------------------------------------------------------------------------
# Steps
# ----------------------------------------------------------------------------
def album_ids(plays: pd.DataFrame, limit: int | None = None) -> list[str]:
    """Album IDs, most-played first."""
    ids = plays["album_mbid"].dropna().value_counts().index.tolist()
    return ids[:limit] if limit else ids


def step_albums(client: MusicBrainz, plays: pd.DataFrame, limit: int | None = None):
    cache = read_cache(RG_FILE, RG_COLUMNS)
    done = set(cache["album_mbid"])
    todo = [i for i in album_ids(plays, limit) if i not in done]
    log.info("albums: %d to fetch (%d cached)", len(todo), len(done))
    for batch in tqdm(list(chunks(todo, SEARCH_BATCH)), desc="albums", unit="batch"):
        found = {d["id"]: d for d in client.search("release-group", "rgid", batch)}
        rows = []
        for album_mbid in batch:
            d = found.get(album_mbid)
            if d is None:  # merged or not in the search index yet: a lookup follows merges
                d = client.get(f"release-group/{album_mbid}", inc="artist-credits+tags")
            rows.append(parse_release_group(album_mbid, d) if d else missing_row("album_mbid", album_mbid, RG_COLUMNS))
        cache = upsert(cache, rows, "album_mbid", RG_COLUMNS)
        write_atomic(cache, RG_FILE)


def step_artists(client: MusicBrainz, plays: pd.DataFrame):
    rgs = read_cache(RG_FILE, RG_COLUMNS)
    if rgs.empty:
        return
    wanted = match_artists(_music(plays), rgs)["mb_artist_id"].unique().tolist()
    cache = read_cache(ARTIST_FILE, ARTIST_COLUMNS)
    done = set(cache["mb_artist_id"])
    todo = [i for i in wanted if i not in done]
    log.info("artists: %d to fetch (%d cached)", len(todo), len(done))
    for batch in tqdm(list(chunks(todo, SEARCH_BATCH)), desc="artists", unit="batch"):
        found = {d["id"]: d for d in client.search("artist", "arid", batch)}
        rows = []
        for mb_artist_id in batch:
            d = found.get(mb_artist_id)
            if d is None:
                d = client.get(f"artist/{mb_artist_id}", inc="tags")
            rows.append(parse_artist(mb_artist_id, d) if d else missing_row("mb_artist_id", mb_artist_id, ARTIST_COLUMNS))
        cache = upsert(cache, rows, "mb_artist_id", ARTIST_COLUMNS)
        write_atomic(cache, ARTIST_FILE)


SPARQL = """
SELECT ?mbid ?genderLabel ?dob ?dobPrec ?inc ?incPrec ?placeLabel ?stateLabel ?countryLabel WHERE {
  VALUES ?mbid { %s }
  ?item wdt:P434 ?mbid .
  OPTIONAL { ?item wdt:P21 ?gender . }
  OPTIONAL { ?item p:P569/psv:P569 [ wikibase:timeValue ?dob ; wikibase:timePrecision ?dobPrec ] . }
  OPTIONAL { ?item p:P571/psv:P571 [ wikibase:timeValue ?inc ; wikibase:timePrecision ?incPrec ] . }
  OPTIONAL {
    { ?item wdt:P19 ?place . } UNION { ?item wdt:P740 ?place . }
    OPTIONAL { ?place wdt:P17 ?country . }
    OPTIONAL { ?place wdt:P131* ?state . ?state wdt:P31/wdt:P279* wd:Q10864048 . }
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}"""


def needs_wikidata(a: pd.DataFrame) -> pd.Series:
    """Matched artists missing gender (people only), a begin date, or a begin area."""
    missing_gender = (a["gender"] == "") & ~a["type"].isin(GROUP_TYPES)
    missing = missing_gender | (a["begin"] == "") | (a["begin_area_id"] == "")
    return (a["status"] == "ok") & ~a["wd_checked"].astype(bool) & missing


def step_wikidata(session: requests.Session | None = None):
    cache = read_cache(ARTIST_FILE, ARTIST_COLUMNS)
    if cache.empty:
        return
    session = session or make_session()
    throttle = Throttle(WIKIDATA_INTERVAL_SECONDS)
    todo = cache.loc[needs_wikidata(cache), "id"].tolist()
    log.info("wikidata: %d artists to look up", len(todo))
    for batch in tqdm(list(chunks(todo, WIKIDATA_BATCH)), desc="wikidata", unit="batch"):
        query = SPARQL % " ".join(f'"{i}"' for i in batch)
        r = _retry_get(session, WIKIDATA_SPARQL, {"query": query, "format": "json"},
                       {"Accept": "application/sparql-results+json"}, throttle, time.sleep,
                       f"wikidata ({len(batch)} artists)")
        found = parse_wikidata(r.json()["results"]["bindings"]) if r is not None else {}
        rows = cache["id"].isin(batch)
        for col in WD_COLUMNS:
            cache.loc[rows, col] = cache.loc[rows, "id"].map(lambda i: found.get(i, {}).get(col, ""))
        cache.loc[rows, "wd_checked"] = True
        write_atomic(cache, ARTIST_FILE)


def step_areas(client: MusicBrainz):
    artists = read_cache(ARTIST_FILE, ARTIST_COLUMNS)
    cache = read_cache(AREA_FILE, AREA_COLUMNS)
    known = set(cache["area_id"])
    queue = [i for i in artists["begin_area_id"].dropna().unique() if i and i not in known]
    # parents of cached areas that were never fetched (e.g. an interrupted walk)
    queue += [p for p in cache["parent_id"].dropna().unique() if p not in known and p not in queue]
    log.info("areas: %d to fetch (%d cached)", len(queue), len(known))
    rows, bar = [], tqdm(desc="areas", unit="area")
    while queue:
        area_id = queue.pop(0)
        if area_id in known:
            continue
        d = client.get(f"area/{area_id}", inc="area-rels")
        row = parse_area(area_id, d) if d else missing_row("area_id", area_id, AREA_COLUMNS)
        rows.append(row)
        known.add(area_id)
        if row.get("parent_id") and row["parent_id"] not in known:
            queue.append(row["parent_id"])
        bar.update(1)
        if len(rows) >= SAVE_EVERY:
            cache, rows = upsert(cache, rows, "area_id", AREA_COLUMNS), []
            write_atomic(cache, AREA_FILE)
    bar.close()
    if rows:
        write_atomic(upsert(cache, rows, "area_id", AREA_COLUMNS), AREA_FILE)


def step_genres(client: MusicBrainz):
    """MusicBrainz's official genre list (one request), used to pick genres out of tags."""
    text = client.get("genre/all", fmt="txt")
    names = sorted({line.strip().casefold() for line in (text or "").splitlines() if line.strip()})
    if names:
        MB_DIR.mkdir(parents=True, exist_ok=True)
        GENRE_FILE.write_text("\n".join(names) + "\n", encoding="utf-8")
        log.info("genres: %d", len(names))


def load_genres() -> set[str]:
    if not GENRE_FILE.exists():
        return set()
    return {g for g in GENRE_FILE.read_text(encoding="utf-8").splitlines() if g}


def step_build(plays: pd.DataFrame):
    rgs = read_cache(RG_FILE, RG_COLUMNS)
    artists = read_cache(ARTIST_FILE, ARTIST_COLUMNS)
    areas = read_cache(AREA_FILE, AREA_COLUMNS)
    genres = load_genres()
    artist_table = build_artists(plays, rgs, artists, areas, genres)
    songs = build_songs(plays, rgs, artist_table, genres)
    write_atomic(artist_table, ARTISTS_FILE)
    write_atomic(songs, SONGS_FILE)
    log.info("build: %d songs, %d artists", len(songs), len(artist_table))
    print(f"Wrote {len(songs):,} songs → {SONGS_FILE.relative_to(DATA_DIR.parent)}, "
          f"{len(artist_table):,} artists → {ARTISTS_FILE.relative_to(DATA_DIR.parent)}")


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
STEPS = ["albums", "artists", "wikidata", "areas", "genres", "build"]


def run(steps: list[str], limit: int | None = None, jitter: bool = False):
    setup_logging()
    plays = load_plays()
    client = MusicBrainz()
    if jitter and set(steps) - {"build"}:
        wait = random.uniform(0, JITTER_MAX_SECONDS)
        log.info("jitter: sleeping %.0fs before the first request", wait)
        time.sleep(wait)
    for step in steps:
        log.info("step %s", step)
        if step == "albums":
            step_albums(client, plays, limit)
        elif step == "artists":
            step_artists(client, plays)
        elif step == "wikidata":
            step_wikidata()
        elif step == "areas":
            step_areas(client)
        elif step == "genres":
            step_genres(client)
        elif step == "build":
            step_build(plays)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("step", choices=["all", "update", *STEPS])
    p.add_argument("--limit", type=int, help="only the N most-played album IDs (for a quick sample)")
    p.add_argument("--jitter", action="store_true", help="wait a random 0-15 min first (for scheduled runs)")
    args = p.parse_args()
    # `update` is `all`: every step only fetches what isn't cached yet
    steps = STEPS if args.step in ("all", "update") else [args.step]
    run(steps, limit=args.limit, jitter=args.jitter)


if __name__ == "__main__":
    main()
