"""
Load The Current's playlist dataset (data/plays/YYYY.parquet) with a few helper
columns that the analysis notebooks share.

    from plays import load_plays, load_songs, load_artists
    df = load_plays()
    songs = load_songs()      # one row per song, with MusicBrainz release date + genres (see enrich.py)
    artists = load_artists()  # one row per artist, with MusicBrainz/Wikidata gender, birth date/place
"""

from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data" / "plays"
SONGS_FILE = DATA_DIR.parent / "songs.parquet"
ARTISTS_FILE = DATA_DIR.parent / "artists.parquet"

# The playlist log isn't only songs: syndicated shows are logged in segments, full
# concerts and specials get one long entry, and hosts log interviews, promos and
# sound clips. These patterns (matched case-insensitively) catch the common ones.
NOT_MUSIC_TITLE = (
    r"^(?:hour \w+ )?segment \d+$"            # "Hour 2 Segment 1" (American Routes, Sounds Eclectic...)
    r"|^rebroadcast$"
    r"|interview"
    r"|^(?:live )?in[- ]?st[ud]{2}io\b"        # "LIVE IN STUDIO", "In-Studio Performance Recorded on..."
    r"|^virtual session$"
    r"|\bclip\b|\bpromo\b"
    r"|theft of the dial"                     # guest DJ show
    r"|^live (?:at|from)\b"                   # "Live at First Avenue" full concerts
)
NOT_MUSIC_ARTIST = r"^(?:hosted by|host|with) |interview"
MAX_SONG_MINUTES = 20  # longer entries are shows/concerts, not songs


def normalize(s: pd.Series) -> pd.Series:
    """Casefold, drop accents (Poliça → polica) and collapse whitespace."""
    return (
        s.str.normalize("NFKD")
        .str.replace(r"[̀-ͯ]", "", regex=True)
        .str.casefold()
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def load_plays() -> pd.DataFrame:
    """All plays, oldest first, plus:

    - seconds:    duration in seconds (NaN when missing or logged as 0:00)
    - song_key:   normalized "title|artist", so the same song on different albums/compilations counts once
    - artist_key: normalized artist name
    - is_music:   False for entries that look like shows, segments, interviews, clips or full concerts
    """
    df = pd.read_parquet(DATA_DIR).sort_values("played_at").reset_index(drop=True)

    parts = df["duration"].str.split(":", expand=True).astype(float)
    df["seconds"] = (parts[0] * 60 + parts[1]).where(lambda s: s > 0)

    df["artist_key"] = normalize(df["artist"])
    df["song_key"] = normalize(df["title"]) + "|" + df["artist_key"]

    looks_like_program = (
        df["title"].str.contains(NOT_MUSIC_TITLE, case=False, na=False)
        | df["artist"].str.contains(NOT_MUSIC_ARTIST, case=False, na=False)
        | (df["seconds"] > MAX_SONG_MINUTES * 60)
    )
    df["is_music"] = ~looks_like_program
    return df


def load_songs() -> pd.DataFrame:
    """data/songs.parquet, built by `python enrich.py build`."""
    return pd.read_parquet(SONGS_FILE)


def load_artists() -> pd.DataFrame:
    """data/artists.parquet, built by `python enrich.py build`."""
    return pd.read_parquet(ARTISTS_FILE)


def display_names(df: pd.DataFrame, key: str, cols: list[str]) -> pd.DataFrame:
    """Most common original spelling of `cols` for each value of `key` (e.g. song_key → title, artist)."""
    counts = df.groupby([key, *cols], dropna=False).size().sort_values(ascending=False).reset_index()
    return counts.drop_duplicates(key).set_index(key)[cols]


def fmt_duration(seconds: float) -> str:
    seconds = int(round(seconds))
    return f"{seconds // 60}:{seconds % 60:02d}"
