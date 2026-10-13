import json

import pandas as pd
import pytest

import enrich

PRINCE = "070d193a-845c-479f-980e-bef15710653e"
VARIOUS = enrich.VARIOUS_ARTISTS_ID


# ---------------------------------------------------------------------------
# Throttle + client
# ---------------------------------------------------------------------------
class FakeClock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def test_throttle_spaces_calls_by_interval():
    clock = FakeClock()
    t = enrich.Throttle(1.1, clock=clock.time, sleep=clock.sleep)
    t.wait()  # first call goes straight through
    clock.now += 0.3
    t.wait()  # 0.3s later: must wait the remaining 0.8s
    assert clock.sleeps == [pytest.approx(0.8)]


class FakeResponse:
    def __init__(self, status, payload=None, text=""):
        self.status_code = status
        self._payload = payload
        self.text = text
        self.url = "https://musicbrainz.org/ws/2/x"

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "headers": dict(headers or {})})
        return self.responses.pop(0)


def make_client(responses):
    clock = FakeClock()
    session = FakeSession(responses)
    client = enrich.MusicBrainz(session=session, throttle=enrich.Throttle(1.1, clock=clock.time, sleep=clock.sleep),
                                sleep=clock.sleep)
    return client, session, clock


def test_client_requests_json():
    client, session, _ = make_client([FakeResponse(200, {"ok": 1})])
    assert client.get("artist/x") == {"ok": 1}
    assert session.calls[0]["params"]["fmt"] == "json"
    assert session.calls[0]["headers"]["Accept"] == "application/json"


def test_client_returns_none_on_404():
    client, _, _ = make_client([FakeResponse(404)])
    assert client.get("release-group/x") is None


def test_client_backs_off_and_retries_on_503():
    client, session, clock = make_client([FakeResponse(503), FakeResponse(200, {"ok": 1})])
    assert client.get("artist/x") == {"ok": 1}
    assert len(session.calls) == 2
    assert enrich.BACKOFF_SECONDS[0] in clock.sleeps


def test_client_gives_up_after_max_retries():
    client, _, _ = make_client([FakeResponse(503)] * (enrich.MAX_RETRIES + 1))
    with pytest.raises(enrich.MusicBrainzError):
        client.get("artist/x")


def test_every_request_goes_through_the_shared_throttle():
    client, _, clock = make_client([FakeResponse(200, {"a": 1}), FakeResponse(200, {"b": 1})])
    client.get("release-group/", query="rgid:x")
    client.get("area/y", inc="area-rels")
    assert clock.sleeps == [pytest.approx(1.1)]


def test_id_query_ors_ids_on_one_field():
    assert enrich.id_query("rgid", ["a", "b"]) == "rgid:a OR rgid:b"


def test_chunks():
    assert list(enrich.chunks([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


# ---------------------------------------------------------------------------
# Tags, genres, dates, titles
# ---------------------------------------------------------------------------
def test_tag_string_orders_by_votes_and_drops_downvoted():
    tags = [{"name": "folk", "count": 2}, {"name": "seen live", "count": 5},
            {"name": "americana", "count": 2}, {"name": "bad", "count": -1}]
    assert enrich.tag_string(tags) == "seen live, americana, folk"


def test_tag_string_empty():
    assert enrich.tag_string(None) == ""


def test_genres_from_tags_keeps_only_official_genres_in_order():
    genres = {"americana", "folk", "indie folk"}
    assert enrich.genres_from_tags("seen live, indie folk, favorites, americana", genres) == "indie folk, americana"


def test_wikidata_date_respects_precision():
    assert enrich.wikidata_date("1958-06-07T00:00:00Z", "11") == "1958-06-07"
    assert enrich.wikidata_date("1958-06-01T00:00:00Z", "10") == "1958-06"
    assert enrich.wikidata_date("1958-01-01T00:00:00Z", "9") == "1958"
    assert enrich.wikidata_date(None, None) == ""


def test_titles_match_ignores_case_punctuation_and_edition_suffixes():
    assert enrich.titles_match("I and Love and You", "I And Love And You (Deluxe Edition)")
    assert enrich.titles_match("Sign o' the Times", "Sign “O” the Times")
    assert not enrich.titles_match("Purple Rain", "1999")


# ---------------------------------------------------------------------------
# Parsing API responses into cache rows
# ---------------------------------------------------------------------------
RG_JSON = {
    "id": "rg1", "title": "Purple Rain", "primary-type": "Album", "secondary-types": ["Soundtrack"],
    "first-release-date": "1984-06-25",
    "artist-credit": [{"name": "Prince and The Revolution",
                       "artist": {"id": PRINCE, "name": "Prince"}}],
    "tags": [{"name": "pop", "count": 3}, {"name": "funk", "count": 4}],
}


def test_parse_release_group():
    row = enrich.parse_release_group("rg-station-id", RG_JSON)
    assert row["album_mbid"] == "rg-station-id"
    assert row["rg_id"] == "rg1"
    assert row["release_type"] == "Album + Soundtrack"
    assert row["first_release_date"] == "1984-06-25"
    assert row["tags"] == "funk, pop"
    assert json.loads(row["artist_credit"]) == [
        {"id": PRINCE, "name": "Prince", "credited": "Prince and The Revolution"}]
    assert row["status"] == "ok"


ARTIST_JSON = {
    "id": PRINCE, "name": "Prince", "type": "Person", "gender": "male", "country": "US",
    "life-span": {"begin": "1958-06-07", "end": "2016-04-21", "ended": True},
    "begin-area": {"id": "mpls", "name": "Minneapolis"},
    "tags": [{"name": "funk", "count": 10}],
}


def test_parse_artist():
    row = enrich.parse_artist(PRINCE, ARTIST_JSON)
    assert row["mb_artist_id"] == PRINCE
    assert row["gender"] == "Male"
    assert row["begin"] == "1958-06-07"
    assert row["begin_area_id"] == "mpls"
    assert row["tags"] == "funk"


def test_parse_area_finds_parent_from_backward_part_of():
    d = {"id": "mpls", "name": "Minneapolis", "type": "City", "relations": [
        {"type": "part of", "direction": "forward", "area": {"id": "child"}},
        {"type": "part of", "direction": "backward", "area": {"id": "hennepin"}}]}
    assert enrich.parse_area("mpls", d) == {"area_id": "mpls", "name": "Minneapolis", "type": "City",
                                            "parent_id": "hennepin", "status": "ok"}


def test_parse_wikidata_takes_first_row_per_artist_and_prefers_birth_over_inception():
    bindings = [
        {"mbid": {"value": PRINCE}, "genderLabel": {"value": "male"},
         "dob": {"value": "1958-06-07T00:00:00Z"}, "dobPrec": {"value": "11"},
         "placeLabel": {"value": "Minneapolis"}, "stateLabel": {"value": "Minnesota"},
         "countryLabel": {"value": "United States"}},
        {"mbid": {"value": PRINCE}, "genderLabel": {"value": "male"},
         "dob": {"value": "1958-06-07T00:00:00Z"}, "dobPrec": {"value": "11"},
         "placeLabel": {"value": "Minneapolis"}, "stateLabel": {"value": "Hennepin"},
         "countryLabel": {"value": "United States"}},
        {"mbid": {"value": "band"}, "inc": {"value": "2000-01-01T00:00:00Z"}, "incPrec": {"value": "9"}},
    ]
    rows = enrich.parse_wikidata(bindings)
    assert rows[PRINCE] == {"wd_gender": "Male", "wd_begin": "1958-06-07", "wd_place": "Minneapolis",
                            "wd_state": "Minnesota", "wd_country": "United States"}
    assert rows["band"]["wd_begin"] == "2000"
    assert rows["band"]["wd_gender"] == ""


# ---------------------------------------------------------------------------
# Locations
# ---------------------------------------------------------------------------
AREAS = {
    "mpls": {"name": "Minneapolis", "type": "City", "parent_id": "hennepin"},
    "hennepin": {"name": "Hennepin County", "type": "County", "parent_id": "mn"},
    "mn": {"name": "Minnesota", "type": "Subdivision", "parent_id": "us"},
    "us": {"name": "United States", "type": "Country", "parent_id": None},
}


def test_location_parts_walks_up_to_state_and_country():
    assert enrich.location_parts("mpls", AREAS) == ("Minneapolis", "Minnesota", "United States")


def test_location_parts_for_a_state_or_country():
    assert enrich.location_parts("mn", AREAS) == ("", "Minnesota", "United States")
    assert enrich.location_parts("us", AREAS) == ("", "", "United States")


def test_location_parts_stops_when_parent_not_cached():
    areas = {"x": {"name": "Somewhere", "type": "City", "parent_id": "missing"}}
    assert enrich.location_parts("x", areas) == ("Somewhere", "", "")


def test_format_location():
    assert enrich.format_location(("Minneapolis", "Minnesota", "United States")) == \
        "Minneapolis, Minnesota, United States"
    assert enrich.format_location(("", "", "")) == ""


# ---------------------------------------------------------------------------
# Matching station artists to MusicBrainz artists
# ---------------------------------------------------------------------------
def credits(*names_ids):
    return [{"id": i, "name": n, "credited": n} for n, i in names_ids]


def test_pick_credit_single_exact():
    assert enrich.pick_credit("prince", credits(("Prince", PRINCE))) == (PRINCE, "Prince", "exact")


def test_pick_credit_single_with_different_name():
    assert enrich.pick_credit("prince & the revolution", credits(("Wendy", "w"))) == ("w", "Wendy", "single_credit")


def test_pick_credit_several_uses_name_contained_in_station_artist():
    c = credits(("Zach Bryan", "zb"), ("Kacey Musgraves", "km"))
    assert enrich.pick_credit("kacey musgraves feat. zach bryan", c)[0] in {"zb", "km"}
    assert enrich.pick_credit("zach bryan", c) == ("zb", "Zach Bryan", "exact")


def test_pick_credit_several_without_any_name_match_is_none():
    assert enrich.pick_credit("someone else", credits(("A", "a"), ("B", "b"))) is None


def test_pick_credit_skips_various_artists():
    assert enrich.pick_credit("prince", credits(("Various Artists", VARIOUS))) is None


def test_pick_credit_ignores_leading_the_and_ampersand():
    assert enrich.pick_credit("avett brothers", credits(("The Avett Brothers", "ab")))[2] == "exact"
    assert enrich.pick_credit("mumford and sons", credits(("Mumford & Sons", "ms")))[2] == "exact"


# ---------------------------------------------------------------------------
# Building the tables
# ---------------------------------------------------------------------------
def plays_frame():
    rows = [
        # Purple Rain played from the original album and from a later compilation
        ("1999-01-01 10:00", "Purple Rain", "Prince", "Purple Rain", "rg-pr"),
        ("2005-01-01 10:00", "Purple Rain", "Prince", "The Hits", "rg-hits"),
        ("2006-01-01 10:00", "Purple Rain", "Prince", "Purple Rain", "rg-pr"),
        # a song whose only album ID points to an album with a different title
        ("2007-01-01 10:00", "Laundry Room", "The Avett Brothers", "I and Love and You", "rg-ep"),
        # an artist with no album ID at all
        ("2008-01-01 10:00", "Song", "Nobody Known", "Demo", None),
    ]
    df = pd.DataFrame(rows, columns=["played_at", "title", "artist", "album", "album_mbid"])
    df["played_at"] = pd.to_datetime(df["played_at"])
    df["artist_key"] = enrich.normalize(df["artist"])
    df["song_key"] = enrich.normalize(df["title"]) + "|" + df["artist_key"]
    df["is_music"] = True
    return df


def rg_row(album_mbid, title, date, artist_id, artist_name, tags="", rtype="Album"):
    return {"album_mbid": album_mbid, "rg_id": album_mbid, "title": title, "release_type": rtype,
            "first_release_date": date, "tags": tags, "status": "ok",
            "artist_credit": json.dumps([{"id": artist_id, "name": artist_name, "credited": artist_name}])}


def caches():
    rgs = pd.DataFrame([
        rg_row("rg-pr", "Purple Rain", "1984-06-25", PRINCE, "Prince", tags="funk, seen live"),
        rg_row("rg-hits", "The Hits", "1993", PRINCE, "Prince", rtype="Album + Compilation"),
        rg_row("rg-ep", "Live Vol. 3", "2010", "ab", "The Avett Brothers"),
    ])
    artists = pd.DataFrame([
        {"mb_artist_id": PRINCE, "id": PRINCE, "name": "Prince", "type": "Person", "gender": "Male",
         "begin": "1958-06-07", "begin_area_id": "mpls", "country": "US", "tags": "funk, pop", "status": "ok",
         "wd_checked": True, "wd_gender": "", "wd_begin": "", "wd_place": "", "wd_state": "", "wd_country": ""},
        {"mb_artist_id": "ab", "id": "ab", "name": "The Avett Brothers", "type": "Group", "gender": "",
         "begin": "", "begin_area_id": "", "country": "US", "tags": "americana, folk", "status": "ok",
         "wd_checked": True, "wd_gender": "", "wd_begin": "2000",
         "wd_place": "Concord", "wd_state": "North Carolina", "wd_country": "United States"},
    ])
    areas = pd.DataFrame([{"area_id": k, **v, "status": "ok"} for k, v in AREAS.items()])
    return rgs, artists, areas


GENRES = {"funk", "pop", "americana", "folk"}


def test_build_artists():
    rgs, artists, areas = caches()
    out = enrich.build_artists(plays_frame(), rgs, artists, areas, GENRES).set_index("artist")
    prince = out.loc["Prince"]
    assert prince["mb_artist_id"] == PRINCE
    assert prince["match_method"] == "exact"
    assert prince["birth_location"] == "Minneapolis, Minnesota, United States"
    assert prince["birth_state"] == "Minnesota"
    assert prince["genres"] == "funk, pop"
    assert prince["plays"] == 3 and prince["songs"] == 1
    assert prince["filled_from_wikidata"] == ""

    avett = out.loc["The Avett Brothers"]
    assert avett["birth_date"] == "2000"
    assert avett["birth_location"] == "Concord, North Carolina, United States"
    assert avett["filled_from_wikidata"] == "birth_date, birth_location"
    assert avett["gender"] == ""  # groups never get a gender

    nobody = out.loc["Nobody Known"]
    assert pd.isna(nobody["mb_artist_id"]) and nobody["plays"] == 1
    assert len(out) == 3


def test_build_songs():
    rgs, artists, areas = caches()
    plays = plays_frame()
    artist_table = enrich.build_artists(plays, rgs, artists, areas, GENRES)
    out = enrich.build_songs(plays, rgs, artist_table, GENRES).set_index("title")

    pr = out.loc["Purple Rain"]
    assert pr["release_date"] == "1984-06-25"
    assert pr["release_year"] == 1984
    assert pr["release_album"] == "Purple Rain"
    assert bool(pr["release_album_matches"]) is True
    assert pr["genres"] == "funk" and pr["genres_source"] == "album"
    assert pr["tags"] == "funk, seen live"
    assert pr["plays"] == 3 and pr["album_ids_checked"] == 2

    laundry = out.loc["Laundry Room"]
    assert bool(laundry["release_album_matches"]) is False
    assert laundry["genres"] == "americana, folk" and laundry["genres_source"] == "artist"

    song = out.loc["Song"]
    assert pd.isna(song["release_year"]) and song["album_ids_checked"] == 0
    assert len(out) == 3


def test_build_skips_non_music_plays():
    rgs, artists, areas = caches()
    plays = plays_frame()
    plays.loc[plays["artist"] == "Nobody Known", "is_music"] = False
    assert "Nobody Known" not in set(enrich.build_artists(plays, rgs, artists, areas, GENRES)["artist"])
