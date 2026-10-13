import pandas as pd

from plays import normalize


def test_normalize_drops_accents_case_and_extra_spaces():
    s = pd.Series(["  Poliça ", "The  Avett Brothers"])
    assert normalize(s).tolist() == ["polica", "the avett brothers"]
