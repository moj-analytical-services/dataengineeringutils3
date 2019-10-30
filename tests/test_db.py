import pytest

from dataengineeringutils3.db import SelectQuerySet


class MockCursor:
    def __init__(self, total_rows):
        self.n = 0
        self.results = range(total_rows)

    def execute(self, *args, **kwargs):
        pass

    def fetchmany(self, fetch_size):
        n = self.n
        self.n = min(len(self.results), self.n + fetch_size)
        return self.results[n: self.n]


@pytest.fixture
def select_queryset():
    return SelectQuerySet(
        MockCursor(15),
        2,
        "query"
    )


def test_select_queryset(select_queryset):
    results = []
    for res in select_queryset:
        results.append(res)
    assert results == list(range(15))
