from unittest.mock import call

from dataengineeringutils3.db import SelectQuerySet
from tests.mocks import MockQs


def test_select_queryset(select_queryset):
    """
    Tests that the SelectQuerySet chunks the query in to sets of two calling fetcxhmany
    on the cursor.
    """
    results = []
    for rows in select_queryset.iter_chunks():
        for res in rows:
            results.append(res)
    assert (
        results
        == ['{"uuid": "fkjherpiutrgponfevpoir3qjgp8prueqhf9pq34hf89hwfpu92q"}'] * 15
    )
    select_queryset.cursor.fetchmany.assert_has_calls(
        [call(2), call(2), call(2), call(2), call(2), call(2), call(2), call(2)]
    )


def gen(result_set):
    while True:
        return (res for res in result_set)


def loop_through_qs(result_set):
    select_queryset = SelectQuerySet(MockQs(result_set), "", 1000000)
    results = []
    [results.append(row) for rows in select_queryset.iter_chunks() for row in rows]
    return results


def loop_through_list(result_set):
    results = []
    while True:
        for out in MockQs(result_set).fetchmany(1000000):
            results.append(out)
        break
    return results
