from unittest.mock import call

from dataengineeringutils3.db import SelectQuerySet
from tests.helpers import time_func
from tests.mocks import MockQs


def test_select_queryset(select_queryset):
    """
    Tests that the SelectQuerySet chunks the query in to sets of two calling fetcxhmany
    on the cursor.
    """
    results = []
    for res in select_queryset:
        results.append(res)
    assert results == [
        '{"uuid": "fkjherpiutrgponfevpoir3qjgp8prueqhf9pq34hf89hwfpu92q"}'
    ] * 15
    select_queryset.cursor.fetchmany.assert_has_calls([
        call(2),
        call(2),
        call(2),
        call(2),
        call(2),
        call(2),
        call(2),
        call(2),
    ])


def loop_through_qs(result_set):
    select_queryset = SelectQuerySet(
        MockQs(result_set),
        "",
        10000,
    )
    results = []
    for l in select_queryset:
        results.append(l)
    return results


def loop_through_list(result_set):
    results = []
    for l in result_set:
        results.append(l)
    return results


def test_speed_of_iterator(result_set):
    """
    Test that generator is not much slower than a flat list
    """
    qs_time = time_func(loop_through_qs, result_set)

    range_time = time_func(loop_through_list, result_set)

    assert qs_time * 0.5 < range_time
