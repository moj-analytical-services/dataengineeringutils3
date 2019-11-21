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


def get_list():
    return [
        '{"uuid": "fkjherpiutrgponfevpoir3qjgp8prueqhf9pq34hf89hwfpu92q"}'
    ] * 10000


def loop_through_qs():
    select_queryset = SelectQuerySet(
        MockQs(get_list()),
        "",
        10000,
    )
    results = []
    for l in select_queryset:
        results.append(l)
    return results


def loop_through_list():
    results = []
    for l in get_list():
        results.append(l)
    return results


def test_speed_of_iterator():
    """
    Test that generator is not much slower than a flat list
    """
    qs_time = time_func(loop_through_qs)

    range_time = time_func(loop_through_list)

    assert qs_time * 0.5 < range_time
