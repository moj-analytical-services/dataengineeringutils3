from unittest.mock import call


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
