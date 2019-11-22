class SelectQuerySet:
    """
    Iterator for fetching select query results in chunks.

    The following example uses the select queryset along with JsonNlSplitFileWriter to
    write the contents of a table to s3 in .jsonl.gz

    con = cx_Oracle.connect(
        parameters['DB_USER_ID'],
        parameters['DB_PWD'],
        parameters['DB_DSN']
    )

    select_queryset = SelectQuerySet(
        con.cursor(),
        "select * from table",
        1000,
    )

    with JsonNlSplitFileWriter("s3://test/test-file.jsonl.gz") as writer:
        column_names = select_queryset.headers
        for row in select_queryset:
            writer.write_line(json.dumps(zip(column_names, row), cls=DateTimeEncoder))
    """
    def __init__(self, cursor, select_query, fetch_size=1000, **query_kwargs):
        """
        Sets the curser, query and executes
        :param cursor: curser object: such as cx_Oracle.connect().cursor
        :param select_query: string: "select * from table"
        :param fetch_size: int: 1000
        :param query_kwargs: kwargs: kwargs for query formatting
        """
        self.query = select_query
        self.cursor = cursor
        self.fetch_size = fetch_size
        self.cursor.execute(select_query, **query_kwargs)
        self._result_cache = self.cursor.fetchmany(self.fetch_size)

    def __iter__(self):
        """Reset iterator and n to 0"""
        while self._result_cache:
            for r in self._result_cache:
                yield r

            self._result_cache = self._fetch_many()

    def _fetch_many(self):
        try:
            return self.cursor.fetchmany(self.fetch_size)
        except Exception:
            return None

    @property
    def headers(self):
        """Return column names"""
        return [c[0] for c in self.cursor.description]
