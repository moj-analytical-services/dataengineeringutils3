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

    # Write out json string
    with JsonNlSplitFileWriter("s3://test/test-file.jsonl.gz") as writer:
        column_names = select_queryset.headers
        for row in select_queryset:
            json_line_str = json.dumps(
                dict(zip(column_names, row)),
                cls=DateTimeEncoder
            )
            writer.write_line(json_line_str)

    # Use a function to convert row to json
    with JsonNlSplitFileWriter("s3://test/test-file.jsonl.gz") as writer:
        column_names = select_queryset.headers
        def transform_line(row):
            return json.dumps(dict(zip(column_names, row)), cls=DateTimeEncoder)
        select_queryset.write_to_file(writer, transform_line)

    # Use a function to convert row to json but write multiple lines at once
    with JsonNlSplitFileWriter("s3://test/test-file.jsonl.gz") as writer:
        column_names = select_queryset.headers
        def transform_line(row):
            return json.dumps(dict(zip(column_names, row)), cls=DateTimeEncoder)
        for results in select_queryset.iter_chunks():
            writer.write_lines(results, transform_line)
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
        self.cursor.arraysize = fetch_size
        self.fetch_size = fetch_size
        self.cursor.execute(select_query, **query_kwargs)

    def __iter__(self):
        """Reset iterator and n to 0"""
        for r in self.cursor:
            yield r

    def iter_chunks(self):
        results = self.cursor.fetchmany(self.fetch_size)
        while results:
            yield results
            try:
                results = self.cursor.fetchmany(self.fetch_size)
            except Exception:
                results = None
                break

    @property
    def headers(self):
        """Return column names"""
        return [c[0] for c in self.cursor.description]

    def write_to_file(self, file_writer, line_transform=lambda x: x):
        for results in self.iter_chunks():
            file_writer.write_lines(results, line_transform)
